import requests
import json
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from datetime import datetime
import time

# ---------- Утилиты ----------
def get_column_letter(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

def read_api_credentials(file_path: str):
    with open(file_path, "r", encoding="utf-8") as f:
        lines = [line.strip() for line in f if line.strip()]
    if len(lines) < 4:
        raise ValueError("Файл должен содержать 4 строки: client_id, api_key, spreadsheet_id, sheet_name")
    client_id, api_key, spreadsheet_id, sheet_name = lines[:4]
    return api_key, client_id, spreadsheet_id, sheet_name

# ---------- OZON ----------
def get_all_products(client_id: str, api_key: str):
    all_products, last_id, limit = [], "", 1000
    url = "https://api-seller.ozon.ru/v3/product/list"
    headers = {"Client-Id": client_id, "Api-Key": api_key, "Content-Type": "application/json"}
    while True:
        payload = {"filter": {}, "last_id": last_id, "limit": limit}
        resp = requests.post(url, headers=headers, data=json.dumps(payload))
        if resp.status_code != 200:
            print(f"Ошибка OZON: {resp.status_code} - {resp.text}")
            break
        result = resp.json().get("result", {})
        items = result.get("items", []) or []
        all_products.extend(items)
        if len(items) < limit:
            break
        last_id = result.get("last_id", "")
        if not last_id:
            break
    return all_products

# ---------- Google Sheets: сервис и метаданные ----------
def build_sheets_service(credentials_path: str):
    creds = service_account.Credentials.from_service_account_file(credentials_path)
    return build("sheets", "v4", credentials=creds)

def get_sheet_meta(service, spreadsheet_id: str, fields: str):
    return service.spreadsheets().get(spreadsheetId=spreadsheet_id, fields=fields).execute()

def get_sheet_by_name(service, spreadsheet_id: str, sheet_name: str):
    meta = get_sheet_meta(service, spreadsheet_id, "sheets(properties(sheetId,title,gridProperties(rowCount,columnCount)))")
    return next((s for s in meta.get("sheets", []) if s["properties"]["title"] == sheet_name), None)

def adjust_sheet_rows(service, spreadsheet_id: str, sheet_id: int, current_rows: int, need_rows: int):
    if current_rows < need_rows:
        service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": [{"appendDimension": {"sheetId": sheet_id, "dimension": "ROWS", "length": need_rows - current_rows}}]},
        ).execute()
        print(f"Добавлено {need_rows - current_rows} строк.")

def adjust_sheet_columns(service, spreadsheet_id: str, sheet_id: int, current_cols: int, need_cols: int):
    """Расширяет лист по столбцам до need_cols (минимум)."""
    if current_cols < need_cols:
        service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": [{"appendDimension": {"sheetId": sheet_id, "dimension": "COLUMNS", "length": need_cols - current_cols}}]},
        ).execute()
        print(f"Добавлено {need_cols - current_cols} столбцов.")

def trim_sheet_rows(service, spreadsheet_id: str, sheet_id: int, keep_rows: int):
    """Удаляет лишние строки снизу, оставляя ровно keep_rows (1-based)."""
    meta = get_sheet_meta(service, spreadsheet_id, "sheets(properties(sheetId,gridProperties(rowCount)))")
    sheet = next((s for s in meta["sheets"] if s["properties"]["sheetId"] == sheet_id), None)
    if not sheet:
        return
    current_rows = sheet["properties"]["gridProperties"]["rowCount"]
    if current_rows > keep_rows:
        service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": [{
                "deleteDimension": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "ROWS",
                        "startIndex": keep_rows,      # 0-based; удаляем начиная со строки keep_rows+1
                        "endIndex": current_rows
                    }
                }
            }]}
        ).execute()
        print(f"Удалено {current_rows - keep_rows} лишних строк снизу.")

# ---------- Очистка/фильтры ----------
def remove_filter(service, spreadsheet_id: str, sheet_name: str):
    try:
        meta = get_sheet_meta(service, spreadsheet_id, "sheets(properties(sheetId,title))")
        sheet = next((s for s in meta["sheets"] if s["properties"]["title"] == sheet_name), None)
        if not sheet:
            raise ValueError(f"Лист '{sheet_name}' не найден")
        reqs = [{"clearBasicFilter": {"sheetId": sheet["properties"]["sheetId"]}}]
        service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": reqs}).execute()
        print("Фильтр удален")
    except Exception as e:
        print(f"Ошибка при удалении фильтра: {e}")

def get_last_filled_row_in_column(service, spreadsheet_id: str, sheet_name: str, column_letter: str = "B", min_row: int = 4) -> int:
    """
    Возвращает индекс строки (1-based) последней непустой ячейки в указанном столбце.
    Если данных нет, возвращает min_row (для сохранения шапки).
    """
    resp = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=f"{sheet_name}!{column_letter}:{column_letter}",
        majorDimension="COLUMNS"
    ).execute()
    col_vals = resp.get("values", [[]])
    last = len(col_vals[0]) if col_vals and col_vals[0] else min_row
    return max(last, min_row)

def add_full_range_filter(service, spreadsheet_id: str, sheet_name: str):
    """
    Базовый фильтр на строки 4..last_row и столбцы A..последний.
    last_row считаем по столбцу B, чтобы не обрезать диапазон.
    """
    try:
        sheet = get_sheet_by_name(service, spreadsheet_id, sheet_name)
        if not sheet:
            raise ValueError(f"Лист '{sheet_name}' не найден")

        sheet_id = sheet["properties"]["sheetId"]
        end_col = sheet["properties"]["gridProperties"]["columnCount"]

        last_row = get_last_filled_row_in_column(service, spreadsheet_id, sheet_name, column_letter="B", min_row=4)

        reqs = [{
            "setBasicFilter": {
                "filter": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 3,          # с 4-й строки
                        "endRowIndex": last_row,
                        "startColumnIndex": 0,       # начиная с A
                        "endColumnIndex": end_col,   # до последнего столбца листа
                    }
                }
            }
        }]
        service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": reqs}).execute()
        print(f"Фильтр добавлен на строки 4-{last_row} и столбцы A..")
    except Exception as e:
        print(f"Ошибка при добавлении фильтра: {e}")

def clear_google_sheet_range(service, spreadsheet_id: str, sheet_name: str):
    """Очищает все данные с 5-й строки по всем существующим столбцам (A5:...)."""
    try:
        sheet = get_sheet_by_name(service, spreadsheet_id, sheet_name)
        if not sheet:
            raise ValueError(f"Лист '{sheet_name}' не найден")

        max_cols = sheet["properties"]["gridProperties"]["columnCount"]
        max_rows = sheet["properties"]["gridProperties"]["rowCount"]

        clear_range = f"{sheet_name}!A5:{get_column_letter(max_cols)}{max_rows}"
        service.spreadsheets().values().clear(spreadsheetId=spreadsheet_id, range=clear_range, body={}).execute()
        print(f"Очищен диапазон {clear_range}")
        return True
    except Exception as e:
        print(f"Ошибка при очистке: {e}")
        return False

# ---------- Запись данных ----------
def save_products_to_google_sheets(service, products, spreadsheet_id: str, sheet_name: str):
    """
    Новая схема записи:
      B = offer_id
      L = product_id
    """
    products = [p for p in products if "product_id" in p and "offer_id" in p]
    if not products:
        print("Пустой список товаров")
        return

    # --- формируем DataFrame ---
    df = pd.DataFrame(products)[["offer_id", "product_id"]]
    # Убираем полностью пустые записи
    df = df[~(df["offer_id"].astype(str).str.strip().eq("") & df["product_id"].astype(str).str.strip().eq(""))]
    df = df.fillna("")

    values_offer = df[["offer_id"]].values.tolist()    # для B
    values_pid   = df[["product_id"]].values.tolist()  # для L
    data_rows = len(df)

    # 1) убрать фильтр
    remove_filter(service, spreadsheet_id, sheet_name)

    # 2) гарантировать достаточное число строк (4 служебные + данные) и столбцов (до L)
    sheet = get_sheet_by_name(service, spreadsheet_id, sheet_name)
    if not sheet:
        raise ValueError(f"Лист '{sheet_name}' не найден")
    sheet_id = sheet["properties"]["sheetId"]
    current_rows = sheet["properties"]["gridProperties"]["rowCount"]
    current_cols = sheet["properties"]["gridProperties"]["columnCount"]

    need_rows = max(current_rows, 4 + data_rows)
    need_cols = max(current_cols, 12)  # L = 12-й столбец
    adjust_sheet_rows(service, spreadsheet_id, sheet_id, current_rows, need_rows)
    adjust_sheet_columns(service, spreadsheet_id, sheet_id, current_cols, need_cols)

    # 3) очистить диапазон данных (с 5-й строки, начиная с A)
    clear_google_sheet_range(service, spreadsheet_id, sheet_name)

    # 4) записать данные: B5 (offer_id) и L5 (product_id)
    print(f"Записывается {data_rows} строк")
    if data_rows:
        service.spreadsheets().values().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={
                "valueInputOption": "RAW",
                "data": [
                    {"range": f"{sheet_name}!B5", "values": values_offer},
                    {"range": f"{sheet_name}!L5", "values": values_pid},
                ],
            },
        ).execute()

    # 5) после записи пересчитать фактический последний ряд по столбцу B и урезать лист
    time.sleep(0.2)
    last_row = get_last_filled_row_in_column(service, spreadsheet_id, sheet_name, column_letter="B", min_row=4)
    trim_sheet_rows(service, spreadsheet_id, sheet_id, keep_rows=last_row)

    # 6) добавить фильтр (A..последний) по актуальному диапазону
    time.sleep(0.2)
    add_full_range_filter(service, spreadsheet_id, sheet_name)

# ---------- Оформление/формат ----------
def format_google_sheet(service, spreadsheet_id: str, sheet_name: str):
    sheet = get_sheet_by_name(service, spreadsheet_id, sheet_name)
    if not sheet:
        raise ValueError(f"Лист '{sheet_name}' не найден")
    sheet_props = sheet["properties"]
    sheet_id = sheet_props["sheetId"]

    # гарантируем наличие L
    end_col_current = sheet_props["gridProperties"]["columnCount"]
    if end_col_current < 12:
        adjust_sheet_columns(service, spreadsheet_id, sheet_id, end_col_current, 12)
        # перечитаем метаданные
        sheet = get_sheet_by_name(service, spreadsheet_id, sheet_name)
        sheet_props = sheet["properties"]
        end_col_current = sheet_props["gridProperties"]["columnCount"]

    # >>> ДОБАВЛЕНО: гарантируем наличие CB (80-й столбец) и чистим BW3:CB3
    need_min_cols = 80  # CB
    if end_col_current < need_min_cols:
        adjust_sheet_columns(service, spreadsheet_id, sheet_id, end_col_current, need_min_cols)
        # перечитаем метаданные
        sheet = get_sheet_by_name(service, spreadsheet_id, sheet_name)
        sheet_props = sheet["properties"]

    # очистка только значений в BW3:CB3 (форматирование не трогаем)
    service.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id,
        range=f"{sheet_name}!BW3:CB3",
        body={}
    ).execute()
    print("Очищены значения в диапазоне BW3:CB3")
    # <<< КОНЕЦ ДОБАВЛЕНИЙ

    end_col = sheet_props["gridProperties"]["columnCount"]
    end_row = sheet_props["gridProperties"]["rowCount"]

    update_date = datetime.now().strftime("%d.%m %H:%M")

    # Цвета
    gray_rgb = {"red": 243/255, "green": 243/255, "blue": 243/255}  # #f3f3f3
    white_rgb = {"red": 1, "green": 1, "blue": 1}
    black_rgb = {"red": 0, "green": 0, "blue": 0}

    # 1) Объединения: A2:B2, A3:B3 (объединения D1:D3 нет)
    merge_reqs = [
        {   # A2:B2
            "mergeCells": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": 2,
                          "startColumnIndex": 0, "endColumnIndex": 2},
                "mergeType": "MERGE_ALL"
            }
        },
        {   # A3:B3
            "mergeCells": {
                "range": {"sheetId": sheet_id, "startRowIndex": 2, "endRowIndex": 3,
                          "startColumnIndex": 0, "endColumnIndex": 2},
                "mergeType": "MERGE_ALL"
            }
        },
    ]

    # 2) Подписи/дата, заголовки (B4, L4). Никаких операций с D1:D3.
    service.spreadsheets().values().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={
            "valueInputOption": "USER_ENTERED",
            "data": [
                {"range": f"{sheet_name}!A2", "values": [["Обновлено"]]},
                {"range": f"{sheet_name}!A3", "values": [[update_date]]},
                {"range": f"{sheet_name}!B4", "values": [["Артикул"]]},
                {"range": f"{sheet_name}!L4", "values": [["Product ID"]]},
            ],
        },
    ).execute()

    # 3) Ширины столбцов: B=174, L=75 (D не трогаем)
    width_reqs = [
        {"updateDimensionProperties": {
            "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 1, "endIndex": 2},  # B
            "properties": {"pixelSize": 174}, "fields": "pixelSize"
        }},
        {"updateDimensionProperties": {
            "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 11, "endIndex": 12},  # L
            "properties": {"pixelSize": 75}, "fields": "pixelSize"
        }},
    ]

    # 4) Стили шапки (A2:B2, A3:B3, B4, L4)
    header_reqs = [
        # A2:B2 — центрирование
        {"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": 2,
                                  "startColumnIndex": 0, "endColumnIndex": 2},
                        "cell": {"userEnteredFormat": {
                            "textFormat": {"fontFamily": "Oswald", "fontSize": 13, "bold": True},
                            "horizontalAlignment": "CENTER", "verticalAlignment": "MIDDLE"
                        }},
                        "fields": "userEnteredFormat.textFormat,userEnteredFormat.horizontalAlignment,userEnteredFormat.verticalAlignment"}},
        # A3:B3 — центрирование
        {"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": 2, "endRowIndex": 3,
                                  "startColumnIndex": 0, "endColumnIndex": 2},
                        "cell": {"userEnteredFormat": {
                            "textFormat": {"fontFamily": "Oswald", "fontSize": 16, "bold": True},
                            "horizontalAlignment": "CENTER", "verticalAlignment": "MIDDLE"
                        }},
                        "fields": "userEnteredFormat.textFormat,userEnteredFormat.horizontalAlignment,userEnteredFormat.verticalAlignment"}},
        # B4 — центрирование
        {"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": 3, "endRowIndex": 4,
                                  "startColumnIndex": 1, "endColumnIndex": 2},
                        "cell": {"userEnteredFormat": {
                            "textFormat": {"fontFamily": "Oswald", "fontSize": 10, "bold": True},
                            "horizontalAlignment": "CENTER", "verticalAlignment": "MIDDLE"
                        }},
                        "fields": "userEnteredFormat.textFormat,userEnteredFormat.horizontalAlignment,userEnteredFormat.verticalAlignment"}},
        # L4 — центрирование + перенос
        {"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": 3, "endRowIndex": 4,
                                  "startColumnIndex": 11, "endColumnIndex": 12},
                        "cell": {"userEnteredFormat": {
                            "textFormat": {"fontFamily": "Oswald", "bold": True},
                            "horizontalAlignment": "CENTER", "verticalAlignment": "MIDDLE",
                            "wrapStrategy": "WRAP"
                        }},
                        "fields": "userEnteredFormat.textFormat,userEnteredFormat.horizontalAlignment,userEnteredFormat.verticalAlignment,userEnteredFormat.wrapStrategy"}},
    ]

    # 5) Заливка строки 4 целиком (A..последний, включая D)
    row4_gray_req = {
        "repeatCell": {
            "range": {"sheetId": sheet_id,
                      "startRowIndex": 3, "endRowIndex": 4,
                      "startColumnIndex": 0, "endColumnIndex": end_col},
            "cell": {"userEnteredFormat": {"backgroundColorStyle": {"rgbColor": gray_rgb}}},
            "fields": "userEnteredFormat.backgroundColorStyle"
        }
    }

    # 6) Формат текста в B и L c 5-й строки
    be_text_req = [
        {"repeatCell": {
            "range": {"sheetId": sheet_id, "startRowIndex": 4, "startColumnIndex": 1, "endColumnIndex": 2},  # B
            "cell": {"userEnteredFormat": {
                "textFormat": {"fontFamily": "Oswald", "fontSize": 9, "bold": True,
                               "foregroundColorStyle": {"rgbColor": black_rgb}}
            }},
            "fields": "userEnteredFormat.textFormat"
        }},
        {"repeatCell": {
            "range": {"sheetId": sheet_id, "startRowIndex": 4, "startColumnIndex": 11, "endColumnIndex": 12},  # L
            "cell": {"userEnteredFormat": {
                "textFormat": {"fontFamily": "Oswald", "fontSize": 10, "bold": False,
                               "foregroundColorStyle": {"rgbColor": black_rgb}}
            }},
            "fields": "userEnteredFormat.textFormat"
        }},
    ]
    be_bg_white_req = [
        {"repeatCell": {
            "range": {"sheetId": sheet_id, "startRowIndex": 4, "startColumnIndex": 1, "endColumnIndex": 2},  # B
            "cell": {"userEnteredFormat": {"backgroundColorStyle": {"rgbColor": white_rgb}}},
            "fields": "userEnteredFormat.backgroundColorStyle"
        }},
        {"repeatCell": {
            "range": {"sheetId": sheet_id, "startRowIndex": 4, "startColumnIndex": 11, "endColumnIndex": 12},  # L
            "cell": {"userEnteredFormat": {"backgroundColorStyle": {"rgbColor": white_rgb}}},
            "fields": "userEnteredFormat.backgroundColorStyle"
        }},
    ]

    # 7) Высоты строк
    row_size_reqs = [
        {"updateDimensionProperties": {
            "range": {"sheetId": sheet_id, "dimension": "ROWS", "startIndex": 0, "endIndex": 3},
            "properties": {"pixelSize": 30}, "fields": "pixelSize"
        }},
        {"updateDimensionProperties": {
            "range": {"sheetId": sheet_id, "dimension": "ROWS", "startIndex": 3, "endIndex": 4},
            "properties": {"pixelSize": 42}, "fields": "pixelSize"
        }},
        {"updateDimensionProperties": {
            "range": {"sheetId": sheet_id, "dimension": "ROWS", "startIndex": 4, "endIndex": end_row},
            "properties": {"pixelSize": 35}, "fields": "pixelSize"
        }},
    ]

    # 8) Центрирование данных в B и L начиная с 4-й строки
    center_data_cols_req = [
        {"repeatCell": {
            "range": {"sheetId": sheet_id, "startRowIndex": 3, "endRowIndex": end_row,
                      "startColumnIndex": 1, "endColumnIndex": 2},  # B4:B
            "cell": {"userEnteredFormat": {"horizontalAlignment": "CENTER", "verticalAlignment": "MIDDLE"}},
            "fields": "userEnteredFormat.horizontalAlignment,userEnteredFormat.verticalAlignment"
        }},
        {"repeatCell": {
            "range": {"sheetId": sheet_id, "startRowIndex": 3, "endRowIndex": end_row,
                      "startColumnIndex": 11, "endColumnIndex": 12},  # L4:L
            "cell": {"userEnteredFormat": {"horizontalAlignment": "CENTER", "verticalAlignment": "MIDDLE"}},
            "fields": "userEnteredFormat.horizontalAlignment,userEnteredFormat.verticalAlignment"
        }},
    ]

    # 9) Перенос по словам в B начиная с B5
    wrap_b_from_5_req = {
        "repeatCell": {
            "range": {"sheetId": sheet_id, "startRowIndex": 4, "endRowIndex": end_row,
                      "startColumnIndex": 1, "endColumnIndex": 2},  # B5:B
            "cell": {"userEnteredFormat": {"wrapStrategy": "WRAP"}},
            "fields": "userEnteredFormat.wrapStrategy"
        }
    }

    requests = (
        merge_reqs
        + width_reqs
        + header_reqs
        + [row4_gray_req]          # заливка 4-й строки целиком, включая D
        + be_text_req
        + be_bg_white_req
        + row_size_reqs
        + center_data_cols_req
        + [wrap_b_from_5_req]
    )

    service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": requests}).execute()
    print("Форматирование применено: D1:D3 не трогаем; строка 4 целиком с заливкой; Product ID в L4; данные: B=offer_id, L=product_id.")

# ---------- Точка входа ----------
if __name__ == "__main__":
    try:
        print("Начало работы скрипта")
        api_key, client_id, spreadsheet_id, sheet_name = read_api_credentials("API.txt")
        print(f"Client ID: {client_id[:3]}...")
        print(f"API Key: {api_key[:6]}...")
        print(f"Spreadsheet ID: {spreadsheet_id}")
        print(f"Sheet Name: {sheet_name}")

        sheets_service = build_sheets_service("credentials.json")

        products = get_all_products(client_id, api_key)
        if products:
            save_products_to_google_sheets(sheets_service, products, spreadsheet_id, sheet_name)
            format_google_sheet(sheets_service, spreadsheet_id, sheet_name)
            print("Готово")
        else:
            print("Не удалось получить товары из Ozon")
    except Exception as e:
        print(f"Критическая ошибка: {e}")
