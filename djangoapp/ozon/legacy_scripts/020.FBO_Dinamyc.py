# ozon_to_sheets_dates_only.py
# -*- coding: utf-8 -*-

import time
import math
import datetime as dt
from typing import Dict, List, Tuple, Optional

import requests
import gspread
from google.oauth2.service_account import Credentials

# ========= НАСТРОЙКИ =========
BATCH_SIZE = 50
BATCH_DELAY_SEC = 1.0  # 1000 мс

# ======== ЧТЕНИЕ КОНФИГА ========
def read_api_txt(path: str = "API.txt") -> Tuple[str, str, str, str]:
    """
    API.txt без заголовков, 5 строк:
    1) Client-ID
    2) API-KEY
    3) spreadsheet_id
    4) (резерв / не используется скриптом)
    5) worksheet_name
    """
    with open(path, "r", encoding="utf-8") as f:
        lines = [line.strip() for line in f if line.strip() != ""]
    if len(lines) < 5:
        raise RuntimeError("В API.txt должно быть минимум 5 строк (лист на 5-й).")
    client_id = lines[0]
    api_key = lines[1]
    spreadsheet_id = lines[2]
    sheet_name = lines[4]  # <-- имя листа теперь на 5-й строке
    return client_id, api_key, spreadsheet_id, sheet_name

# ======== АВТОРИЗАЦИЯ GOOGLE SHEETS ========
def open_worksheet(spreadsheet_id: str, sheet_name: str, credentials_path: str = "credentials.json"):
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(credentials_path, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(spreadsheet_id)
    return sh.worksheet(sheet_name)

# ======== УТИЛИТЫ ========
def col_to_letter(col: int) -> str:
    out = ""
    while col > 0:
        col, rem = divmod(col - 1, 26)
        out = chr(65 + rem) + out
    return out

def letter_to_col(letter: str) -> int:
    s = 0
    for ch in letter.upper():
        s = s * 26 + (ord(ch) - 64)
    return s

def today_ddmm(tz: Optional[dt.tzinfo] = None) -> str:
    now = dt.datetime.now(tz) if tz else dt.datetime.now()
    return now.strftime("%d.%m")

def right_trim(values: List[str]) -> List[str]:
    last = -1
    for i in range(len(values) - 1, -1, -1):
        if str(values[i]).strip() != "":
            last = i
            break
    return values[: last + 1] if last >= 0 else []

def ensure_min_cols(ws, min_cols: int):
    """
    Гарантирует, что у листа есть как минимум min_cols столбцов.
    При необходимости автоматически добавляет недостающие столбцы.
    """
    if ws.col_count < min_cols:
        ws.add_cols(min_cols - ws.col_count)

def get_or_create_daily_column(ws, start_col: int) -> int:
    """
    Возвращает индекс столбца, в который писать 'available' за сегодня.
    """
    ensure_min_cols(ws, start_col)

    start_col_letter = col_to_letter(start_col)
    last_col_letter = col_to_letter(ws.col_count)

    headers_range = f"{start_col_letter}2:{last_col_letter}2"
    headers_row = ws.get(headers_range, value_render_option="UNFORMATTED_VALUE")
    headers = headers_row[0] if headers_row else []

    today = today_ddmm()

    # 1) Уже есть сегодняшняя дата?
    for i, v in enumerate(headers):
        if str(v).strip() == today:
            return start_col + i

    # 2) Ищем последний непустой заголовок
    last_idx = -1
    for i in range(len(headers) - 1, -1, -1):
        if str(headers[i]).strip() != "":
            last_idx = i
            break

    # 3) Если не было ни одного заголовка — пишем в start_col
    if last_idx == -1:
        ws.update(values=[[today]], range_name=f"{start_col_letter}2")
        return start_col

    # 4) Иначе — следующий столбец справа
    next_col = start_col + last_idx + 1

    # Гарантируем, что этот столбец существует
    ensure_min_cols(ws, next_col)

    ws.update(values=[[today]], range_name=f"{col_to_letter(next_col)}2")
    return next_col

# ======== ВАЛИДАЦИЯ SKU ========
def is_valid_sku(val: str) -> bool:
    """
    Проверяет, является ли значение валидным SKU для запроса.
    Здесь считаем, что SKU Ozon - это только цифры.
    Если строка содержит буквы, эмодзи или пуста - возвращаем False.
    """
    if not val:
        return False
    # Проверка: состоит ли строка только из цифр
    return val.isdigit()

# ======== OZON API ========
def to_int_or_zero(v) -> int:
    try:
        n = float(v)
        if math.isfinite(n):
            return int(n)
    except Exception:
        pass
    return 0

def fetch_ozon_available(client_id: str, api_key: str, skus: List[str]) -> Dict[str, int]:
    """
    Возвращает sku -> available (int). Суммируется по складам.
    """
    url = "https://api-seller.ozon.ru/v1/analytics/stocks"
    result: Dict[str, int] = {}

    # Фильтруем пустые списки перед отправкой, на всякий случай
    if not skus:
        return result

    for i in range(0, len(skus), BATCH_SIZE):
        batch = skus[i : i + BATCH_SIZE]
        payload = {
            "skus": batch,
            "warehouse_ids": [],
            "limit": len(batch),
        }
        headers = {
            "Client-Id": client_id,
            "Api-Key": api_key,
            "Content-Type": "application/json",
        }

        resp = requests.post(url, json=payload, headers=headers, timeout=60)
        if not (200 <= resp.status_code < 300):
            print(f"[OZON] HTTP {resp.status_code}: {resp.text}")
        else:
            try:
                data = resp.json()
            except Exception as e:
                print(f"[OZON] Ошибка парсинга JSON: {e}")
                data = None

            items = (data or {}).get("items", [])
            for item in items:
                sku = str(item.get("sku", "")).strip()
                if not sku:
                    continue
                result.setdefault(sku, 0)
                result[sku] += to_int_or_zero(item.get("available_stock_count"))

        time.sleep(BATCH_DELAY_SEC)

    return result

# ======== ОСНОВНАЯ ЛОГИКА ========
def update_available_stocks():
    client_id, api_key, spreadsheet_id, sheet_name = read_api_txt("API.txt")
    ws = open_worksheet(spreadsheet_id, sheet_name, "credentials.json")

    # 1) SKU из диапазона A5:A
    colA = ws.col_values(1)  # колонка A
    colA = right_trim(colA)
    # Получаем сырые значения (включая эмодзи и текст)
    sku_values_raw = [str(v).strip() for v in (colA[4:] if len(colA) > 4 else [])]
    
    row_count = len(sku_values_raw)
    if row_count == 0:
        print("Нет данных ниже строки 4 — нечего обновлять.")
        return

    # 2) Формируем СПИСОК для отправки в Ozon (ТОЛЬКО цифры)
    # Исключаем эмодзи и заголовки, чтобы не ломать API
    skus_for_api = [v for v in sku_values_raw if is_valid_sku(v)]

    # 3) Получаем данные из OZON только для валидных SKU
    sku_to_available = fetch_ozon_available(client_id, api_key, skus_for_api) if skus_for_api else {}

    # 4) Определяем/создаём «сегодняшний» столбец
    START_COL_X = letter_to_col("X")
    START_DATA_ROW = 5
    target_col = get_or_create_daily_column(ws, START_COL_X)

    # 4.1) Очищаем столбец этого дня
    clear_range = f"{col_to_letter(target_col)}{START_DATA_ROW}:{col_to_letter(target_col)}{ws.row_count}"
    ws.batch_clear([clear_range])

    # 5) Формируем вывод (сохраняя пустые строки там, где были эмодзи)
    def out_available_row(val: str):
        sku = val.strip()
        
        # ГЛАВНОЕ ИЗМЕНЕНИЕ: Если SKU не валидный (эмодзи, текст) - возвращаем пустоту
        if not is_valid_sku(sku):
            return [""]
            
        cnt = sku_to_available.get(sku)
        # Если данные пришли числом - пишем число, иначе пусто
        return [cnt if isinstance(cnt, int) else ""]

    end_row = START_DATA_ROW + row_count - 1
    rng_avail = f"{col_to_letter(target_col)}{START_DATA_ROW}:{col_to_letter(target_col)}{end_row}"
    
    # Проходимся по исходному списку, чтобы сохранить соответствие строк
    out_avail = [out_available_row(v) for v in sku_values_raw]

    ws.update(values=out_avail, range_name=rng_avail, value_input_option="USER_ENTERED")
    print(f"Готово. Данные за {today_ddmm()} обновлены в столбце {col_to_letter(target_col)}.")

if __name__ == "__main__":
    update_available_stocks()