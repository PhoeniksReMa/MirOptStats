import json
import requests
import gspread
from google.oauth2.service_account import Credentials

# =========================
# 1) Чтение авторизационных данных из файла
# =========================
def read_auth_data(filename='API.txt'):
    """
    Читает CLIENT_ID, API_KEY и ID таблицы из файла
    Структура файла:
    CLIENT_ID
    API-KEY
    ID ТАБЛИЦЫ
    """
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            lines = [line.strip() for line in f.readlines() if line.strip()]
        if len(lines) < 3:
            raise ValueError("Файл авторизации должен содержать минимум 3 строки: CLIENT_ID, API-KEY, ID ТАБЛИЦЫ")
        CLIENT_ID = lines[0]
        API_KEY = lines[1]
        SPREADSHEET_ID = lines[2]
        return CLIENT_ID, API_KEY, SPREADSHEET_ID
    except FileNotFoundError:
        raise FileNotFoundError(f"Файл авторизации {filename} не найден")

# =========================
# 2) Константы
# =========================
TARGET_SHEET_NAME = '⚙️'
TARGET_HEADER = ['Имя', 'ID']  # Заголовки столбцов
OZON_CLUSTER_URL = 'https://api-seller.ozon.ru/v1/cluster/list'

# Пустой список = запросить все кластеры выбранного типа
CLUSTER_IDS = []

# =========================
# 3) Утилиты
# =========================
def to_int(v):
    """Преобразует значение в целое число, возвращает None если невозможно"""
    if v is None or v == '':
        return None
    try:
        n = int(v)
        return n
    except (ValueError, TypeError):
        return None

# =========================
# 4) Запись в Google Sheets
# =========================
def write_to_sheet(rows, spreadsheet_id):
    """Записывает данные в Google Sheets начиная с 3 строки"""
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    try:
        creds = Credentials.from_service_account_file('credentials.json', scopes=scope)
        client = gspread.authorize(creds)

        # Открываем таблицу и лист
        spreadsheet = client.open_by_key(spreadsheet_id)
        try:
            worksheet = spreadsheet.worksheet(TARGET_SHEET_NAME)
        except gspread.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=TARGET_SHEET_NAME, rows="100", cols="20")

        # Увеличим число строк при необходимости
        if worksheet.row_count < 1000:
            worksheet.add_rows(1000 - worksheet.row_count)

        # Очищаем диапазон A3:B1000
        worksheet.batch_clear(['A3:B1000'])

        # Заголовки во 2 строку
        worksheet.update(values=[[TARGET_HEADER[0]]], range_name='A2')  # "Имя" в A2
        worksheet.update(values=[[TARGET_HEADER[1]]], range_name='B2')  # "ID" в B2

        # Данные с 3 строки
        if rows:
            # Подготовим раздельные массивы для пакетной записи по столбцам
            data_to_write_names = [[row[0]] for row in rows]  # A
            data_to_write_ids = [[row[1]] for row in rows]    # B

            worksheet.update(values=data_to_write_names, range_name=f'A3:A{len(rows) + 2}')
            worksheet.update(values=data_to_write_ids, range_name=f'B3:B{len(rows) + 2}')

            # Попробуем установить числовой формат для столбца B ("ID")
            try:
                body = {
                    "requests": [
                        {
                            "repeatCell": {
                                "range": {
                                    "sheetId": worksheet.id,
                                    "startRowIndex": 2,  # с 3 строки (0-индексация)
                                    "endRowIndex": len(rows) + 2,
                                    "startColumnIndex": 1,  # Колонка B
                                    "endColumnIndex": 2
                                },
                                "cell": {
                                    "userEnteredFormat": {
                                        "numberFormat": {
                                            "type": "NUMBER"
                                        }
                                    }
                                },
                                "fields": "userEnteredFormat.numberFormat"
                            }
                        }
                    ]
                }
                spreadsheet.batch_update(body)
            except Exception as format_error:
                print(f"Не удалось установить числовой формат для колонки B: {format_error}")

    except Exception as e:
        raise Exception(f"Ошибка при работе с Google Sheets: {e}")

# Альтернативная упрощенная версия записи (если основная не работает)
def write_to_sheet_simple(rows, spreadsheet_id):
    """Упрощенная версия записи в таблицу (одной операцией, без форматирования)"""
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = Credentials.from_service_account_file('credentials.json', scopes=scope)
        client = gspread.authorize(creds)

        spreadsheet = client.open_by_key(spreadsheet_id)
        try:
            worksheet = spreadsheet.worksheet(TARGET_SHEET_NAME)
        except gspread.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=TARGET_SHEET_NAME, rows="100", cols="10")

        worksheet.batch_clear(['A3:B1000'])
        worksheet.update(values=[[TARGET_HEADER[0]]], range_name='A2')
        worksheet.update(values=[[TARGET_HEADER[1]]], range_name='B2')

        if rows:
            data_to_write = [[row[0], row[1]] for row in rows]
            worksheet.update(values=data_to_write, range_name=f'A3:B{len(rows) + 2}')

    except Exception as e:
        raise Exception(f"Ошибка при работе с Google Sheets: {e}")

# =========================
# 5) Работа с Ozon API
# =========================
def fetch_clusters(client_id: str, api_key: str, cluster_type: str) -> dict[int, str]:
    """
    Возвращает словарь {cluster_id: cluster_name} для заданного типа кластера.
    cluster_type: 'CLUSTER_TYPE_OZON' (Россия) или 'CLUSTER_TYPE_CIS' (СНГ)
    """
    payload = {
        "cluster_ids": CLUSTER_IDS,     # пустой список = все кластеры данного типа
        "cluster_type": cluster_type
    }
    headers = {
        'Client-Id': client_id,
        'Api-Key': api_key,
        'Content-Type': 'application/json'
    }

    resp = requests.post(OZON_CLUSTER_URL, headers=headers, json=payload, timeout=30)
    if resp.status_code != 200:
        raise Exception(f'Ozon API error {resp.status_code}: {resp.text}')

    try:
        data = resp.json()
    except json.JSONDecodeError as e:
        raise Exception(f'Не удалось распарсить ответ OZON API: {e}')

    clusters = data.get('clusters', []) if isinstance(data, dict) else []
    result: dict[int, str] = {}
    for c in clusters:
        if not isinstance(c, dict):
            continue
        cid = to_int(c.get('id'))
        name = c.get('name', '')
        if cid is not None and cid not in result:
            result[cid] = name
    return result

def sync_ozon_clusters():
    """Синхронизирует кластеры из России и СНГ и пишет их в Google Sheets."""
    CLIENT_ID, API_KEY, SPREADSHEET_ID = read_auth_data()

    # Два запроса: РФ и СНГ
    ru = fetch_clusters(CLIENT_ID, API_KEY, 'CLUSTER_TYPE_OZON')
    cis = fetch_clusters(CLIENT_ID, API_KEY, 'CLUSTER_TYPE_CIS')

    # Слияние и устранение дублей по id
    merged: dict[int, str] = {}
    merged.update(ru)
    for cid, name in cis.items():
        merged.setdefault(cid, name)

    # Преобразование к виду [Имя, ID] и сортировка по имени
    rows = [[name, cid] for cid, name in merged.items()]
    rows.sort(key=lambda r: (r[0] or '').lower())

    # Запись в таблицу
    write_to_sheet(rows, SPREADSHEET_ID)

    print(f"Успешно синхронизировано {len(rows)} кластеров (РФ + СНГ)")
    print(f"Данные записаны в таблицу {SPREADSHEET_ID} на лист '{TARGET_SHEET_NAME}'")
    print("Структура:")
    print("A2: Имя")
    print("B2: ID")
    print("A3:B...: данные кластеров")

# =========================
# 6) Точка входа
# =========================
if __name__ == "__main__":
    try:
        sync_ozon_clusters()
    except Exception as e:
        print(f"Ошибка: {e}")
