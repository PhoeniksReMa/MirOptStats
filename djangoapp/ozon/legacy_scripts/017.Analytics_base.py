# -*- coding: utf-8 -*-
import requests
from datetime import datetime, timezone, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from decimal import Decimal, ROUND_HALF_UP


def read_api_config(filename: str) -> dict:
    """Чтение конфигурации API из файла."""
    with open(filename, 'r', encoding='utf-8') as file:
        lines = [line.strip() for line in file.readlines()]
    if len(lines) < 4:
        raise ValueError("Файл API.txt должен содержать 4 строки: client_id, api-key, Google spreadsheet ID, sheet name")
    return {
        'client_id': lines[0],
        'api_key': lines[1],
        'spreadsheet_id': lines[2],
        'sheet_name': lines[3],
    }


def fetch_product_queries_data(url: str, headers: dict, payload: dict) -> list:
    """Получить данные product-queries (батч до 1000 SKU)."""
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=30)
        if resp.status_code != 200:
            print(f"Response status: {resp.status_code} - {resp.text[:200]}")
            return []
        data = resp.json()
        return data.get('items', []) or []
    except Exception as e:
        print(f"Request error: {e}")
        return []


def chunks(lst, n):
    """Разбить список на чанки по n элементов."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def msk_utc_bounds_for_date(target_date):
    """Вернуть UTC-границы для календарного дня по МСК."""
    msk_offset = timedelta(hours=3)
    date_from = (datetime.combine(target_date, datetime.min.time()) - msk_offset).isoformat() + "Z"
    date_to = (datetime.combine(target_date, datetime.max.time()) - msk_offset - timedelta(seconds=1)).isoformat() + "Z"
    return date_from, date_to


def round_half_up(value):
    """Округление до целого: .5 уходит от нуля."""
    try:
        return int(Decimal(str(value)).quantize(Decimal('1'), rounding=ROUND_HALF_UP))
    except Exception:
        return 0


def main():
    # Конфиг
    try:
        config = read_api_config('API.txt')
        print("Configuration loaded successfully")
    except Exception as e:
        print(f"Error reading config file: {e}")
        return

    # Google Sheets
    scope = ["https://spreadsheets.google.com/feeds",
             "https://www.googleapis.com/auth/drive"]
    try:
        creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(config['spreadsheet_id']).worksheet(config['sheet_name'])
        print("Successfully connected to Google Sheets")
    except Exception as e:
        print(f"Error accessing Google Sheets: {e}")
        return

    # SKU: теперь колонка K (11-я), с 5-й строки
    try:
        sku_col = sheet.col_values(11)[4:]  # K5:K
        skus = [sku.strip() for sku in sku_col if sku.strip()]
        sku_to_row = {sku: idx + 5 for idx, sku in enumerate(sku_col) if sku.strip()}
        print(f"Got {len(sku_to_row)} SKU from Google Sheet")
        if not skus:
            print("SKU list is empty.")
            return
    except Exception as e:
        print(f"Error getting SKU: {e}")
        return

    # API Ozon
    url = "https://api-seller.ozon.ru/v1/analytics/product-queries"
    headers = {
        "Client-Id": config['client_id'],
        "Api-Key": config['api_key'],
        "Content-Type": "application/json",
    }

    # ---- Диапазон 28 дней с лагом 3 дня ----
    # Конец периода: сегодня - 3 дня (включительно, 23:59:59 MSK)
    # Начало периода: сегодня - 30 дней (включительно, 00:00:00 MSK)
    today_utc_date = datetime.now(timezone.utc).date()
    start_date = today_utc_date - timedelta(days=30)
    end_date = today_utc_date - timedelta(days=3)

    date_from, _ = msk_utc_bounds_for_date(start_date)
    _, date_to = msk_utc_bounds_for_date(end_date)

    date_str = f"{start_date.strftime('%d.%m.%Y')} — {end_date.strftime('%d.%m.%Y')} (28 дней)"
    print(f"Data for {date_str}")

    # === Заголовок в объединённой ячейке IT3:IX3 ===
    try:
        try:
            sheet.merge_cells('IT3:IX3')
        except Exception:
            pass
        sheet.update(values=[[f"Данные за период {date_str}"]], range_name='IT3')
        print("Date range written to IT3:IX3")
    except Exception as e:
        print(f"Error writing date to IT3:IX3: {e}")

    # Сбор данных батчами
    all_items = []
    for i, batch in enumerate(chunks(skus, 1000), start=1):
        payload = {
            "date_from": date_from,
            "date_to": date_to,
            "skus": batch,
            "page_size": 1000,
            "sort_by": "BY_SEARCHES",
            "sort_dir": "DESCENDING",
        }
        items = fetch_product_queries_data(url, headers, payload)
        print(f"Batch {i}: {len(items)} records")
        all_items.extend(items)

    print(f"Total received {len(all_items)} records")

    # SKU -> данные
    sku_data = {str(it.get('sku', '')).strip(): it for it in all_items if it.get('sku')}

    updates = []
    missing_skus = []

    # >>> Запись метрик в IT:IX
    # IT: unique_search_users
    # IU: position
    # IV: unique_view_users
    # IW: view_conversion
    # IX: gmv (округлён до целого)
    for sku, row in sku_to_row.items():
        if sku in sku_data:
            it = sku_data[sku]
            values = [
                it.get('unique_search_users', 0),  # IT
                it.get('position', 0),             # IU
                it.get('unique_view_users', 0),    # IV
                it.get('view_conversion', 0),      # IW
                round_half_up(it.get('gmv', 0)),   # IX
            ]
        else:
            missing_skus.append(sku)
            values = [0, 0, 0, 0, 0]
        updates.append({'range': f"IT{row}:IX{row}", 'values': [values]})

    if missing_skus:
        print(f"Data not found for {len(missing_skus)} SKU")

    # Обновление данных
    if updates:
        try:
            sheet.batch_update(updates)
            print(f"Updated {len(updates)} rows of data (columns IT:IX)")
        except Exception as e:
            print(f"Error updating data: {e}")


if __name__ == "__main__":
    main()