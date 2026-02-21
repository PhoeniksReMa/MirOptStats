# -*- coding: utf-8 -*-
import asyncio
import aiohttp
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from datetime import datetime
import logging
import warnings
import sys
import os

# ===================== ЛОГИ (минимум) =====================
warnings.filterwarnings("ignore", message="file_cache is only supported with oauth2client<4.0.0")
logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)
logging.getLogger('googleapiclient.discovery').setLevel(logging.ERROR)

logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# ===================== КОНФИГ =====================
API_CREDENTIALS_FILE = 'API.txt'          # 4 строки: Client-Id, Api-Key, spreadsheetId, sheetName
SA_CREDENTIALS_FILE = 'credentials.json'  # сервисный аккаунт Google
OZON_PRODUCT_INFO_URL = 'https://api-seller.ozon.ru/v3/product/info/list'

# ===================== УТИЛИТЫ =====================
def read_api_credentials(file_path=API_CREDENTIALS_FILE):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            lines = [line.strip() for line in file if line.strip()]
            if len(lines) >= 4:
                return lines[0], lines[1], lines[2], lines[3]
            raise ValueError("Файл должен содержать 4 строки: Client-Id, Api-Key, ID таблицы, название листа")
    except Exception as e:
        logger.error(f"Ошибка чтения файла '{file_path}': {e}")
        sys.exit(1)

def format_date(iso_date):
    if not iso_date:
        return ''
    for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            return datetime.strptime(iso_date, fmt).strftime("%d.%m.%Y")
        except ValueError:
            continue
    return ''

def format_vat(v):
    if v is None or v == '':
        return ''
    try:
        if isinstance(v, str):
            s = v.strip()
            if s.endswith('%'):
                num = float(s[:-1].strip())
                return f"{int(round(num))}%"
            num = float(s.replace(',', '.'))
        else:
            num = float(v)
        if abs(num) <= 1:
            num *= 100.0
        return f"{int(round(num))}%"
    except Exception:
        return ''

def get_service():
    if not os.path.exists(SA_CREDENTIALS_FILE):
        raise FileNotFoundError(
            f"Не найден файл сервисного аккаунта: {SA_CREDENTIALS_FILE}. "
            f"Скачайте JSON и положите рядом со скриптом."
        )
    creds = service_account.Credentials.from_service_account_file(SA_CREDENTIALS_FILE)
    return build('sheets', 'v4', credentials=creds, cache_discovery=False)

def get_sheet_id(service, spreadsheet_id, sheet_name):
    meta = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    for s in meta.get('sheets', []):
        props = s.get('properties', {})
        if props.get('title') == sheet_name:
            return props.get('sheetId')
    raise RuntimeError(f"Лист '{sheet_name}' не найден")

# ===================== ЧТЕНИЕ ID ИЗ ТАБЛИЦЫ =====================
def get_product_ids_from_google_sheets(service, spreadsheet_id, sheet_name):
    try:
        rng = f"{sheet_name}!L5:L"   # читаем ID из L5:L
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=rng
        ).execute()

        ids = []
        for row in result.get('values', []):
            if not row:
                continue
            raw = str(row[0]).strip()
            if raw.isdigit():
                ids.append(int(raw))
        return ids
    except HttpError as he:
        logger.error(f"Google Sheets API (чтение ID): {he}")
        return []
    except Exception as e:
        logger.error(f"Ошибка получения ID из Google Sheets: {e}")
        return []

# ===================== ЗАПИСЬ В ТАБЛИЦУ =====================
def write_data_to_google_sheets(service, spreadsheet_id, sheet_name, product_ids, data):
    """
    ЧТЕНИЕ: L5:L — product_id (OZON ID)
    ЗАГОЛОВКИ (строка 4):
      C4=Наименование, K4=SKU, M4=OZON ID, N4=Дата создания, O4=Дата обновл., P4=НДС, Y4=Объем вес, JG4=Дата обновл.
    ЗАПИСЬ (с 5-й строки):
      A — картинка (IMAGE(url))
      C — Наименование (name)
      D — Super (✔️/❌)  # ИЗМЕНЕНО: было E, стало D
      K — SKU
      M — Штрихкод (barcode)
      N — Дата создания
      O — Дата обновл.
      P — НДС
      Y — Объем вес (volume_weight)
      JG — Дата обновл. (дублируем O)
    """
    try:
        data_dict = {item.get('id'): item for item in data}

        values_a, values_c, values_m, values_d, values_n, values_o, values_p, values_y, values_k = ([] for _ in range(9))

        for product_id in product_ids:
            item = data_dict.get(product_id, {}) or {}

            primary_image = item.get('primary_image', [])
            image_url = (primary_image[0] if isinstance(primary_image, list) and primary_image else primary_image)
            image_formula = (
                f'=IMAGE("{image_url}")'
                if image_url and isinstance(image_url, str) and image_url.startswith(('http://', 'https://'))
                else ''
            )

            barcodes = item.get('barcodes', [])
            barcode = barcodes[0] if (isinstance(barcodes, list) and barcodes) else ''

            is_super = '✔️' if item.get('is_super') is True else '❌' if item.get('is_super') is False else ''

            created_at = format_date(item.get('created_at'))
            updated_at = format_date(item.get('updated_at'))
            vat_str = format_vat(item.get('vat'))
            volume_weight = item.get('volume_weight') or ''
            name = item.get('name') or ''

            # Извлекаем SKU из sources[0].get('sku', 'N/A') если sources не пустой, иначе 'N/A'
            sources = item.get('sources', [])
            sku = sources[0].get('sku', 'N/A') if sources else 'N/A'

            values_a.append([image_formula])   # A – картинка
            values_c.append([name])            # C – Наименование
            values_m.append([barcode])         # M – Штрихкод
            values_d.append([is_super])        # D – Super ✔️/❌
            values_n.append([created_at])      # N – Дата создания
            values_o.append([updated_at])      # O – Дата обновл.
            values_p.append([vat_str])         # P – НДС
            values_y.append([volume_weight])   # Y – Объем вес
            values_k.append([sku])             # K – SKU

        # ===== Заголовки в 4-й строке =====
        headers_body = {
            'valueInputOption': 'USER_ENTERED',
            'data': [
                {'range': f"{sheet_name}!C4",  'values': [["Наименование"]]},
                {'range': f"{sheet_name}!K4",  'values': [["SKU"]]},
                {'range': f"{sheet_name}!M4",  'values': [["OZON ID"]]},
                {'range': f"{sheet_name}!N4",  'values': [["Дата создания"]]},
                {'range': f"{sheet_name}!O4",  'values': [["Дата обновл."]]},
                {'range': f"{sheet_name}!P4",  'values': [["НДС"]]},
                {'range': f"{sheet_name}!Y4",  'values': [["Объем вес"]]},
                {'range': f"{sheet_name}!JG4", 'values': [["Дата обновл."]]},  # дубликат заголовка
            ]
        }
        service.spreadsheets().values().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body=headers_body
        ).execute()

        # ===== Данные начиная с 5-й строки =====
        start_row = 5
        end_row = start_row + max(len(product_ids) - 1, 0)

        body = {
            'valueInputOption': 'USER_ENTERED',
            'data': [
                {'range': f"{sheet_name}!A{start_row}:A{end_row}",   'values': values_a},
                {'range': f"{sheet_name}!C{start_row}:C{end_row}",   'values': values_c},
                {'range': f"{sheet_name}!M{start_row}:M{end_row}",   'values': values_m},
                {'range': f"{sheet_name}!D{start_row}:D{end_row}",   'values': values_d},  # ИЗМЕНЕНО: было E, стало D
                {'range': f"{sheet_name}!N{start_row}:N{end_row}",   'values': values_n},
                {'range': f"{sheet_name}!O{start_row}:O{end_row}",   'values': values_o},  # основная запись
                {'range': f"{sheet_name}!JG{start_row}:JG{end_row}", 'values': values_o},  # дублируем в JG
                {'range': f"{sheet_name}!P{start_row}:P{end_row}",   'values': values_p},
                {'range': f"{sheet_name}!Y{start_row}:Y{end_row}",   'values': values_y},
                {'range': f"{sheet_name}!K{start_row}:K{end_row}",   'values': values_k},
            ]
        }

        service.spreadsheets().values().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body=body
        ).execute()

    except HttpError as he:
        logger.error(f"Google Sheets API (запись): {he}")
    except Exception as e:
        logger.error(f"Ошибка записи в Google Sheets: {e}")

# ===================== ЗАПРОС К OZON =====================
async def fetch_product_info(session, product_ids, client_id, api_key):
    try:
        headers = {
            'Client-Id': client_id,
            'Api-Key': api_key,
            'Content-Type': 'application/json'
        }
        payload = {"product_id": product_ids}
        async with session.post(
            OZON_PRODUCT_INFO_URL, headers=headers, json=payload, timeout=60
        ) as response:
            if response.status == 200:
                data = await response.json()
                return data.get('items', [])
            _ = await response.text()
            return []
    except asyncio.TimeoutError:
        return []
    except Exception:
        return []

# ===================== MAIN =====================
async def main():
    client_id, api_key, spreadsheet_id, sheet_name = read_api_credentials()

    service = get_service()

    product_ids = get_product_ids_from_google_sheets(service, spreadsheet_id, sheet_name)
    if not product_ids:
        logger.warning("Список product_id пуст. Проверь колонку L в таблице.")
        return

    async with aiohttp.ClientSession() as session:
        all_data = []
        for i in range(0, len(product_ids), 1000):
            batch = product_ids[i:i+1000]
            data = await fetch_product_info(session, batch, client_id, api_key)
            if data:
                all_data.extend(data)

        if all_data:
            write_data_to_google_sheets(service, spreadsheet_id, sheet_name, product_ids, all_data)
        else:
            logger.warning("Данные по товарам не получены или пусты.")

# ===================== ТОЧКА ВХОДА =====================
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logger.error(f"Критическая ошибка выполнения: {e}")
