# -*- coding: utf-8 -*-
import json
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta, timezone
from pytz import timezone as tz
from collections import defaultdict
import logging
from typing import List, Tuple

# -------------------- Логирование --------------------
logging.basicConfig(
    level=logging.WARNING,  # только WARNING/ERROR/CRITICAL
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("ozon-gsheet-sync")

def mask(s: str, show: int = 4) -> str:
    if not s:
        return ""
    return s[:show] + "…" + ("*" * max(0, len(s) - show - 1))

def jdumps(data) -> str:
    try:
        return json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True)
    except Exception:
        return str(data)

# -------------------- Конфиг столбцов --------------------
# B=2; HH=216, HI=217, HJ=218, HK=219, HL=220, HM=221, HN=222
COL_OFFER_ID = 2
CLEAR_COLS = ["HI5:HI", "HK5:HK", "HM5:HM", "HN5:HN"]  # очищаем перед записью

# -------------------- Утилиты дат --------------------
def fmt_utc_z(dt: datetime) -> str:
    """2025-10-09T00:00:00Z"""
    dt_utc = dt.astimezone(timezone.utc)
    return dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

def chunk_ranges(start_dt: datetime, end_dt: datetime, days: int = 30) -> List[Tuple[datetime, datetime]]:
    """Дробим диапазон [start_dt, end_dt) на куски по N дней."""
    chunks = []
    cur = start_dt
    while cur < end_dt:
        nxt = min(cur + timedelta(days=days), end_dt)
        chunks.append((cur, nxt))
        cur = nxt
    return chunks

# -------------------- IO --------------------
def read_api_keys(filename='API.txt'):
    with open(filename, 'r', encoding='utf-8') as file:
        lines = [line.strip() for line in file if line.strip()]
        if len(lines) >= 4:
            client_id, api_key, sheet_id, target_sheet_name = lines[0], lines[1], lines[2], lines[3]
            logger.debug(f"API.txt прочитан: client_id={client_id}, api_key={mask(api_key)}, sheet_id={mask(sheet_id)}, sheet='{target_sheet_name}'")
            return client_id, api_key, sheet_id, target_sheet_name
        raise ValueError("Файл 'API.txt' должен содержать минимум 4 непустые строки (client_id, api_key, sheet_id, target_sheet_name).")

def setup_google_sheets(sheet_id, target_sheet_name):
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(sheet_id)

    try:
        worksheet = spreadsheet.worksheet(target_sheet_name)
        logger.info(f"Найден лист '{target_sheet_name}'")
    except gspread.exceptions.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=target_sheet_name, rows="1000", cols="300")
        logger.info(f"Создан новый лист '{target_sheet_name}'")

    worksheet.batch_clear(CLEAR_COLS)
    return client, spreadsheet, worksheet

# -------------------- Ozon API --------------------
def ozon_request(client_id, api_key, since_z, to_z, offset, limit):
    url = "https://api-seller.ozon.ru/v3/posting/fbs/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": api_key,
        "Content-Type": "application/json"
    }
    payload = {
        "dir": "DESC",
        "filter": {
            "since": since_z,
            "to": to_z,
            "status": "delivered"
        },
        "limit": limit,
        "offset": offset,
        "with": {
            "analytics_data": False,
            "financial_data": False
        }
    }

    logger.debug("---- Ozon REQUEST ----")
    logger.debug(f"URL: {url}")
    logger.debug(f"Headers: {jdumps({'Client-Id': client_id, 'Api-Key': mask(api_key)})}")
    logger.debug(f"Payload: {jdumps(payload)}")

    resp = requests.post(url, headers=headers, json=payload, timeout=60)

    logger.debug(f"HTTP {resp.status_code}")
    text_preview = resp.text if len(resp.text) < 5000 else resp.text[:5000] + "…[truncated]"
    logger.debug(f"Response body: {text_preview}")

    return resp

def get_ozon_orders(client_id, api_key, since_dt, to_dt):
    """Загружаем заказы; при PERIOD_IS_TOO_LONG дробим на 30-дневные чанки."""
    since_z = fmt_utc_z(since_dt)
    to_z = fmt_utc_z(to_dt)

    all_orders = []
    offset = 0
    limit = 1000

    resp = ozon_request(client_id, api_key, since_z, to_z, offset, limit)

    if not resp.ok:
        try:
            data = resp.json()
        except Exception:
            data = {}
        message = data.get("message")
        logger.error(f"Ozon API error {resp.status_code} | message={message} | raw={resp.text}")

        if message == "PERIOD_IS_TOO_LONG":
            logger.warning("PERIOD_IS_TOO_LONG — делим период по 30 дней.")
            chunks = chunk_ranges(since_dt, to_dt, days=30)
            for i, (c_from, c_to) in enumerate(chunks, start=1):
                cz_from, cz_to = fmt_utc_z(c_from), fmt_utc_z(c_to)
                logger.info(f"[Chunk {i}/{len(chunks)}] {c_from} .. {c_to} (Z: {cz_from} .. {cz_to})")
                offset = 0
                while True:
                    resp_c = ozon_request(client_id, api_key, cz_from, cz_to, offset, limit)
                    if not resp_c.ok:
                        logger.error(f"[Chunk {i}] HTTP {resp_c.status_code}: {resp_c.text}")
                        break
                    data_c = resp_c.json()
                    postings = data_c.get("result", {}).get("postings", [])
                    if not postings:
                        break
                    all_orders.extend(postings)
                    offset += limit
                    if not data_c.get("result", {}).get("has_next", False):
                        break
            logger.info(f"Получено заказов после чанкинга: {len(all_orders)}")
            return all_orders
        else:
            resp.raise_for_status()

    # основной проход
    data = resp.json()
    postings = data.get("result", {}).get("postings", [])
    all_orders.extend(postings)

    while data.get("result", {}).get("has_next", False):
        offset += limit
        resp = ozon_request(client_id, api_key, since_z, to_z, offset, limit)
        if not resp.ok:
            logger.error(f"Page fetch error {resp.status_code}: {resp.text}")
            break
        data = resp.json()
        postings = data.get("result", {}).get("postings", [])
        all_orders.extend(postings)

    logger.info(f"Итого получено заказов: {len(all_orders)}")
    return all_orders

# -------------------- Бизнес-логика --------------------
def get_last_year_month_delivered(orders, year, month):
    """Считаем delivered по offer_id за месяц в часовом поясе Мск."""
    moscow_tz = tz('Europe/Moscow')
    delivered_map = defaultdict(int)

    start_date = datetime(year, month, 1, tzinfo=moscow_tz)
    end_date = datetime(year + (1 if month == 12 else 0),
                        1 if month == 12 else month + 1,
                        1, tzinfo=moscow_tz)

    missed_orders = 0
    for order in orders:
        if order.get("status") != "delivered":
            continue
        try:
            raw_ts = order.get("in_process_at")
            if not raw_ts:
                continue
            utc_time = datetime.fromisoformat(raw_ts.replace('Z', '+00:00')).astimezone(timezone.utc)
            moscow_time = utc_time.astimezone(moscow_tz)
            if start_date <= moscow_time < end_date:
                for product in order.get("products", []):
                    offer_id = product.get("offer_id")
                    quantity_raw = product.get("quantity", 0)
                    try:
                        quantity = int(quantity_raw)
                    except Exception:
                        quantity = 0
                    if offer_id:
                        delivered_map[offer_id] += quantity
        except Exception as e:
            missed_orders += 1
            logger.warning(f"Ошибка обработки заказа: {e} | order_id={order.get('posting_number')}")

    period = f"{start_date.strftime('%d.%m.%y')} {end_date.strftime('%d.%m.%y')}"
    return delivered_map, period

def main():
    try:
        client_id, api_key, sheet_id, target_sheet_name = read_api_keys()
        client, spreadsheet, worksheet = setup_google_sheets(sheet_id, target_sheet_name)

        now = datetime.now(tz('Europe/Moscow'))

        last_year = now.year - 1
        # три будущих месяца относительно текущего — но прошлого года
        m1 = ((now.month) % 12) + 1
        m2 = ((now.month + 1) % 12) + 1
        m3 = ((now.month + 2) % 12) + 1
        months_needed = [m1, m2, m3]

        # Диапазон загрузки для API
        start_range = datetime(last_year, min(months_needed), 1, tzinfo=tz('Europe/Moscow'))
        end_range = datetime(
            last_year + (1 if max(months_needed) == 12 else 0),
            (max(months_needed) % 12) + 1, 1, tzinfo=tz('Europe/Moscow')
        )

        orders = get_ozon_orders(client_id, api_key, start_range, end_range)
        if not orders:
            logger.warning("Нет заказов от API (после всех попыток).")
            return

        # читаем offer_id из столбца B (начиная с 5-й строки)
        offer_column = worksheet.col_values(COL_OFFER_ID)[4:]  # B5:B
        n_rows = len(offer_column)

        # --------- СЧЁТ В ПАМЯТИ (БУФЕРЫ) ---------
        buf_HH = [0] * n_rows  # для месяца m1
        buf_HJ = [0] * n_rows  # для месяца m2
        buf_HL = [0] * n_rows  # для месяца m3

        # delivered по месяцам
        d1, p1 = get_last_year_month_delivered(orders, last_year, m1)
        d2, p2 = get_last_year_month_delivered(orders, last_year, m2)
        d3, p3 = get_last_year_month_delivered(orders, last_year, m3)

        # заполняем буферы по порядку offer_id
        offer_to_idx = {offer_id: i for i, offer_id in enumerate(offer_column)}
        for offer_id, qty in d1.items():
            idx = offer_to_idx.get(offer_id)
            if idx is not None:
                buf_HH[idx] = qty
        for offer_id, qty in d2.items():
            idx = offer_to_idx.get(offer_id)
            if idx is not None:
                buf_HJ[idx] = qty
        for offer_id, qty in d3.items():
            idx = offer_to_idx.get(offer_id)
            if idx is not None:
                buf_HL[idx] = qty

        # Итоговые суммы
        buf_SUM = [[buf_HH[i] + buf_HJ[i] + buf_HL[i]] for i in range(n_rows)]

        # Преобразуем буферы к values
        values_HI = [[v] for v in buf_HH]  # HI5:HI
        values_HK = [[v] for v in buf_HJ]  # HK5:HK
        values_HM = [[v] for v in buf_HL]  # HM5:HM

        # --------- ЕДИНОВРЕМЕННАЯ ЗАПИСЬ ---------
        # ВАЖНО: для множественного обновления значений используем values_batch_update,
        # а не batch_update. Формат body соответствует "spreadsheets.values.batchUpdate".
        sheet_title = worksheet.title
        body = {
            "valueInputOption": "RAW",
            "data": [
                {"range": f"'{sheet_title}'!HI3", "values": [[p1]]},
                {"range": f"'{sheet_title}'!HK3", "values": [[p2]]},
                {"range": f"'{sheet_title}'!HM3", "values": [[p3]]},
                {"range": f"'{sheet_title}'!HI5:HI", "values": values_HI},
                {"range": f"'{sheet_title}'!HK5:HK", "values": values_HK},
                {"range": f"'{sheet_title}'!HM5:HM", "values": values_HM},
                {"range": f"'{sheet_title}'!HN5:HN", "values": buf_SUM},
            ],
        }
        spreadsheet.values_batch_update(body)

        logger.warning(f"Записано разом: HI/HK/HM ({n_rows} строк), периоды (строка 3) и сумма в HN ({n_rows} строк).")

    except Exception as e:
        logger.error(f"Ошибка выполнения: {e}", exc_info=True)
    finally:
        logger.info("Скрипт завершён")

if __name__ == "__main__":
    main()
