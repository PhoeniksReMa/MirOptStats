#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Ozon -> Google Sheets sync.
Изменения под ваш запрос:
- Сопоставляем по offer_id (строковое поле из API);
- Значения offer_id берём из колонки B (B5 и ниже);
- 28-дневное окно: DS:ET;
- файл кредов: API.txt
- В DIAPAZONE DS5:ET нули пишем, суммы считаем по числовой матрице
- УДАЛЕНО: подсчёт и запись отмен (колонки GY и HA)
"""

import os
import time
import logging
from collections import defaultdict, Counter
from datetime import datetime, timedelta, timezone, date
from typing import Dict, List, Tuple, DefaultDict, Optional

import pytz
import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from dateutil.relativedelta import relativedelta

# ======================
# Логирование отключено для скорости
# ======================
LOGGER_NAME = "ozon_sync"
logger = logging.getLogger(LOGGER_NAME)
logger.handlers = []
logger.propagate = False
logger.setLevel(logging.CRITICAL + 1)
logger.disabled = True
logging.getLogger("urllib3").setLevel(logging.CRITICAL + 1)
logging.getLogger("requests").setLevel(logging.CRITICAL + 1)

# ================
# Константы/настройки
# ================
MSK_TZ = pytz.timezone("Europe/Moscow")
UTC = timezone.utc

DAYS_WINDOW = 28
LIST_COLUMNS_RANGE = ("DS", "ET")

PERIOD_COLUMNS = {"FY": 7, "GA": 14, "GC": 28, "GE": 60, "GG": 90}
DELIVERED_COLUMNS = {"GO": 30, "GP": 60, "GQ": 90}
# УДАЛЕНО: CANCELLED_COLUMNS = {"GY": 7, "HA": 28}

SLEEP_BETWEEN_PAGES = 0.1
API_LIMIT = 1000
DEFAULT_TIMEOUT = 30
LOOKBACK_DAYS = 91


# =====================
# Вспомогательные функции
# =====================
def read_api_credentials(file_path: str = "API.txt") -> Tuple[str, str, str, str]:
    with open(file_path, "r", encoding="utf-8") as f:
        lines = [line.strip() for line in f if line.strip()]
    if len(lines) < 4:
        raise ValueError("Файл должен содержать 4 строки: client_id, API ключ, ID таблицы и название листа")
    client_id, api_key, sheet_id, sheet_name = lines[:4]
    return client_id, api_key, sheet_id, sheet_name


def to_msk_dt(utc_iso: str) -> Optional[datetime]:
    if not utc_iso:
        return None
    fmt_candidates = ['%Y-%m-%dT%H:%M:%S.%fZ', '%Y-%m-%dT%H:%M:%SZ']
    dt = None
    for fmt in fmt_candidates:
        try:
            dt = datetime.strptime(utc_iso, fmt).replace(tzinfo=UTC)
            break
        except ValueError:
            continue
    if dt is None:
        return None
    return dt.astimezone(MSK_TZ)


def msk_today_date_from(dt_msk: datetime) -> date:
    return date(year=dt_msk.year, month=dt_msk.month, day=dt_msk.day)


def get_month_ranges(start_dt_utc: datetime, end_dt_utc: datetime) -> List[Tuple[datetime, datetime]]:
    ranges = []
    current = start_dt_utc
    while current < end_dt_utc:
        next_month = current + relativedelta(months=1)
        ranges.append((current, min(next_month, end_dt_utc)))
        current = next_month
    return ranges


def iso_z(dt: datetime) -> str:
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def build_date_headers(base_msk_dt: datetime, days: int = DAYS_WINDOW) -> Tuple[List[str], List[str]]:
    base_date = base_msk_dt.date()
    headers: List[str] = []
    keys: List[str] = []
    for i in reversed(range(days)):
        d = base_date - timedelta(days=i)
        headers.append(d.strftime("%d.%m"))
        keys.append(d.strftime("%d.%m.%Y"))
    return headers, keys


# =====================
# Ozon API клиент
# =====================
class OzonAPI:
    BASE_URL = "https://api-seller.ozon.ru"

    def __init__(self, client_id: str, api_key: str, timeout: int = DEFAULT_TIMEOUT):
        self.client_id = client_id
        self.api_key = api_key
        self.timeout = timeout

        self.session = requests.Session()
        self.session.headers.update({
            "Client-Id": self.client_id,
            "Api-Key": self.api_key,
            "Content-Type": "application/json",
        })

        retry = Retry(
            total=5,
            backoff_factor=0.6,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["POST"]
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

        self.request_count = 0

    def get_fbo_posting_list(self, payload: dict) -> Optional[dict]:
        url = f"{self.BASE_URL}/v2/posting/fbo/list"
        try:
            self.request_count += 1
            resp = self.session.post(url, json=payload, timeout=self.timeout)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException:
            return None


# =====================
# Подготовка данных
# =====================
def collect_postings(api: OzonAPI, start_utc: datetime, end_utc: datetime, limit: int = API_LIMIT) -> List[dict]:
    all_postings: List[dict] = []
    month_ranges = get_month_ranges(start_utc, end_utc)

    for (m_start, m_end) in month_ranges:
        offset = 0
        while True:
            payload = {
                "dir": "DESC",
                "filter": {"since": iso_z(m_start), "to": iso_z(m_end)},
                "limit": limit,
                "offset": offset,
                "translit": True,
                "with": {"analytics_data": True, "financial_data": True},
            }
            data = api.get_fbo_posting_list(payload)
            if not data:
                break

            result = data.get("result", [])
            postings = result if isinstance(result, list) else result.get("postings", [])
            count = len(postings)

            if count == 0:
                break

            all_postings.extend(postings)
            offset += limit

            if count < limit:
                break

            if SLEEP_BETWEEN_PAGES > 0:
                time.sleep(SLEEP_BETWEEN_PAGES)

    return all_postings


def aggregate_counts(postings: List[dict], run_started_msk: datetime, days_window: int = DAYS_WINDOW):
    date_headers, date_keys = build_date_headers(run_started_msk, days_window)
    key_to_idx = {k: i for i, k in enumerate(date_keys)}

    day_counts_by_offer: Dict[str, List[int]] = defaultdict(lambda: [0] * days_window)
    period_counts_by_col: Dict[str, DefaultDict[str, int]] = {c: defaultdict(int) for c in PERIOD_COLUMNS}
    delivered_counts_by_col: Dict[str, DefaultDict[str, int]] = {c: defaultdict(int) for c in DELIVERED_COLUMNS}
    # УДАЛЕНО: cancelled_counts_by_col

    today_msk = msk_today_date_from(run_started_msk)

    for p in postings:
        dt_msk = to_msk_dt(p.get("in_process_at", ""))
        if not dt_msk:
            continue
        d_msk = dt_msk.date()
        d_key = dt_msk.strftime("%d.%m.%Y")

        idx = key_to_idx.get(d_key)
        products = p.get("products", [])
        for pr in products:
            offer_id = str(pr.get("offer_id") or "").strip()
            qty = int(pr.get("quantity", 0) or 0)

            if not offer_id:
                continue

            if idx is not None:
                day_counts_by_offer[offer_id][idx] += qty

            delta_days = (today_msk - d_msk).days
            if 0 <= delta_days <= 365:
                for col, days in PERIOD_COLUMNS.items():
                    if delta_days <= (days - 1):
                        period_counts_by_col[col][offer_id] += qty
                status = (p.get("status") or "").lower()
                if status == "delivered":
                    for col, days in DELIVERED_COLUMNS.items():
                        if delta_days <= (days - 1):
                            delivered_counts_by_col[col][offer_id] += qty
                # УДАЛЕНО: обработка status == "cancelled"

    return (
        date_headers, date_keys,
        day_counts_by_offer,
        delivered_counts_by_col,
        period_counts_by_col,
    )


# =====================
# Работа с Google Sheets
# =====================
def gsheets_client(creds_json_path: str):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_json_path, scope)
    return gspread.authorize(creds)


def clear_ranges(sheet, ranges: List[str]) -> None:
    if not ranges:
        return
    sheet.batch_clear(ranges)


def safe_batch_update(sheet, updates: List[dict]) -> None:
    if updates:
        sheet.batch_update(updates)


# =====================
# Основная логика
# =====================
def main():
    t0 = time.time()
    try:
        RUN_STARTED_UTC = datetime.now(UTC)
        RUN_STARTED_MSK = RUN_STARTED_UTC.astimezone(MSK_TZ)

        client_id, api_key, sheet_id, sheet_name = read_api_credentials("API.txt")
        gc = gsheets_client("credentials.json")
        sheet = gc.open_by_key(sheet_id).worksheet(sheet_name)

        # очистка
        clear_list = [
            f"{LIST_COLUMNS_RANGE[0]}3:{LIST_COLUMNS_RANGE[1]}",
            "FY5:FY", "GA5:GA", "GC5:GC", "GE5:GE", "GG5:GG",
            # УДАЛЕНО: "GY5:GY", "HA5:HA",
            "GO5:GO", "GP5:GP", "GQ5:GQ",
        ]
        clear_ranges(sheet, clear_list)

        today_utc = RUN_STARTED_UTC
        start_utc = (today_utc - timedelta(days=LOOKBACK_DAYS)).replace(tzinfo=UTC)
        end_utc = today_utc.replace(tzinfo=UTC)

        api = OzonAPI(client_id, api_key)
        postings = collect_postings(api, start_utc, end_utc, limit=API_LIMIT)
        if not postings:
            print("Постинги не найдены")
            return

        (
            date_headers, date_keys, day_counts_by_offer,
            delivered_by_col, period_by_col
        ) = aggregate_counts(postings, RUN_STARTED_MSK, DAYS_WINDOW)

        offers = sheet.col_values(2)[4:]
        last_row_index = len(offers)
        while last_row_index > 0 and not offers[last_row_index - 1]:
            last_row_index -= 1
        offers = offers[:last_row_index]
        if not offers:
            print("Список offer_id пуст")
            return

        all_updates = []
        # Шапка дат DS4:ET4
        all_updates.append({"range": f"{LIST_COLUMNS_RANGE[0]}4:{LIST_COLUMNS_RANGE[1]}4", "values": [date_headers]})

        # Числовая матрица 28 дней по офферам
        numeric_matrix_28 = []
        for offer in offers:
            if not offer:
                row_nums = [0] * DAYS_WINDOW
            else:
                row_nums = day_counts_by_offer.get(offer, [0] * DAYS_WINDOW)
            numeric_matrix_28.append(row_nums)

        # ПИШЕМ НУЛИ КАК ЕСТЬ
        matrix_28 = numeric_matrix_28
        all_updates.append({
            "range": f"{LIST_COLUMNS_RANGE[0]}5:{LIST_COLUMNS_RANGE[1]}{4 + len(matrix_28)}",
            "values": matrix_28
        })

        # Суммы по колонкам (DS3:ET3)
        if numeric_matrix_28:
            col_sums = [sum(col) for col in zip(*numeric_matrix_28)]
            all_updates.append({"range": f"{LIST_COLUMNS_RANGE[0]}3:{LIST_COLUMNS_RANGE[1]}3", "values": [col_sums]})

        # Запись периодов FY/GA/GC/GE/GG
        for col_name, _days in PERIOD_COLUMNS.items():
            col_values = [[int(period_by_col[col_name].get(offer, 0))] for offer in offers]
            all_updates.append({"range": f"{col_name}5:{col_name}{4 + len(offers)}", "values": col_values})

        # Запись delivered GO/GP/GQ и метки начала периода в строку 3
        for col_name, _days in DELIVERED_COLUMNS.items():
            col_values = [[int(delivered_by_col[col_name].get(offer, 0))] for offer in offers]
            all_updates.append({"range": f"{col_name}5:{col_name}{4 + len(offers)}", "values": col_values})

        today_msk_dt = RUN_STARTED_MSK.replace(hour=0, minute=0, second=0, microsecond=0)
        for col_name, days in DELIVERED_COLUMNS.items():
            start_period = (today_msk_dt - timedelta(days=days - 1)).strftime("%d.%m.%y")
            all_updates.append({"range": f"{col_name}3", "values": [[start_period]]})

        safe_batch_update(sheet, all_updates)

    except Exception as e:
        print("Критическая ошибка:", e)
    finally:
        elapsed = time.time() - t0
        print(f"Скрипт завершён за {elapsed:.2f} сек")


if __name__ == "__main__":
    main()
