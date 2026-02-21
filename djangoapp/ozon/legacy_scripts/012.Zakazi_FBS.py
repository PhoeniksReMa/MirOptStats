# -*- coding: utf-8 -*-
import logging
import warnings
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import gspread
import requests
from gspread.utils import a1_to_rowcol, rowcol_to_a1
from pytz import timezone as tz
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- Логи: показываем только ошибки ---
logging.basicConfig(
    level=logging.ERROR,  # <<< был INFO
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

warnings.filterwarnings(
    "ignore", category=DeprecationWarning, message="Parsing dates involving.*"
)

# ----------------------- ВСПОМОГАТЕЛЬНЫЕ ----------------------- #

def read_api_keys(filename="API.txt"):  # файл API.txt (ваше требование)
    """Файл: 4 строки — client_id, api_key, sheet_id, target_sheet_name."""
    with open(filename, "r", encoding="utf-8") as f:
        lines = [line.strip() for line in f if line.strip()]
    if len(lines) < 4:
        raise ValueError("Файл должен содержать минимум 4 строки")
    return lines[0], lines[1], lines[2], lines[3]


def setup_google_sheets(sheet_id, target_sheet_name):
    """Авторизация и подготовка листа (создаст при отсутствии, очистит диапазоны).
    ВНИМАНИЕ: отмены больше не трогаем — колонки GZ/HB не очищаются и не заполняются.
    """
    try:
        client = gspread.service_account(filename="credentials.json")
        spreadsheet = client.open_by_key(sheet_id)

        try:
            ws = spreadsheet.worksheet(target_sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            ws = spreadsheet.add_worksheet(title=target_sheet_name, rows="2000", cols="400")

        ws.batch_clear([
            "EV3:FW",           # 28-дневное окно
            "FZ5:FZ", "GB5:GB", "GD5:GD", "GF5:GF", "GH5:GH",  # периоды заказов (FBS)
            "GI5:GI", "GJ5:GJ", "GK5:GK", "GL5:GL", "GM5:GM",  # суммы FBO+FBS заказов
            # "GZ5:GZ", "HB5:HB",                              # отмены — БОЛЬШЕ НЕ ЧИСТИМ
            "GR5:GR", "GS5:GS", "GT5:GT",                      # доставленные (FBS)
            "GR3:GT3",                                         # шапка дат доставок
            "GU5:GU", "GV5:GV", "GW5:GW",                      # суммы доставок FBO+FBS
        ])
        return ws
    except Exception as e:
        logger.error(f"Ошибка Google Sheets: {e}")
        raise


def _build_session():
    """HTTP session с ретраями и таймаутами для Ozon API."""
    s = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=0.6,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("POST", "GET"),
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retries, pool_connections=10, pool_maxsize=10))
    return s


# ----------------------- OZON ----------------------- #

def get_ozon_orders(client_id, api_key, since_iso, to_iso):
    """Выгрузка FBS-постингов Ozon с пагинацией."""
    url = "https://api-seller.ozon.ru/v3/posting/fbs/list"
    headers = {"Client-Id": client_id, "Api-Key": api_key, "Content-Type": "application/json"}

    all_orders = []
    offset = 0
    limit = 1000
    session = _build_session()

    while True:
        payload = {
            "dir": "DESC",
            "filter": {"since": since_iso, "to": to_iso},
            "limit": limit,
            "offset": offset,
            "with": {"analytics_data": False, "financial_data": False},
        }
        try:
            r = session.post(url, headers=headers, json=payload, timeout=30)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            logger.error(f"Ошибка API Ozon: {e}")
            break

        postings = data.get("result", {}).get("postings", [])
        if not postings:
            break

        all_orders.extend(postings)
        offset += limit
        if not data.get("result", {}).get("has_next", False):
            break

    return all_orders


# ----------------------- АГРЕГАЦИИ ----------------------- #

def prepare_orders(orders):
    """Единоразово приводим даты к Москве, считаем ключи дат и суммарные количества в заказе."""
    msk = tz("Europe/Moscow")
    utc = timezone.utc

    prepped = []
    for o in orders:
        try:
            raw_dt = o.get("in_process_at", "")
            utc_dt = datetime.fromisoformat(raw_dt.replace("Z", "+00:00")).replace(tzinfo=utc)
            msk_dt = utc_dt.astimezone(msk)
            date_key = msk_dt.strftime("%d.%m")
            total_qty = sum(int(p.get("quantity", 0)) for p in o.get("products", []))
            prepped.append(
                {
                    "msk_dt": msk_dt,
                    "date_key": date_key,
                    "status": o.get("status"),
                    "products": o.get("products", []),
                    "total_qty": total_qty,
                }
            )
        except Exception as e:
            logger.error(f"Ошибка обработки даты: {o.get('in_process_at')} — {e}")
    return prepped


def build_date_range(days=28):
    msk = tz("Europe/Moscow")
    today = datetime.now(msk).replace(hour=0, minute=0, second=0, microsecond=0)
    return [(today - timedelta(days=i)).strftime("%d.%m") for i in reversed(range(days))]


def aggregate_all(prepped_orders):
    """
    - дневные суммы по всем заказам (для EV3) и по offer_id x день (для EV5..FW)
    - суммы по периодам заказов (7/14/28/60/90)
    - доставлено за 30/60/90
    (ОТМЕНЫ БОЛЬШЕ НЕ СЧИТАЕМ)
    """
    msk = tz("Europe/Moscow")
    now0 = datetime.now(msk).replace(hour=0, minute=0, second=0, microsecond=0)

    order_periods = [7, 14, 28, 60, 90]
    delivered_periods = [30, 60, 90]

    starts = {d: now0 - timedelta(days=d - 1) for d in set(order_periods + delivered_periods)}
    end = now0 + timedelta(days=1)

    daily_total = defaultdict(int)
    daily_by_offer = defaultdict(lambda: defaultdict(int))
    totals_by_period = {d: defaultdict(int) for d in order_periods}
    delivered_by_period = {d: defaultdict(int) for d in delivered_periods}

    for o in prepped_orders:
        date_key = o["date_key"]
        mdt = o["msk_dt"]
        status = o["status"]
        daily_total[date_key] += o["total_qty"]

        for p in o["products"]:
            offer_id = p.get("offer_id")
            if not offer_id:
                continue
            qty = int(p.get("quantity", 0))

            # Динамика по дням (28-дневное окно)
            daily_by_offer[offer_id][date_key] += qty

            # Периоды заказов (FBS)
            for d in (7, 14, 28, 60, 90):
                if starts[d] <= mdt < end:
                    totals_by_period[d][offer_id] += qty

            # Доставлено
            if status == "delivered":
                for d in (30, 60, 90):
                    if starts[d] <= mdt < end:
                        delivered_by_period[d][offer_id] += qty

    return daily_total, daily_by_offer, totals_by_period, delivered_by_period


# ----------------------- GOOGLE SHEETS I/O ----------------------- #

def _last_col_after(start_a1: str, add_cols: int) -> str:
    start_row, start_col = a1_to_rowcol(start_a1)
    end_col = start_col + add_cols - 1
    return rowcol_to_a1(1, end_col)[:-1]


def _batch_get_columns(ws, columns, start_row=5):
    ranges = [f"{c}{start_row}:{c}" for c in columns]
    values_list = ws.batch_get(ranges) if ranges else []
    out = {}
    for c, vals in zip(columns, values_list):
        col = [row[0] if row else "" for row in vals]
        out[c] = col
    return out


def _safe_int(x):
    try:
        return int(str(x).replace(" ", "").strip())
    except Exception:
        return 0


def write_to_sheet(ws, dates, daily_total, daily_by_offer,
                   totals_by_period, delivered_by_period):
    """
    Обновление листа через batch_update по новым столбцам.
    (Отмены не пишем вообще.)
    """
    updates = []
    written_cols_cache = {}

    # offer_id теперь в колонке B
    offer_column = ws.col_values(2)[4:]

    # 1) 28-дневное окно EV:FW
    updates.append({"range": "EV4", "values": [dates]})
    updates.append({"range": "EV3", "values": [[daily_total.get(d, 0) for d in dates]]})

    # EV5:FW — ПИШЕМ ВСЕ ЗНАЧЕНИЯ, ВКЛЮЧАЯ 0 (изменение тут)
    if offer_column:
        matrix = []
        for offer_id in offer_column:
            row = []
            by_day = daily_by_offer.get(offer_id, {})
            for d in dates:
                v = by_day.get(d, 0)
                row.append(v)  # <<< ключевое изменение: записываем и 0, и >0
            matrix.append(row)
        updates.append({"range": "EV5", "values": matrix})

    # 2) Периоды заказов (FBS)
    period_cols = {7: "FZ", 14: "GB", 28: "GD", 60: "GF", 90: "GH"}
    for days, col in period_cols.items():
        col_vals = [[totals_by_period[days].get(offer_id, 0)] for offer_id in offer_column]
        updates.append({"range": f"{col}5", "values": col_vals})
        written_cols_cache[col] = [v[0] for v in col_vals]

    # 3) Доставлено
    delivered_cols = {30: "GR", 60: "GS", 90: "GT"}
    for days, col in delivered_cols.items():
        vals = [[delivered_by_period[days].get(offer_id, 0)] for offer_id in offer_column]
        updates.append({"range": f"{col}5", "values": vals})
        written_cols_cache[col] = [v[0] for v in vals]

    # Шапка дат начала периодов доставок GR3:GT3
    today = datetime.now(tz("Europe/Moscow"))
    start_dates = [(today - timedelta(days=d - 1)).strftime("%d.%m.%y") for d in delivered_cols.keys()]
    updates.append({"range": "GR3:GT3", "values": [start_dates]})

    # 4) Суммы FBO+FBS по заказам (в памяти)
    left_for_sum = {"GI": "FY", "GJ": "GA", "GK": "GC", "GL": "GE", "GM": "GG"}
    right_for_sum = {"GI": "FZ", "GJ": "GB", "GK": "GD", "GL": "GF", "GM": "GH"}
    read_left_cols = _batch_get_columns(ws, list(left_for_sum.values()), start_row=5)

    for target_col, left_col in left_for_sum.items():
        right_col = right_for_sum[target_col]
        left_vals = [_safe_int(v) for v in read_left_cols.get(left_col, [])]
        right_vals = [_safe_int(v) for v in written_cols_cache.get(right_col, [])]
        n = max(len(left_vals), len(right_vals), len(offer_column))
        if len(left_vals) < n:
            left_vals += [0] * (n - len(left_vals))
        if len(right_vals) < n:
            right_vals += [0] * (n - len(right_vals))
        summed = [[l + r] for l, r in zip(left_vals[:len(offer_column)], right_vals[:len(offer_column)])]
        updates.append({"range": f"{target_col}5", "values": summed})

    # 5) Суммы FBO+FBS по доставкам (в памяти)
    sum_pairs_deliv = {"GU": ("GO", "GR"), "GV": ("GP", "GS"), "GW": ("GQ", "GT")}
    read_cols_deliv_left = _batch_get_columns(ws, ["GO", "GP", "GQ"], start_row=5)

    for target_col, (left_col, right_col) in sum_pairs_deliv.items():
        left_vals = [_safe_int(v) for v in read_cols_deliv_left.get(left_col, [])]
        right_vals = [_safe_int(v) for v in written_cols_cache.get(right_col, [])]
        n = max(len(left_vals), len(right_vals), len(offer_column))
        if len(left_vals) < n:
            left_vals += [0] * (n - len(left_vals))
        if len(right_vals) < n:
            right_vals += [0] * (n - len(right_vals))
        summed = [[l + r] for l, r in zip(left_vals[:len(offer_column)], right_vals[:len(offer_column)])]
        updates.append({"range": f"{target_col}5", "values": summed})

    # 6) Одна пачка обновлений
    if updates:
        ws.batch_update(updates)

    # 7) Форматирование (числовое)
    try:
        end_col = _last_col_after("EV1", len(dates))
        # EV3 (суммы по дням) и EV5:FW (матрица) — формат NUMBER не мешает пустым
        ws.format(f"EV3:{end_col}3", {"numberFormat": {"type": "NUMBER"}})
        ws.format(f"EV5:{end_col}", {"numberFormat": {"type": "NUMBER"}})

        to_format = (
            list(period_cols.values()) +
            # ["GZ", "HB"]                       # отмены — БОЛЬШЕ НЕ ФОРМАТИРУЕМ
            list(delivered_cols.values()) +
            ["GI", "GJ", "GK", "GL", "GM"] +
            ["GU", "GV", "GW"]
        )
        for col in to_format:
            ws.format(f"{col}5:{col}", {"numberFormat": {"type": "NUMBER"}})
    except Exception as fe:
        logger.error(f"Форматирование не применено: {fe}")


# ----------------------- MAIN ----------------------- #

def main():
    start_time = time.time()
    try:
        client_id, api_key, sheet_id, target_sheet_name = read_api_keys()

        ws = setup_google_sheets(sheet_id, target_sheet_name)

        msk = tz("Europe/Moscow")
        now_msk = datetime.now(msk)
        since = (now_msk - timedelta(days=91)).astimezone(timezone.utc).isoformat()
        to = now_msk.astimezone(timezone.utc).isoformat()

        orders = get_ozon_orders(client_id, api_key, since, to)
        if not orders:
            logger.error("Не получены заказы от API Ozon")
            return

        prepped = prepare_orders(orders)
        dates = build_date_range(days=28)
        daily_total, daily_by_offer, totals_by_period, delivered_by_period = aggregate_all(prepped)

        write_to_sheet(
            ws,
            dates,
            daily_total,
            daily_by_offer,
            totals_by_period,
            delivered_by_period,
        )
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}", exc_info=True)
    finally:
        elapsed = round(time.time() - start_time, 2)
        print(f"Скрипт завершил работу за {elapsed} сек.")  # <<< всегда показываем время


if __name__ == "__main__":
    main()
