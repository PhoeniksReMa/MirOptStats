from __future__ import annotations

from datetime import datetime, timedelta, timezone
from collections import defaultdict
from typing import DefaultDict, Dict, List, Optional, Tuple

import pytz

from ozon.services.ozon_client import OzonClient
from ozon.services.reporting import ensure_columns, get_or_create_report, upsert_rows
from shops.models import Shop


ORDER_COLUMNS = [
    ("posting_number", "Номер отправления", 10, "text"),
    ("order_number", "Номер заказа", 20, "text"),
    ("status", "Статус", 30, "text"),
    ("created_at", "Создан", 40, "date"),
    ("shipment_date", "Дата отгрузки", 50, "date"),
    ("product_name", "Товар", 60, "text"),
    ("sku", "SKU", 70, "text"),
    ("offer_id", "Артикул", 80, "text"),
    ("quantity", "Кол-во", 90, "number"),
    ("price", "Цена", 100, "number"),
]

MSK_TZ = pytz.timezone("Europe/Moscow")
UTC = timezone.utc
DAYS_WINDOW = 28
LOOKBACK_DAYS = 91
PERIOD_COLUMNS_FBO = {"FY": 7, "GA": 14, "GC": 28, "GE": 60, "GG": 90}
DELIVERED_COLUMNS_FBO = {"GO": 30, "GP": 60, "GQ": 90}
PERIOD_COLUMNS_FBS = {7: "FZ", 14: "GB", 28: "GD", 60: "GF", 90: "GH"}
DELIVERED_COLUMNS_FBS = {30: "GR", 60: "GS", 90: "GT"}


def _to_msk_dt(utc_iso: str) -> Optional[datetime]:
    if not utc_iso:
        return None
    fmt_candidates = ["%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"]
    dt = None
    for fmt in fmt_candidates:
        try:
            dt = datetime.strptime(utc_iso, fmt).replace(tzinfo=UTC)
            break
        except ValueError:
            continue
    if dt is None:
        try:
            dt = datetime.fromisoformat(utc_iso.replace("Z", "+00:00")).astimezone(UTC)
        except Exception:
            dt = None
    if dt is None:
        return None
    return dt.astimezone(MSK_TZ)


def _build_date_headers(base_msk_dt: datetime, days: int = DAYS_WINDOW) -> Tuple[List[str], List[str]]:
    base_date = base_msk_dt.date()
    headers: List[str] = []
    keys: List[str] = []
    for i in reversed(range(days)):
        d = base_date - timedelta(days=i)
        headers.append(d.strftime("%d.%m"))
        keys.append(d.strftime("%d.%m.%Y"))
    return headers, keys


def _col_to_index(col: str) -> int:
    col = (col or "").strip().upper()
    n = 0
    for ch in col:
        if not ("A" <= ch <= "Z"):
            raise ValueError(f"Bad column: {col!r}")
        n = n * 26 + (ord(ch) - ord("A") + 1)
    return n - 1


def _index_to_col(idx: int) -> str:
    if idx < 0:
        raise ValueError("Index must be >= 0")
    s = ""
    n = idx + 1
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(ord("A") + r) + s
    return s


def _col_range(start_col: str, count: int) -> List[str]:
    start_idx = _col_to_index(start_col)
    return [_index_to_col(start_idx + i) for i in range(count)]


def _month_ranges(start_utc: datetime, end_utc: datetime) -> List[Tuple[datetime, datetime]]:
    ranges = []
    current = start_utc
    while current < end_utc:
        if current.month == 12:
            next_month = current.replace(year=current.year + 1, month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        else:
            next_month = current.replace(month=current.month + 1, day=1, hour=0, minute=0, second=0, microsecond=0)
        ranges.append((current, min(next_month, end_utc)))
        current = next_month
    return ranges


def _iso_z(dt: datetime) -> str:
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _collect_fbo_postings(client: OzonClient, start_utc: datetime, end_utc: datetime) -> List[Dict]:
    items: List[Dict] = []
    for (m_start, m_end) in _month_ranges(start_utc, end_utc):
        offset = 0
        limit = 1000
        while True:
            payload = {
                "dir": "DESC",
                "filter": {"since": _iso_z(m_start), "to": _iso_z(m_end)},
                "limit": limit,
                "offset": offset,
                "translit": True,
                "with": {"analytics_data": True, "financial_data": True},
            }
            data = client.post("/v2/posting/fbo/list", payload)
            result = data.get("result", [])
            postings = result if isinstance(result, list) else result.get("postings", [])
            if not postings:
                break
            items.extend(postings)
            if len(postings) < limit:
                break
            offset += limit
    return items


def _aggregate_fbo(postings: List[Dict], run_started_msk: datetime) -> Tuple[List[str], List[str], Dict[str, List[int]], Dict[str, DefaultDict[str, int]], Dict[str, DefaultDict[str, int]]]:
    date_headers, date_keys = _build_date_headers(run_started_msk, DAYS_WINDOW)
    key_to_idx = {k: i for i, k in enumerate(date_keys)}

    day_counts_by_offer: Dict[str, List[int]] = {}
    period_counts_by_col: Dict[str, DefaultDict[str, int]] = {c: defaultdict(int) for c in PERIOD_COLUMNS_FBO}
    delivered_counts_by_col: Dict[str, DefaultDict[str, int]] = {c: defaultdict(int) for c in DELIVERED_COLUMNS_FBO}

    today_msk = run_started_msk.date()

    for p in postings:
        dt_msk = _to_msk_dt(p.get("in_process_at", ""))
        if not dt_msk:
            continue
        d_msk = dt_msk.date()
        d_key = dt_msk.strftime("%d.%m.%Y")
        idx = key_to_idx.get(d_key)
        for pr in p.get("products", []) or []:
            offer_id = str(pr.get("offer_id") or "").strip()
            qty = int(pr.get("quantity", 0) or 0)
            if not offer_id:
                continue
            if offer_id not in day_counts_by_offer:
                day_counts_by_offer[offer_id] = [0] * DAYS_WINDOW
            if idx is not None:
                day_counts_by_offer[offer_id][idx] += qty
            delta_days = (today_msk - d_msk).days
            if 0 <= delta_days <= 365:
                for col, days in PERIOD_COLUMNS_FBO.items():
                    if delta_days <= (days - 1):
                        period_counts_by_col[col][offer_id] += qty
                if (p.get("status") or "").lower() == "delivered":
                    for col, days in DELIVERED_COLUMNS_FBO.items():
                        if delta_days <= (days - 1):
                            delivered_counts_by_col[col][offer_id] += qty

    return date_headers, date_keys, day_counts_by_offer, delivered_counts_by_col, period_counts_by_col


def _collect_fbs_orders(client: OzonClient, since_iso: str, to_iso: str) -> List[Dict]:
    items: List[Dict] = []
    offset = 0
    limit = 1000
    while True:
        payload = {
            "dir": "DESC",
            "filter": {"since": since_iso, "to": to_iso},
            "limit": limit,
            "offset": offset,
            "with": {"analytics_data": False, "financial_data": False},
        }
        data = client.post("/v3/posting/fbs/list", payload)
        postings = data.get("result", {}).get("postings", []) or []
        if not postings:
            break
        items.extend(postings)
        if not data.get("result", {}).get("has_next", False):
            break
        offset += limit
    return items


def _prepare_fbs_orders(orders: List[Dict]) -> List[Dict]:
    prepped = []
    for o in orders:
        try:
            raw_dt = o.get("in_process_at", "")
            utc_dt = datetime.fromisoformat(raw_dt.replace("Z", "+00:00")).replace(tzinfo=UTC)
            msk_dt = utc_dt.astimezone(MSK_TZ)
            date_key = msk_dt.strftime("%d.%m")
            total_qty = sum(int(p.get("quantity", 0)) for p in o.get("products", []))
            prepped.append({
                "msk_dt": msk_dt,
                "date_key": date_key,
                "status": o.get("status"),
                "products": o.get("products", []),
                "total_qty": total_qty,
            })
        except Exception:
            continue
    return prepped


def _aggregate_fbs(prepped_orders: List[Dict]) -> Tuple[List[str], Dict[str, int], Dict[str, Dict[str, int]], Dict[int, Dict[str, int]], Dict[int, Dict[str, int]]]:
    now0 = datetime.now(MSK_TZ).replace(hour=0, minute=0, second=0, microsecond=0)
    order_periods = [7, 14, 28, 60, 90]
    delivered_periods = [30, 60, 90]
    starts = {d: now0 - timedelta(days=d - 1) for d in set(order_periods + delivered_periods)}
    end = now0 + timedelta(days=1)

    daily_total: Dict[str, int] = {}
    daily_by_offer: Dict[str, Dict[str, int]] = {}
    totals_by_period: Dict[int, Dict[str, int]] = {d: {} for d in order_periods}
    delivered_by_period: Dict[int, Dict[str, int]] = {d: {} for d in delivered_periods}

    for o in prepped_orders:
        date_key = o["date_key"]
        mdt = o["msk_dt"]
        status = o["status"]
        daily_total[date_key] = daily_total.get(date_key, 0) + o["total_qty"]

        for p in o["products"]:
            offer_id = p.get("offer_id")
            if not offer_id:
                continue
            offer_id = str(offer_id)
            qty = int(p.get("quantity", 0))
            daily_by_offer.setdefault(offer_id, {})
            daily_by_offer[offer_id][date_key] = daily_by_offer[offer_id].get(date_key, 0) + qty
            for d in order_periods:
                if starts[d] <= mdt < end:
                    totals_by_period[d][offer_id] = totals_by_period[d].get(offer_id, 0) + qty
            if status == "delivered":
                for d in delivered_periods:
                    if starts[d] <= mdt < end:
                        delivered_by_period[d][offer_id] = delivered_by_period[d].get(offer_id, 0) + qty

    dates = [(now0 - timedelta(days=i)).strftime("%d.%m") for i in reversed(range(28))]
    return dates, daily_total, daily_by_offer, totals_by_period, delivered_by_period

def _date_range_days(days: int = 30):
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days)
    return start.strftime("%Y-%m-%dT%H:%M:%SZ"), end.strftime("%Y-%m-%dT%H:%M:%SZ")


def _collect_postings(client: OzonClient, path: str, days: int = 30) -> List[Dict]:
    start, end = _date_range_days(days)
    offset = 0
    limit = 1000
    items: List[Dict] = []
    while True:
        payload = {
            "dir": "DESC",
            "filter": {"since": start, "to": end},
            "limit": limit,
            "offset": offset,
            "with": {"analytics_data": True, "financial_data": True},
        }
        data = client.post(path, payload)
        result = data.get("result", [])
        postings = result if isinstance(result, list) else result.get("postings", [])
        if not postings:
            break
        items.extend(postings)
        if len(postings) < limit:
            break
        offset += limit
    return items


def _aggregate_by_offer(postings: List[Dict], days_windows: List[int]) -> Dict[str, Dict]:
    result: Dict[str, Dict] = {}
    now = datetime.now(timezone.utc)
    for p in postings:
        created = p.get("created_at") or p.get("in_process_at")
        try:
            dt = datetime.fromisoformat(created.replace("Z", "+00:00")) if isinstance(created, str) else None
        except Exception:
            dt = None
        if not dt:
            continue
        products = p.get("products", []) or []
        for prod in products:
            offer_id = str(prod.get("offer_id") or "").strip()
            if not offer_id:
                continue
            rec = result.setdefault(offer_id, {f"orders_{d}": 0 for d in days_windows})
            for d in days_windows:
                if dt >= now - timedelta(days=d):
                    rec[f"orders_{d}"] += int(prod.get("quantity") or 1)
    return result


def sync_orders_fbo_agg(shop: Shop) -> None:
    report = get_or_create_report(shop, "orders_fbo_agg", "Заказы FBO (агр.)", "Агрегация заказов FBO по офферу")
    ensure_columns(report, [
        ("offer_id", "Артикул", 10, "text"),
        ("orders_7", "Заказы 7д", 20, "number"),
        ("orders_14", "Заказы 14д", 30, "number"),
        ("orders_28", "Заказы 28д", 40, "number"),
        ("orders_60", "Заказы 60д", 50, "number"),
        ("orders_90", "Заказы 90д", 60, "number"),
    ])
    client = OzonClient(client_id=shop.client_id, api_key=shop.token)
    postings = _collect_postings(client, "/v2/posting/fbo/list", days=90)
    agg = _aggregate_by_offer(postings, [7, 14, 28, 60, 90])
    rows: Dict[str, Dict] = {}
    for offer_id, data in agg.items():
        rows[offer_id] = {"offer_id": offer_id, **data}
    upsert_rows(report, rows)


def sync_orders_fbs_agg(shop: Shop) -> None:
    report = get_or_create_report(shop, "orders_fbs_agg", "Заказы FBS (агр.)", "Агрегация заказов FBS по офферу")
    ensure_columns(report, [
        ("offer_id", "Артикул", 10, "text"),
        ("orders_7", "Заказы 7д", 20, "number"),
        ("orders_14", "Заказы 14д", 30, "number"),
        ("orders_28", "Заказы 28д", 40, "number"),
        ("orders_60", "Заказы 60д", 50, "number"),
        ("orders_90", "Заказы 90д", 60, "number"),
    ])
    client = OzonClient(client_id=shop.client_id, api_key=shop.token)
    postings = _collect_postings(client, "/v3/posting/fbs/list", days=90)
    agg = _aggregate_by_offer(postings, [7, 14, 28, 60, 90])
    rows: Dict[str, Dict] = {}
    for offer_id, data in agg.items():
        rows[offer_id] = {"offer_id": offer_id, **data}
    upsert_rows(report, rows)


def sync_orders_fbo_matrix(shop: Shop) -> None:
    report = get_or_create_report(shop, "orders_fbo_matrix", "Заказы FBO (таблица)", "DS..ET, FY..GG, GO..GQ")
    run_started_utc = datetime.now(UTC)
    run_started_msk = run_started_utc.astimezone(MSK_TZ)

    start_utc = (run_started_utc - timedelta(days=LOOKBACK_DAYS)).replace(tzinfo=UTC)
    end_utc = run_started_utc.replace(tzinfo=UTC)

    client = OzonClient(client_id=shop.client_id, api_key=shop.token)
    postings = _collect_fbo_postings(client, start_utc, end_utc)
    if not postings:
        return

    date_headers, _, day_counts_by_offer, delivered_by_col, period_by_col = _aggregate_fbo(postings, run_started_msk)

    columns = [("offer_id", "Артикул", 10, "text")]
    order = 20
    date_cols = _col_range("DS", DAYS_WINDOW)
    for i, col_key in enumerate(date_cols):
        columns.append((col_key, date_headers[i], order, "number"))
        order += 10
    for col_key in PERIOD_COLUMNS_FBO.keys():
        columns.append((col_key, col_key, order, "number"))
        order += 10
    for col_key in DELIVERED_COLUMNS_FBO.keys():
        columns.append((col_key, col_key, order, "number"))
        order += 10
    ensure_columns(report, columns)

    monitor = shop.ozon_reports.filter(code="monitor").first()
    if not monitor:
        return
    offer_ids = [str(r.data.get("offer_id")).strip() for r in monitor.rows.all() if r.data.get("offer_id")]
    if not offer_ids:
        return

    rows: Dict[str, Dict] = {}
    day_cols = date_cols
    for offer_id in offer_ids:
        day_values = day_counts_by_offer.get(offer_id, [0] * DAYS_WINDOW)
        data = {"offer_id": offer_id, "sort_key": "2"}
        for i, col_key in enumerate(day_cols):
            data[col_key] = day_values[i] if i < len(day_values) else 0
        for col_key in PERIOD_COLUMNS_FBO.keys():
            data[col_key] = int(period_by_col[col_key].get(offer_id, 0))
        for col_key in DELIVERED_COLUMNS_FBO.keys():
            data[col_key] = int(delivered_by_col[col_key].get(offer_id, 0))
        rows[offer_id] = data

    if offer_ids:
        totals = {"offer_id": "Итого/периоды", "sort_key": "0"}
        for i, col_key in enumerate(day_cols):
            totals[col_key] = sum(day_counts_by_offer.get(offer, [0] * DAYS_WINDOW)[i] for offer in offer_ids)
        today_msk_dt = run_started_msk.replace(hour=0, minute=0, second=0, microsecond=0)
        for col_key, days in DELIVERED_COLUMNS_FBO.items():
            start_period = (today_msk_dt - timedelta(days=days - 1)).strftime("%d.%m.%y")
            totals[col_key] = start_period
        rows["__row3__"] = totals

    upsert_rows(report, rows)


def sync_orders_fbs_matrix(shop: Shop) -> None:
    report = get_or_create_report(shop, "orders_fbs_matrix", "Заказы FBS (таблица)", "EV..FW, FZ..GH, GR..GT, GI..GM, GU..GW")
    client = OzonClient(client_id=shop.client_id, api_key=shop.token)

    now_msk = datetime.now(MSK_TZ)
    since = (now_msk - timedelta(days=LOOKBACK_DAYS)).astimezone(UTC).isoformat()
    to = now_msk.astimezone(UTC).isoformat()
    orders = _collect_fbs_orders(client, since, to)
    if not orders:
        return

    prepped = _prepare_fbs_orders(orders)
    dates, daily_total, daily_by_offer, totals_by_period, delivered_by_period = _aggregate_fbs(prepped)

    columns = [("offer_id", "Артикул", 10, "text")]
    order = 20
    date_cols = _col_range("EV", DAYS_WINDOW)
    for i, col_key in enumerate(date_cols):
        columns.append((col_key, dates[i], order, "number"))
        order += 10
    for days, col in PERIOD_COLUMNS_FBS.items():
        columns.append((col, col, order, "number"))
        order += 10
    for days, col in DELIVERED_COLUMNS_FBS.items():
        columns.append((col, col, order, "number"))
        order += 10
    for key in ["GI", "GJ", "GK", "GL", "GM"]:
        columns.append((key, key, order, "number"))
        order += 10
    for key in ["GU", "GV", "GW"]:
        columns.append((key, key, order, "number"))
        order += 10
    ensure_columns(report, columns)

    monitor = shop.ozon_reports.filter(code="monitor").first()
    if not monitor:
        return
    offer_ids = [str(r.data.get("offer_id")).strip() for r in monitor.rows.all() if r.data.get("offer_id")]
    if not offer_ids:
        return

    fbo_report = shop.ozon_reports.filter(code="orders_fbo_matrix").first()
    fbo_by_offer: Dict[str, Dict] = {}
    if fbo_report:
        for r in fbo_report.rows.all():
            offer_id = str(r.data.get("offer_id") or "").strip()
            if not offer_id:
                continue
            fbo_by_offer[offer_id] = r.data

    rows: Dict[str, Dict] = {}
    day_cols = date_cols
    for offer_id in offer_ids:
        data = {"offer_id": offer_id, "sort_key": "2"}
        by_day = daily_by_offer.get(offer_id, {})
        for i, col_key in enumerate(day_cols):
            date_label = dates[i] if i < len(dates) else ""
            data[col_key] = int(by_day.get(date_label, 0))
        for days, col in PERIOD_COLUMNS_FBS.items():
            data[col] = int(totals_by_period[days].get(offer_id, 0))
        for days, col in DELIVERED_COLUMNS_FBS.items():
            data[col] = int(delivered_by_period[days].get(offer_id, 0))
        fbo_row = fbo_by_offer.get(offer_id, {})
        data["GI"] = int(fbo_row.get("FY", 0)) + int(data.get("FZ", 0))
        data["GJ"] = int(fbo_row.get("GA", 0)) + int(data.get("GB", 0))
        data["GK"] = int(fbo_row.get("GC", 0)) + int(data.get("GD", 0))
        data["GL"] = int(fbo_row.get("GE", 0)) + int(data.get("GF", 0))
        data["GM"] = int(fbo_row.get("GG", 0)) + int(data.get("GH", 0))
        data["GU"] = int(fbo_row.get("GO", 0)) + int(data.get("GR", 0))
        data["GV"] = int(fbo_row.get("GP", 0)) + int(data.get("GS", 0))
        data["GW"] = int(fbo_row.get("GQ", 0)) + int(data.get("GT", 0))
        rows[offer_id] = data

    totals = {"offer_id": "Итого/периоды", "sort_key": "0"}
    for i, col_key in enumerate(day_cols):
        day_key = dates[i] if i < len(dates) else ""
        totals[col_key] = int(daily_total.get(day_key, 0))
    for days, col in DELIVERED_COLUMNS_FBS.items():
        start_period = (now_msk.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=days - 1)).strftime("%d.%m.%y")
        totals[col] = start_period
    rows["__row3__"] = totals

    upsert_rows(report, rows)


def sync_orders_fbo(shop: Shop) -> None:
    if not shop.client_id or not shop.token:
        return
    report = get_or_create_report(shop, "orders_fbo", "Заказы FBO", "Список отправлений FBO")
    ensure_columns(report, ORDER_COLUMNS)

    client = OzonClient(client_id=shop.client_id, api_key=shop.token)
    postings = _collect_postings(client, "/v2/posting/fbo/list")
    rows: Dict[str, Dict] = {}
    for p in postings:
        posting_number = p.get("posting_number") or p.get("posting_number") or ""
        key = str(posting_number)
        products = p.get("products", []) or []
        if products:
            prod = products[0]
            rows[key] = {
                "posting_number": posting_number,
                "order_number": p.get("order_number"),
                "status": p.get("status"),
                "created_at": p.get("created_at"),
                "shipment_date": p.get("shipment_date"),
                "product_name": prod.get("name"),
                "sku": prod.get("sku"),
                "offer_id": prod.get("offer_id"),
                "quantity": prod.get("quantity"),
                "price": prod.get("price"),
                "sort_key": p.get("created_at") or "",
            }
        else:
            rows[key] = {
                "posting_number": posting_number,
                "order_number": p.get("order_number"),
                "status": p.get("status"),
                "created_at": p.get("created_at"),
                "shipment_date": p.get("shipment_date"),
                "sort_key": p.get("created_at") or "",
            }
    upsert_rows(report, rows)


def sync_orders_fbs(shop: Shop) -> None:
    if not shop.client_id or not shop.token:
        return
    report = get_or_create_report(shop, "orders_fbs", "Заказы FBS", "Список отправлений FBS")
    ensure_columns(report, ORDER_COLUMNS)

    client = OzonClient(client_id=shop.client_id, api_key=shop.token)
    postings = _collect_postings(client, "/v3/posting/fbs/list")
    rows: Dict[str, Dict] = {}
    for p in postings:
        posting_number = p.get("posting_number") or ""
        key = str(posting_number)
        products = p.get("products", []) or []
        if products:
            prod = products[0]
            rows[key] = {
                "posting_number": posting_number,
                "order_number": p.get("order_number"),
                "status": p.get("status"),
                "created_at": p.get("created_at"),
                "shipment_date": p.get("shipment_date"),
                "product_name": prod.get("name"),
                "sku": prod.get("sku"),
                "offer_id": prod.get("offer_id"),
                "quantity": prod.get("quantity"),
                "price": prod.get("price"),
                "sort_key": p.get("created_at") or "",
            }
        else:
            rows[key] = {
                "posting_number": posting_number,
                "order_number": p.get("order_number"),
                "status": p.get("status"),
                "created_at": p.get("created_at"),
                "shipment_date": p.get("shipment_date"),
                "sort_key": p.get("created_at") or "",
            }
    upsert_rows(report, rows)
