from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple

import pytz

from ozon.services.ozon_client import OzonClient
from ozon.services.reporting import ensure_columns, get_or_create_report, upsert_rows
from shops.models import Shop

MSK = pytz.timezone("Europe/Moscow")


def _last_year_month_range(month_offset: int) -> Tuple[datetime, datetime]:
    today = datetime.now(MSK)
    month = today.month + month_offset
    year = today.year
    while month > 12:
        month -= 12
        year += 1
    target_year = year - 1
    start = datetime(target_year, month, 1, tzinfo=MSK)
    if month == 12:
        end = datetime(target_year + 1, 1, 1, tzinfo=MSK) - timedelta(seconds=1)
    else:
        end = datetime(target_year, month + 1, 1, tzinfo=MSK) - timedelta(seconds=1)
    return start, end


def _delivered_fbo_counts(client: OzonClient, start: datetime, end: datetime) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    since = start.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    to = end.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    offset = 0
    limit = 1000
    while True:
        payload = {
            "dir": "DESC",
            "filter": {"since": since, "to": to, "status": "delivered"},
            "limit": limit,
            "offset": offset,
            "translit": False,
            "with": {"analytics_data": False, "financial_data": False},
        }
        data = client.post("/v2/posting/fbo/list", payload)
        result = data.get("result", [])
        postings = result if isinstance(result, list) else result.get("postings", [])
        if not postings:
            break
        for p in postings:
            for pr in p.get("products", []) or []:
                offer_id = pr.get("offer_id")
                if not offer_id:
                    continue
                counts[offer_id] = counts.get(offer_id, 0) + int(pr.get("quantity", 0))
        if len(postings) < limit:
            break
        offset += limit
    return counts


def _delivered_fbs_counts(client: OzonClient, start: datetime, end: datetime) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    since = start.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    to = end.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    offset = 0
    limit = 1000
    while True:
        payload = {
            "dir": "DESC",
            "filter": {"since": since, "to": to, "status": "delivered"},
            "limit": limit,
            "offset": offset,
            "with": {"analytics_data": False, "financial_data": False},
        }
        data = client.post("/v3/posting/fbs/list", payload)
        postings = data.get("result", {}).get("postings", [])
        if not postings:
            break
        for p in postings:
            for pr in p.get("products", []) or []:
                offer_id = pr.get("offer_id")
                if not offer_id:
                    continue
                counts[offer_id] = counts.get(offer_id, 0) + int(pr.get("quantity", 0))
        if len(postings) < limit:
            break
        offset += limit
    return counts


def sync_orders_fbo_old_year(shop: Shop) -> None:
    report = get_or_create_report(shop, "orders_fbo_old_year", "FBO прошлый год", "HH/HJ/HL как в таблице")
    columns = [
        ("offer_id", "Артикул", 10, "text"),
        ("HH", "HH", 20, "text"),
        ("HJ", "HJ", 30, "text"),
        ("HL", "HL", 40, "text"),
    ]
    ensure_columns(report, columns)

    client = OzonClient(client_id=shop.client_id, api_key=shop.token)
    periods = {
        "HH": _last_year_month_range(1),
        "HJ": _last_year_month_range(2),
        "HL": _last_year_month_range(3),
    }
    counts_by_col = {}
    ranges = {}
    for col, (start, end) in periods.items():
        counts_by_col[col] = _delivered_fbo_counts(client, start, end)
        ranges[col] = f"{start.strftime('%d.%m.%y')} {end.strftime('%d.%m.%y')}"

    monitor = shop.ozon_reports.filter(code="monitor").first()
    if not monitor:
        return
    offer_ids = [str(r.data.get("offer_id")).strip() for r in monitor.rows.all() if r.data.get("offer_id")]
    month_names = {
        1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель",
        5: "Май", 6: "Июнь", 7: "Июль", 8: "Август",
        9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь",
    }

    rows: Dict[str, Dict] = {
        "__month__": {
            "offer_id": "Месяц",
            "HH": month_names.get(periods["HH"][0].month, ""),
            "HJ": month_names.get(periods["HJ"][0].month, ""),
            "HL": month_names.get(periods["HL"][0].month, ""),
            "sort_key": "0",
        },
        "__range__": {
            "offer_id": "Период",
            "HH": ranges["HH"],
            "HJ": ranges["HJ"],
            "HL": ranges["HL"],
            "sort_key": "1",
        },
    }
    for offer_id in offer_ids:
        rows[offer_id] = {
            "offer_id": offer_id,
            "HH": counts_by_col["HH"].get(offer_id, 0),
            "HJ": counts_by_col["HJ"].get(offer_id, 0),
            "HL": counts_by_col["HL"].get(offer_id, 0),
            "sort_key": "2",
        }
    upsert_rows(report, rows)


def sync_orders_fbs_old_year(shop: Shop) -> None:
    report = get_or_create_report(shop, "orders_fbs_old_year", "FBS прошлый год", "HI/HK/HM/HN как в таблице")
    columns = [
        ("offer_id", "Артикул", 10, "text"),
        ("HI", "HI", 20, "number"),
        ("HK", "HK", 30, "number"),
        ("HM", "HM", 40, "number"),
        ("HN", "HN", 50, "number"),
    ]
    ensure_columns(report, columns)

    client = OzonClient(client_id=shop.client_id, api_key=shop.token)
    now = datetime.now(MSK)
    last_year = now.year - 1
    m1 = ((now.month) % 12) + 1
    m2 = ((now.month + 1) % 12) + 1
    m3 = ((now.month + 2) % 12) + 1

    def month_range(month: int):
        start = datetime(last_year, month, 1, tzinfo=MSK)
        end = datetime(last_year + (1 if month == 12 else 0), 1 if month == 12 else month + 1, 1, tzinfo=MSK)
        return start, end

    p1 = month_range(m1)
    p2 = month_range(m2)
    p3 = month_range(m3)
    d1 = _delivered_fbs_counts(client, *p1)
    d2 = _delivered_fbs_counts(client, *p2)
    d3 = _delivered_fbs_counts(client, *p3)

    monitor = shop.ozon_reports.filter(code="monitor").first()
    if not monitor:
        return
    offer_ids = [str(r.data.get("offer_id")).strip() for r in monitor.rows.all() if r.data.get("offer_id")]

    rows: Dict[str, Dict] = {
        "__range__": {
            "offer_id": "Период",
            "HI": f"{p1[0].strftime('%d.%m.%y')} {p1[1].strftime('%d.%m.%y')}",
            "HK": f"{p2[0].strftime('%d.%m.%y')} {p2[1].strftime('%d.%m.%y')}",
            "HM": f"{p3[0].strftime('%d.%m.%y')} {p3[1].strftime('%d.%m.%y')}",
            "HN": "",
            "sort_key": "0",
        },
    }
    for offer_id in offer_ids:
        h1 = d1.get(offer_id, 0)
        h2 = d2.get(offer_id, 0)
        h3 = d3.get(offer_id, 0)
        rows[offer_id] = {
            "offer_id": offer_id,
            "HI": h1,
            "HK": h2,
            "HM": h3,
            "HN": h1 + h2 + h3,
            "sort_key": "1",
        }
    upsert_rows(report, rows)
