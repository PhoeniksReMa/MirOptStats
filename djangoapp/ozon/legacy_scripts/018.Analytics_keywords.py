#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import math
import argparse
import threading
import time
from collections import defaultdict
from datetime import datetime, timedelta, time as dtime
from typing import Dict, List, Any, Tuple, Iterable, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from zoneinfo import ZoneInfo

# === Google Sheets ===
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

OZON_ENDPOINT = "https://api-seller.ozon.ru/v1/analytics/product-queries/details"
MSK = ZoneInfo("Europe/Moscow")


# -------------------- I/O helpers --------------------

def read_api_txt(path: str = "API.txt") -> Tuple[str, str, str, str]:
    with open(path, "r", encoding="utf-8") as f:
        lines = [line.strip() for line in f if line.strip()]
    if len(lines) < 4:
        raise ValueError("В API.txt должно быть 4 строки: client_id, api_key, spreadsheet_id, sheet_name")
    return lines[0], lines[1], lines[2], lines[3]


def build_sheets_service(credentials_path: str = "credentials.json"):
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = service_account.Credentials.from_service_account_file(credentials_path, scopes=scopes)
    return build("sheets", "v4", credentials=creds)


# -------------------- SKU reading --------------------

def read_skus_from_sheet(service, spreadsheet_id: str, sheet_name: str) -> List[str]:
    # Читаем SKU из K5:K и флаг отбора из IX5:IX
    ranges = [f"'{sheet_name}'!K5:K", f"'{sheet_name}'!IX5:IX"]
    resp = service.spreadsheets().values().batchGet(
        spreadsheetId=spreadsheet_id,
        ranges=ranges,
        majorDimension="ROWS"
    ).execute()

    k_vals = resp["valueRanges"][0].get("values", [])
    ix_vals = resp["valueRanges"][1].get("values", [])

    max_len = max(len(k_vals), len(ix_vals))
    while len(k_vals) < max_len:
        k_vals.append([])
    while len(ix_vals) < max_len:
        ix_vals.append([])

    skus: List[str] = []
    for i in range(max_len):
        sku = (k_vals[i][0].strip() if k_vals[i] else "")
        ix = (ix_vals[i][0].strip() if ix_vals[i] else "")
        if not sku:
            continue
        try:
            ix_num = float(str(ix).replace(",", "."))
        except ValueError:
            ix_num = 0.0
        if ix_num > 0:
            skus.append(sku.replace(" ", ""))

    return skus


# -------------------- Utilities --------------------

def chunked_iterable(lst: List[str], size: int) -> Iterable[List[str]]:
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


def compute_period() -> Tuple[str, str]:
    now_msk = datetime.now(MSK)
    start_date = now_msk.date() - timedelta(days=30)
    end_date = now_msk.date() - timedelta(days=3)
    date_from = datetime.combine(start_date, dtime(0, 0, 0), tzinfo=MSK)
    date_to = datetime.combine(end_date, dtime(23, 59, 59), tzinfo=MSK)
    return date_from.isoformat(timespec="seconds"), date_to.isoformat(timespec="seconds")


def build_session(timeout: int, max_retries: int) -> requests.Session:
    sess = requests.Session()
    retry = Retry(
        total=max_retries,
        read=max_retries,
        connect=max_retries,
        backoff_factor=1.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["POST"])
    )
    adapter = HTTPAdapter(pool_connections=32, pool_maxsize=64, max_retries=retry)
    sess.mount("https://", adapter)
    sess.mount("http://", adapter)
    sess.request = _wrap_request_with_timeout(sess.request, timeout)
    return sess


def _wrap_request_with_timeout(orig_request, timeout: int):
    def wrapped(method, url, **kwargs):
        if "timeout" not in kwargs:
            kwargs["timeout"] = timeout
        return orig_request(method, url, **kwargs)
    return wrapped


# -------------------- Ozon API --------------------

def fetch_all_pages(
    session: requests.Session,
    headers: Dict[str, str],
    skus: List[str],
    limit_by_sku: int = 15,
    page_size: int = 100,
) -> Dict[str, Any]:
    date_from, date_to = compute_period()
    page = 0
    all_queries: List[Dict[str, Any]] = []
    reported_total: Optional[int] = None
    reported_page_count: Optional[int] = None

    while True:
        payload = {
            "date_from": date_from,
            "date_to": date_to,
            "limit_by_sku": limit_by_sku,
            "page": page,
            "page_size": page_size,
            "skus": skus,
            "sort_by": "BY_GMV",
            "sort_dir": "DESCENDING",
        }

        resp = session.post(OZON_ENDPOINT, headers=headers, json=payload)
        resp.raise_for_status()
        data = resp.json()

        if reported_total is None:
            reported_total = data.get("total")
        if reported_page_count is None:
            reported_page_count = data.get("page_count")

        batch = data.get("queries") or []
        if not batch:
            break

        all_queries.extend(batch)

        if isinstance(reported_page_count, int) and page >= reported_page_count - 1:
            break
        page += 1

    return {
        "period": (date_from, date_to),
        "total": reported_total if reported_total is not None else len(all_queries),
        "page_count": reported_page_count,
        "queries": all_queries,
    }


# -------------------- Writing to Sheets --------------------

def write_period_to_iz3(service, spreadsheet_id: str, sheet_name: str,
                        date_from_iso: str, date_to_iso: str) -> None:
    start_dt = datetime.fromisoformat(date_from_iso)
    end_dt = datetime.fromisoformat(date_to_iso)
    days_inclusive = (end_dt.date() - start_dt.date()).days + 1
    text = f"Данные за период {start_dt:%d.%m.%Y} — {end_dt:%d.%m.%Y} ({days_inclusive} дней)"
    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=f"'{sheet_name}'!IZ3",
        valueInputOption="RAW",
        body={"values": [[text]]}
    ).execute()


def write_results_to_iz_jd_block(service, spreadsheet_id: str, sheet_name: str,
                                 all_queries: List[Dict[str, Any]]) -> None:
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for r in all_queries:
        gmv = r.get("gmv", 0)
        if isinstance(gmv, (int, float)) and gmv > 0:
            grouped[str(r.get("sku"))].append(r)

    for sku, items in grouped.items():
        items.sort(
            key=lambda x: (
                x.get("order_count", 0) or 0,
                x.get("gmv", 0) or 0,
                x.get("view_conversion", 0) or 0,
            ),
            reverse=True,
        )

    # Пробегаем строки по столбцу K (SKU)
    resp = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=f"'{sheet_name}'!K5:K"
    ).execute()
    k_vals = resp.get("values", [])

    updates = []
    clears = []

    for i, row in enumerate(k_vals, start=5):
        if not row:
            continue
        sku = row[0].strip().replace(" ", "")
        if not sku:
            continue

        if sku not in grouped:
            # чистим IZ:JD (блок из 5 столбцов)
            clears.append({"range": f"'{sheet_name}'!IZ{i}:JD{i}"})
            continue

        items = grouped[sku]
        col_q, col_pos, col_conv, col_ord = [], [], [], []

        for r in items:
            q = str(r.get("query") or "").replace("\n", " ").strip()
            pos = r.get("position")
            conv = r.get("view_conversion")
            conv_str = f"{float(conv):.2f}%" if isinstance(conv, (int, float)) else ""
            orders = r.get("order_count") or 0

            col_q.append(q)
            col_pos.append("" if pos is None else str(pos))
            col_conv.append(conv_str)
            col_ord.append(str(orders))

        joined_semicolon = "; ".join([q for q in col_q if q])

        # Обновляем диапазон IZ:JD (5 колонок)
        updates.append({
            "range": f"'{sheet_name}'!IZ{i}:JD{i}",
            "values": [[
                "\n".join(col_q),    # IZ: ключевые, столбик
                "\n".join(col_pos),  # JA: позиции
                "\n".join(col_conv), # JB: конверсия
                "\n".join(col_ord),  # JC: заказы
                joined_semicolon,    # JD: ключевые одной строкой через "; "
            ]]
        })

    if clears:
        service.spreadsheets().values().batchClear(
            spreadsheetId=spreadsheet_id,
            body={"ranges": [c["range"] for c in clears]}
        ).execute()

    if updates:
        service.spreadsheets().values().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"valueInputOption": "RAW", "data": updates}
        ).execute()
        print(f"Результаты записаны в столбцы IZ:JD листа '{sheet_name}'.")
    else:
        print("Нет данных для записи в IZ:JD.")


# -------------------- CLI --------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--api-file", default="API.txt")
    p.add_argument("--credentials", default="credentials.json")
    p.add_argument("--batch-size", type=int, default=50)
    p.add_argument("--threads", type=int, default=8)
    p.add_argument("--limit-by-sku", type=int, default=15)
    p.add_argument("--page-size", type=int, default=100)
    p.add_argument("--timeout", type=int, default=120)
    p.add_argument("--max-retries", type=int, default=5)
    return p.parse_args()


# -------------------- main --------------------

def main():
    start_time = time.perf_counter()

    args = parse_args()

    client_id, api_key, spreadsheet_id, sheet_name = read_api_txt(args.api_file)
    headers = {
        "Content-Type": "application/json",
        "Client-Id": client_id,
        "Api-Key": api_key,
    }

    service = build_sheets_service(args.credentials)
    skus = read_skus_from_sheet(service, spreadsheet_id, sheet_name)
    print(f"Отобрано SKU: {len(skus)}")
    if not skus:
        sys.exit(1)

    session = build_session(timeout=args.timeout, max_retries=args.max_retries)
    date_from, date_to = compute_period()
    print(f"Период (MSK): {date_from} — {date_to}")
    write_period_to_iz3(service, spreadsheet_id, sheet_name, date_from, date_to)

    batches = list(chunked_iterable(skus, max(1, args.batch_size)))
    total_batches = len(batches)
    print(f"Всего батчей: {total_batches} | Потоков: {args.threads}")

    from concurrent.futures import ThreadPoolExecutor, as_completed

    all_queries_threadsafe: List[Dict[str, Any]] = []
    lock = threading.Lock()

    def _worker(batch_idx: int, sku_batch: List[str]) -> int:
        try:
            result = fetch_all_pages(
                session=session,
                headers=headers,
                skus=sku_batch,
                limit_by_sku=args.limit_by_sku,
                page_size=args.page_size,
            )
            queries = result.get("queries", [])
            with lock:
                all_queries_threadsafe.extend(queries)
            print(f"[Батч {batch_idx}/{total_batches}] SKU={len(sku_batch)} → {len(queries)} записей")
            return len(queries)
        except Exception as e:
            print(f"[Батч {batch_idx}] Ошибка: {e}")
        return 0

    from concurrent.futures import ThreadPoolExecutor, as_completed
    with ThreadPoolExecutor(max_workers=max(1, args.threads)) as ex:
        futures = [ex.submit(_worker, idx, batch) for idx, batch in enumerate(batches, start=1)]
        done = 0
        step = max(1, math.ceil(total_batches / 10))
        for fut in as_completed(futures):
            _ = fut.result()
            done += 1
            if done % step == 0 or done == total_batches:
                print(f"Готово батчей: {done}/{total_batches}")

    write_results_to_iz_jd_block(service, spreadsheet_id, sheet_name, all_queries_threadsafe)
    elapsed = time.perf_counter() - start_time
    print(f"Готово. Время выполнения: {elapsed:.2f} секунд.")


if __name__ == "__main__":
    try:
        main()
    except HttpError as e:
        print(f"Google API error: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print("Прервано пользователем.")
        sys.exit(130)
