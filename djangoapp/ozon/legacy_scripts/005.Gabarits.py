#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Ozon Seller API + Google Sheets интеграция через gspread
(только размеры и вес в граммах, с фиксированными колонками U–X)
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import requests
import gspread
from google.oauth2.service_account import Credentials

# -------------------------- Константы --------------------------

API_URL = "https://api-seller.ozon.ru/v4/product/info/attributes"
MAX_PRODUCTS_PER_REQUEST = 1000

# Заголовки таблицы (U–X)
OUTPUT_HEADERS = [
    "Длина (мм)",  # U
    "Ширина (мм)", # V
    "Высота (мм)", # W
    "Вес (г.)"     # X
]
OUTPUT_START_COL_LETTER = "U"

# -------------------------- Утилиты для колонок --------------------------

def _col_letter_from(letter: str, offset: int) -> str:
    return chr(ord(letter) + offset)

def _end_col_letter(start_letter: str, num_cols: int) -> str:
    return _col_letter_from(start_letter, num_cols - 1)

# -------------------------- Google Sheets --------------------------

def authenticate_google_sheets(credentials_file: Path) -> gspread.Client:
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    creds = Credentials.from_service_account_file(credentials_file, scopes=scopes)
    return gspread.authorize(creds)

def get_sheet_data(sheet, range_name: str) -> List[List[Any]]:
    return sheet.get(range_name)

def update_sheet_data(sheet, range_name: str, values: List[List[Any]]):
    sheet.update(values, range_name, value_input_option='RAW')
    print(f"Обновлено ячеек в диапазоне: {range_name}")

def read_product_ids_from_sheets(sheet, range_name: str = "L5:L") -> List[str]:
    data = get_sheet_data(sheet, range_name)
    product_ids: List[str] = []
    for row in data:
        if row and str(row[0]).isdigit():
            product_ids.append(str(row[0]))
    return product_ids

def prepare_sheet_headers(sheet, start_column: str = OUTPUT_START_COL_LETTER, start_row: int = 4):
    end_col = _end_col_letter(start_column, len(OUTPUT_HEADERS))
    header_range = f"{start_column}{start_row}:{end_col}{start_row}"
    update_sheet_data(sheet, header_range, [OUTPUT_HEADERS])

# -------------------------- Ozon API --------------------------

def read_api_config(path: Path):
    """
    Формат файла:
    client_id: XXXXX
    api_key: XXXXX
    spreadsheet_id: XXXXX
    sheet_name: XXXXX
    (также допустим '=')
    """
    raw = path.read_text(encoding="utf-8").strip().splitlines()

    def _extract(s: str) -> str:
        s = s.strip()
        for sep in (":", "="):
            if sep in s:
                s = s.split(sep, 1)[1]
                break
        return s.strip()

    return _extract(raw[0]), _extract(raw[1]), _extract(raw[2]), _extract(raw[3])

def build_payload(product_ids: List[str], limit: int = 1000, last_id: Optional[str] = None) -> Dict[str, Any]:
    payload: Dict[str, Any] = {"filter": {}, "limit": limit, "sort_dir": "ASC"}
    if product_ids:
        payload["filter"]["product_id"] = product_ids[:MAX_PRODUCTS_PER_REQUEST]
    if last_id:
        payload["last_id"] = last_id
    return payload

def make_headers(client_id: str, api_key: str) -> Dict[str, str]:
    return {"Client-Id": client_id, "Api-Key": api_key, "Content-Type": "application/json"}

def fetch_page(session: requests.Session, headers: Dict[str, str], payload: Dict[str, Any]) -> Dict[str, Any]:
    resp = session.post(API_URL, headers=headers, data=json.dumps(payload), timeout=60)
    data = resp.json()
    if resp.status_code != 200:
        raise Exception(f"Ошибка API ({resp.status_code}): {data.get('message')}")
    return data

def fetch_products_batch(session: requests.Session, headers: Dict[str, str], product_ids: List[str]) -> List[Dict[str, Any]]:
    all_products: List[Dict[str, Any]] = []
    for i in range(0, len(product_ids), MAX_PRODUCTS_PER_REQUEST):
        chunk = product_ids[i:i + MAX_PRODUCTS_PER_REQUEST]
        last_id: Optional[str] = None
        while True:
            payload = build_payload(chunk, limit=100, last_id=last_id)
            data = fetch_page(session, headers, payload)
            result = data.get("result") or []
            all_products.extend(result)
            last_id = data.get("last_id")
            if not last_id or not result:
                break
            time.sleep(0.1)
    return all_products

def chunked(seq: Sequence[str], n: int):
    for i in range(0, len(seq), n):
        yield list(seq[i: i + n])

# -------------------------- Обработка данных --------------------------

def _to_mm(value: Optional[float], unit: Optional[str]) -> Optional[float]:
    if value is None:
        return None
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None
    return v * 10.0 if unit == "cm" else v

def product_to_rows(product: Dict[str, Any]) -> List[List[Any]]:
    dim_unit = product.get("dimension_unit")
    height_mm = _to_mm(product.get("height"), dim_unit)
    width_mm = _to_mm(product.get("width"), dim_unit)
    depth_mm = _to_mm(product.get("depth"), dim_unit)

    row = [
        depth_mm or "",             # Длина → U
        width_mm or "",             # Ширина → V
        height_mm or "",            # Высота → W
        product.get("weight", ""),  # Вес (г.) → X
    ]
    return [row]

def write_products_to_sheets(sheet, products: List[Dict[str, Any]], start_row: int = 5) -> int:
    all_rows: List[List[Any]] = []
    for product in products:
        all_rows.extend(product_to_rows(product))
    if not all_rows:
        return 0
    start_col = OUTPUT_START_COL_LETTER
    end_col = _end_col_letter(start_col, len(all_rows[0]))
    range_name = f"{start_col}{start_row}:{end_col}{start_row + len(all_rows) - 1}"
    update_sheet_data(sheet, range_name, all_rows)
    return len(all_rows)

# -------------------------- Основная логика --------------------------

def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--config", default="API.txt", help="Путь к файлу с client_id, api_key, spreadsheet_id, sheet_name")
    p.add_argument("--credentials", default="credentials.json", help="Файл service account JSON")
    p.add_argument("--product-id-range", default="L5:L", help="Диапазон столбца с product_id")
    p.add_argument("--output-start-row", type=int, default=5, help="Начальная строка записи (по умолчанию 5)")
    p.add_argument("--batch-size", type=int, default=1000, help="Размер батча product_id для запроса к API")
    p.add_argument("--clear-output", action="store_true", help="Очистить диапазон вывода перед записью")
    p.add_argument("--delay", type=float, default=0.5, help="Пауза между батчами (сек.)")
    return p.parse_args(argv)

def main(argv: Optional[List[str]] = None) -> None:
    args = parse_args(argv)
    client_id, api_key, spreadsheet_id, sheet_name = read_api_config(Path(args.config))
    client = authenticate_google_sheets(Path(args.credentials))
    sheet = client.open_by_key(spreadsheet_id).worksheet(sheet_name)

    product_ids = read_product_ids_from_sheets(sheet, args.product_id_range)
    if not product_ids:
        print("Не найдено product_id для обработки")
        return

    # Заголовки в U–X, строка 4
    prepare_sheet_headers(sheet, OUTPUT_START_COL_LETTER, 4)

    if args.clear_output:
        last_row = len(sheet.get_all_values())
        end_col = _end_col_letter(OUTPUT_START_COL_LETTER, len(OUTPUT_HEADERS))
        clear_range = f"{OUTPUT_START_COL_LETTER}5:{end_col}{last_row}"  # U5:X{last_row}
        sheet.batch_clear([clear_range])

    headers = make_headers(client_id, api_key)
    session = requests.Session()

    current_row = args.output_start_row
    for batch in chunked(product_ids, args.batch_size):
        products = fetch_products_batch(session, headers, batch)
        written_rows = write_products_to_sheets(sheet, products, current_row)
        current_row += written_rows
        if args.delay > 0:
            time.sleep(args.delay)

if __name__ == "__main__":
    main()