#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
OZON wrong-volume -> Google Sheets
Колонка L: product_id  -> колонка AA: "⚠️" (если найден в данных OZON)
Колонки U/V/W (мм)     -> колонка Z: литры = (U * V * W) / 1 000 000 (с 2 знаками)

Перед записью очищается диапазон Z5:Z.

API.txt (4 строки, без заголовков):
  1) Client-Id
  2) Api-Key
  3) Spreadsheet ID (между /d/ и /edit в URL)
  4) Имя листа (точно как на вкладке; эмодзи/пробелы допустимы)

Требуется credentials.json — service account JSON, которому дан доступ РЕДАКТОРА к таблице.

Зависимости:
  pip install requests gspread google-auth
"""

import json
import time
import re
from pathlib import Path
from typing import Tuple, Dict

import requests
import gspread
from google.oauth2.service_account import Credentials

API_URL = "https://api-seller.ozon.ru/v1/product/info/wrong-volume"
API_TXT = "API.txt"
CREDENTIALS_JSON = "credentials.json"


def read_config_from_api_txt(path: str) -> Tuple[str, str, str, str]:
    p = Path(path)
    if not p.exists():
        raise RuntimeError(f"Файл не найден: {path}")
    lines = [x.strip() for x in p.read_text(encoding="utf-8").splitlines() if x.strip()]
    if len(lines) < 4:
        raise RuntimeError(
            "API.txt должен содержать 4 строки без заголовков:\n"
            "1) Client-Id\n2) Api-Key\n3) Spreadsheet ID\n4) Имя листа"
        )
    client_id, api_key, spreadsheet_id, sheet_name = lines[:4]
    print(f"[CONF] API.txt ок. Таблица: {spreadsheet_id}; Лист: {sheet_name}")
    return client_id, api_key, spreadsheet_id, sheet_name


def fetch_ozon_all_products(client_id: str, api_key: str, limit: int = 1000) -> Dict[str, dict]:
    headers = {
        "Content-Type": "application/json",
        "Client-Id": client_id,
        "Api-Key": api_key,
    }
    cursor = ""
    page = 1
    by_pid: Dict[str, dict] = {}
    while True:
        payload = {"cursor": cursor, "limit": int(limit)}
        r = requests.post(API_URL, headers=headers, json=payload, timeout=60)
        if r.status_code != 200:
            try:
                err = r.json()
            except Exception:
                err = {"message": r.text}
            raise RuntimeError(f"OZON HTTP {r.status_code}: {err}")

        data = r.json() or {}
        items = data.get("products") or []
        print(f"[OZON] Страница {page}: {len(items)} позиций")
        for p in items:
            pid = p.get("product_id")
            if pid is None:
                continue
            by_pid[str(pid)] = p
        cursor = data.get("cursor") or ""
        if not cursor:
            break
        page += 1
        time.sleep(0.2)
    print(f"[OZON] Уникальных product_id: {len(by_pid)}")
    return by_pid


def open_sheet(credentials_json: str, spreadsheet_id: str, sheet_name: str):
    with open(credentials_json, "r", encoding="utf-8") as f:
        cred = json.load(f)
    if cred.get("type") != "service_account" or not cred.get("client_email"):
        raise RuntimeError("credentials.json не является сервисным аккаунтом.")
    svc_email = cred["client_email"]
    print(f"[AUTH] Сервисный аккаунт: {svc_email}")

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(cred, scopes=scopes)
    gc = gspread.authorize(creds)

    sh = gc.open_by_key(spreadsheet_id.strip())
    ws = sh.worksheet(sheet_name)
    return ws


def a1_quote_sheet_title(title: str) -> str:
    return "'" + title.replace("'", "''") + "'"


def _parse_mm(value: str):
    if value is None:
        return None
    s = str(value).strip().replace(",", ".")
    m = re.search(r"-?\d+(?:\.\d+)?", s)
    if not m:
        return None
    try:
        return float(m.group(0))
    except ValueError:
        return None


def update_sheet_with_flags(ws, ozon_by_pid: Dict[str, dict]) -> None:
    sheet_quoted = a1_quote_sheet_title(ws.title)

    # очистка Z5:Z
    clear_range = f"{sheet_quoted}!Z5:Z"
    ws.spreadsheet.values_update(
        clear_range,
        params={"valueInputOption": "RAW"},
        body={"values": []},
    )
    print("[SHEET] Диапазон Z5:Z очищен")

    # ---------- литры ----------
    colU = ws.col_values(21)
    colV = ws.col_values(22)
    colW = ws.col_values(23)

    max_len = max(len(colU), len(colV), len(colW))
    start_row = 5
    liters_updates = []
    if max_len >= start_row:
        for row in range(start_row, max_len + 1):
            u = _parse_mm(colU[row - 1] if row - 1 < len(colU) else "")
            v = _parse_mm(colV[row - 1] if row - 1 < len(colV) else "")
            w = _parse_mm(colW[row - 1] if row - 1 < len(colW) else "")
            if not u or not v or not w or u <= 0 or v <= 0 or w <= 0:
                continue
            liters = (u * v * w) / 1_000_000.0
            liters_rounded = round(liters, 2)  # ✅ число с 2 знаками
            liters_updates.append((row, liters_rounded))  # ✅ записываем float, не строку
    print(f"[SHEET] Расчитано литров для строк: {len(liters_updates)}")

    data = []
    if liters_updates:
        data.extend(
            {"range": f"{sheet_quoted}!Z{row}:Z{row}", "values": [[val]]}
            for row, val in liters_updates
        )

    # ---------- флаги ----------
    colL = ws.col_values(12)
    start_row_L = 5
    warn_updates = []
    if len(colL) >= start_row_L:
        for row in range(start_row_L, len(colL) + 1):
            raw = (colL[row - 1] or "").strip()
            if not raw:
                continue
            candidates = [raw]
            try:
                as_int = str(int(float(raw)))
                if as_int not in candidates:
                    candidates.append(as_int)
            except ValueError:
                pass
            if any(key in ozon_by_pid for key in candidates):
                warn_updates.append((row, "⚠️"))
    print(f"[SHEET] Совпадений по product_id: {len(warn_updates)}")

    if warn_updates:
        data.extend(
            {"range": f"{sheet_quoted}!AA{row}:AA{row}", "values": [[val]]}
            for row, val in warn_updates
        )

    # ---------- batch update ----------
    if data:
        ws.spreadsheet.values_batch_update({
            "valueInputOption": "RAW",  # RAW + float -> число в ячейке
            "data": data
        })
        print(f"[SHEET] Обновлено диапазонов: {len(data)}")
    else:
        print("[SHEET] Нет ячеек для обновления.")


def main():
    client_id, api_key, spreadsheet_id, sheet_name = read_config_from_api_txt(API_TXT)
    ozon_by_pid = fetch_ozon_all_products(client_id, api_key, limit=1000)
    ws = open_sheet(CREDENTIALS_JSON, spreadsheet_id, sheet_name)
    update_sheet_with_flags(ws, ozon_by_pid)
    print("Готово.")


if __name__ == "__main__":
    main()
