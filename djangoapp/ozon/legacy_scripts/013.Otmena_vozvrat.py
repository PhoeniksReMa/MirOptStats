#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ОБНОВЛЁННЫЙ СКРИПТ (ТИХИЙ):

- В консоль выводит только одну строку: "Время выполнения: N сек".
- Все ошибки (ERROR + traceback) пишутся в файл script.log.
- Временные ошибки Ozon (сеть, 429, 5xx) НЕ роняют скрипт: после MAX_RETRIES
  возвращается пустой результат для страницы и пагинация просто заканчивается.
- Ошибки конфигурации (нет API.txt, credentials.json, листа и т.п.) — фатальные
  (скрипт завершится с кодом 1 и запишет подробности в script.log).
"""

import csv
import json
import sys
import time
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime, timedelta, timezone
import logging
import re
import random
from collections import Counter, defaultdict

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError, WorksheetNotFound

# ================== НАСТРОЙКИ ==================
API_URL = "https://api-seller.ozon.ru/v1/returns/list"
TIMEOUT = 30
MAX_RETRIES = 7               # терпеливее к временным проблемам
BACKOFF_BASE = 2.0            # экспоненциальный бэкофф
LIMIT = 500                   # максимум по документации

# Ограничение на количество страниц пагинации (защита от вечных циклов)
MAX_PAGES = 10000

# Управление созданием локальных файлов (по умолчанию — ничего не писать на диск)
LOG_TO_FILE = True                 # пишем лог на диск
SAVE_PER_OFFER_JSON = False        # если True — сохранять by_offer/<offer>.json
SAVE_LOCAL_AGGREGATES = False      # если True — сохранять returns_last30.json и returns_last30.csv
LOG_FILE = "script.log"            # имя лог-файла (ТОЛЬКО ОН)

# Управление записью в Google Sheets
WRITE_OUT_SHEET = False            # если True — писать подробную таблицу на отдельный лист
OUT_SHEET = "returns_last30"       # имя листа для подробной таблицы (если WRITE_OUT_SHEET=True)

# окно данных (для выгрузки)
WINDOW_DAYS = 30

# Файл авторизации Google (service account или OAuth client)
CREDENTIALS_FILE = "credentials.json"

# Задержка между страницами пагинации (чтобы не ловить лишний 429)
PAGE_DELAY_SEC = 0.1

# ================== ЛОГИ (ТОЛЬКО В ФАЙЛ, ТОЛЬКО ОШИБКИ) ==================
logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)
logger.handlers.clear()

if LOG_TO_FILE:
    fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    fh.setLevel(logging.ERROR)
    formatter = logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s")
    fh.setFormatter(formatter)
    logger.addHandler(fh)


# ================== СЕТЕВОЙ КЛИЕНТ ==================
def make_session() -> requests.Session:
    """
    Session с keep-alive и базовыми настройками пула.
    Логику повторов/бэкоффа делаем вручную в request_with_retries().
    """
    sess = requests.Session()

    retry = Retry(
        total=0,  # все ретраи выполняем сами
        connect=0,
        read=0,
        redirect=0,
        backoff_factor=0,
        raise_on_status=False,
    )

    adapter = HTTPAdapter(
        pool_connections=4,
        pool_maxsize=8,
        max_retries=retry,
    )
    sess.mount("https://", adapter)
    sess.mount("http://", adapter)
    return sess


# ================== OZON ==================
def read_api_file(path: Path = Path("API.txt")) -> Tuple[Dict[str, str], str, str]:
    """
    Возвращает:
      - заголовки OZON (Client-Id, Api-Key, Content-Type)
      - spreadsheet_id
      - sheet_name (лист с offer_id, где B5:B)
    """
    if not path.exists():
        logger.error(f"Не найден API.txt: {path.resolve()}")
        sys.exit(1)
    lines = [l.strip() for l in path.read_text(encoding="utf-8").splitlines() if l.strip()]
    if len(lines) < 4:
        logger.error("API.txt должен содержать 4 строки: client_id, api-key, spreadsheet_id, sheet_name")
        sys.exit(1)

    headers = {
        "Client-Id": lines[0],
        "Api-Key": lines[1],
        "Content-Type": "application/json",
    }
    spreadsheet_id = lines[2]
    sheet_name = lines[3]
    return headers, spreadsheet_id, sheet_name


def sleep_backoff(attempt: int, retry_after: Optional[str] = None) -> None:
    """
    Пауза перед повтором:
      - если Ozon дал Retry-After — уважаем его;
      - иначе — экспоненциальный бэкофф + небольшой джиттер.
    """
    if retry_after and retry_after.isdigit():
        delay = max(1.0, float(retry_after))
    else:
        delay = (BACKOFF_BASE ** attempt) + random.uniform(0, 0.3)
    time.sleep(delay)


def _log_transient(msg: str, attempt: int) -> None:
    """
    Логирование временных проблем (сейчас заглушка).
    """
    # logger.info(msg)
    pass


def request_with_retries(
    session: requests.Session,
    headers: Dict[str, str],
    payload: Dict[str, Any],
    last_id: int
) -> Dict[str, Any]:
    """
    Запрос к Ozon API с мягкими ретраями.
    Логика:
      - сетевые/временные ошибки → до MAX_RETRIES, потом возвращаем пустой результат (без исключения);
      - прочие коды 4xx/5xx → считаем фатальными по конфигурации и поднимаем исключение
        (обычно это неверные ключи/права/формат запроса).
    """
    attempt = 0
    while True:
        try:
            resp = session.post(API_URL, headers=headers, json=payload, timeout=TIMEOUT)
        except requests.RequestException as e:
            attempt += 1
            _log_transient(
                f"Сетевая ошибка: {e}; попытка {attempt}/{MAX_RETRIES}",
                attempt,
            )
            if attempt > MAX_RETRIES:
                # считаем, что по этой странице данных нет, но скрипт продолжает жить
                return {"returns": [], "has_next": False}
            sleep_backoff(attempt)
            continue

        # Успешный ответ
        if 200 <= resp.status_code < 300:
            try:
                return resp.json()
            except ValueError:
                logger.error("Ответ не JSON (вероятно, проблема на стороне API).")
                raise

        # Временные ошибки / троттлинг
        if resp.status_code in (429, 500, 502, 503, 504):
            attempt += 1
            _log_transient(
                f"Троттлинг/временная ошибка {resp.status_code}; повтор {attempt}/{MAX_RETRIES}",
                attempt,
            )
            if attempt > MAX_RETRIES:
                # прекращаем пагинацию
                return {"returns": [], "has_next": False}
            sleep_backoff(attempt, resp.headers.get("Retry-After"))
            continue

        # Все остальные коды 4xx/5xx считаем фатальными (обычно это проблемы с ключами/правами)
        logger.error(f"Фатальный ответ {resp.status_code}: {resp.text}")
        raise RuntimeError(f"HTTP {resp.status_code}")


def fetch_all_returns(
    session: requests.Session,
    headers: Dict[str, str],
    time_from_iso: str,
    time_to_iso: str,
) -> List[Dict[str, Any]]:
    """
    Глобальная пагинация: забираем все возвраты за указанный период (без фильтра по offer_id).

    ВАЖНО:
    - last_id в запросе = id последнего возврата из предыдущей страницы;
    - выходим, если:
        * has_next == False,
        * страница пустая,
        * id последнего элемента не меняется между запросами,
        * превышен MAX_PAGES (предохранитель от вечного цикла).
    """
    items: List[Dict[str, Any]] = []
    last_id: int = 0
    page_num: int = 0

    while True:
        payload = {
            "filter": {
                "visual_status_change_moment": {
                    "time_from": time_from_iso,
                    "time_to": time_to_iso,
                },
            },
            "limit": LIMIT,
            "last_id": last_id,
        }

        data = request_with_retries(session, headers, payload, last_id)
        chunk = data.get("returns", []) or []
        has_next = bool(data.get("has_next"))

        # Пустая страница — данных дальше нет
        if not chunk:
            break

        items.extend(chunk)

        # Новый last_id — ИМЕННО id последнего возврата на этой странице
        try:
            new_last_id_raw = chunk[-1].get("id")
            new_last_id = int(new_last_id_raw)
        except (TypeError, ValueError):
            logger.error(
                f"Некорректный id в последнем элементе страницы: {chunk[-1].get('id')!r}. "
                f"Останавливаю пагинацию."
            )
            break

        # Предохранитель: если id не меняется, выходим, чтобы не зациклиться
        if new_last_id == last_id:
            logger.error(f"last_id не изменился между страницами ({last_id}), прекращаю пагинацию.")
            break

        last_id = new_last_id
        page_num += 1

        # Доп. защита от бесконечной пагинации
        if page_num >= MAX_PAGES:
            logger.error(f"Достигнут лимит страниц MAX_PAGES={MAX_PAGES}, прекращаю пагинацию.")
            break

        if PAGE_DELAY_SEC:
            time.sleep(PAGE_DELAY_SEC)

        # Нормальное условие выхода по API
        if not has_next:
            break

    return items


def flatten_for_csv(item: Dict[str, Any]) -> Dict[str, Any]:
    product = item.get("product") or {}
    storage = item.get("storage") or {}
    logistic = item.get("logistic") or {}
    visual = item.get("visual") or {}
    vs = visual.get("status") or {}

    def p2s(p):
        if not p:
            return ""
        return f"{p.get('price','')} {p.get('currency_code','')}".strip()

    return {
        "return_id": item.get("id"),
        "schema": item.get("schema"),
        "type": item.get("type"),
        "order_id": item.get("order_id"),
        "order_number": item.get("order_number"),
        "posting_number": item.get("posting_number"),
        "sku": product.get("sku"),
        "offer_id": product.get("offer_id"),
        "product_name": product.get("name"),
        "qty": product.get("quantity"),
        "price": p2s(product.get("price")),
        "price_wo_commission": p2s(product.get("price_without_commission")),
        "commission_percent": product.get("commission_percent"),
        "commission": p2s(product.get("commission")),
        "storage_sum": p2s(storage.get("sum")),
        "storage_days": storage.get("days"),
        "storage_arrived": storage.get("arrived_moment"),
        "tariff_first": storage.get("tariffication_first_date"),
        "tariff_start": storage.get("tariffication_start_date"),
        "util_sum": p2s(storage.get("utilization_sum")),
        "util_forecast": storage.get("utilization_forecast_date"),
        "logistic_return_date": logistic.get("return_date"),
        "logistic_final_moment": logistic.get("final_moment"),
        "logistic_barcode": logistic.get("barcode"),
        "visual_status_id": (vs or {}).get("id"),
        "visual_status": (vs or {}).get("display_name"),
        "visual_sys_name": (vs or {}).get("sys_name"),
        "visual_change_moment": visual.get("change_moment"),
        "return_reason_name": item.get("return_reason_name"),
        "warehouse_place": (item.get("place") or {}).get("name"),
        "target_place": (item.get("target_place") or {}).get("name"),
        "is_opened": (item.get("additional_info") or {}).get("is_opened"),
        "is_super_econom": (item.get("additional_info") or {}).get("is_super_econom"),
        "company_id": item.get("company_id"),
    }


# ================== ВСПОМОГАТЕЛЬНОЕ: метрики для C…H ==================
CANCEL_KEYS = {"cancellation", "cancel", "cancelled", "canceled"}
RET_KEYS = {"partialreturn", "partial_return", "clientreturn", "client_return", "returnbyclient"}

_status_cleaner = re.compile(r"[\s_-]+")


def _norm(s: str) -> str:
    """Нормализация строки статуса: lower + убрать пробелы/подчёркивания/дефисы."""
    return _status_cleaner.sub("", (s or "").strip().lower())


def _parse_iso_utc(s: str) -> Optional[datetime]:
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s.replace("Z", "+00:00")
        return datetime.fromisoformat(s)
    except Exception:
        return None


def _best_when(item: Dict[str, Any]) -> Optional[datetime]:
    """Основная дата — визуальное изменение; если её нет — пробуем логистику."""
    visual = item.get("visual") or {}
    logistic = item.get("logistic") or {}
    for key in (
        (visual.get("change_moment") or ""),
        (logistic.get("final_moment") or ""),
        (logistic.get("return_date") or ""),
    ):
        dt = _parse_iso_utc(key)
        if dt:
            return dt
    return None


def _status_bucket(item: Dict[str, Any]) -> Optional[str]:
    """
    Возвращает:
      'cancel' для Cancellation,
      'ret' для PartialReturn/ClientReturn,
      None — если не распознано.
    Приоритет: item.type → visual.status.sys_name → visual.status.display_name
    """
    visual = item.get("visual") or {}
    st = (visual.get("status") or {})
    candidates = [
        item.get("type") or "",
        st.get("sys_name") or "",
        st.get("display_name") or "",
    ]
    for cand in candidates:
        n = _norm(cand)
        if any(n.startswith(k) or k in n for k in CANCEL_KEYS):
            return "cancel"
        if any(k in n for k in RET_KEYS):
            return "ret"
    return None


def compute_offer_metrics(items: List[Dict[str, Any]], now_utc: datetime) -> Tuple[int, int, str, int, int, str]:
    """
    Возвращает: (C, D, E_str, F, G, H_str)

      C — cancel за 14 дней
      D — cancel за 28 дней
      E — причины cancel за 28 дней (построчно в одной ячейке)

      F — partial+client за 14 дней
      G — partial+client за 28 дней
      H — причины partial+client за 28 дней (построчно в одной ячейке)
    """
    t14 = now_utc - timedelta(days=14)
    t28 = now_utc - timedelta(days=28)

    c14 = c28 = 0
    r14 = r28 = 0
    cancel_reasons_28 = Counter()
    ret_reasons_28 = Counter()

    for it in items:
        bucket = _status_bucket(it)  # 'cancel' | 'ret' | None
        when = _best_when(it)
        if not when:
            # Без даты не учитываем вообще, чтобы цифры и причины не разъезжались
            continue

        in_14 = when >= t14
        in_28 = when >= t28
        rr = (it.get("return_reason_name") or "").strip()

        if bucket == "cancel":
            if in_14:
                c14 += 1
            if in_28:
                c28 += 1
                if rr:
                    cancel_reasons_28[rr] += 1

        elif bucket == "ret":
            if in_14:
                r14 += 1
            if in_28:
                r28 += 1
                if rr:
                    ret_reasons_28[rr] += 1

    def reasons_to_str(cntr: Counter) -> str:
        if not cntr:
            return ""
        pairs = sorted(cntr.items(), key=lambda kv: (-kv[1], kv[0].lower()))
        # переносы строк внутри одной ячейки
        return "\n".join(f"{name} ({cnt})" for name, cnt in pairs)

    e_str = reasons_to_str(cancel_reasons_28)  # → HA
    h_str = reasons_to_str(ret_reasons_28)     # → HD

    return c14, c28, e_str, r14, r28, h_str


# ================== GOOGLE SHEETS ==================
def gs_client() -> gspread.Client:
    """
    credentials.json может быть:
      - service account (type == 'service_account');
      - OAuth client (installed/web) — запускаем gspread.oauth и сохраняем token.json.
    """
    cred_path = Path(CREDENTIALS_FILE)
    if not cred_path.exists():
        logger.error(f"Не найден файл авторизации: {cred_path.resolve()}")
        sys.exit(1)

    try:
        raw = json.loads(cred_path.read_text(encoding="utf-8"))
    except Exception as e:
        logger.error(f"Не удалось прочитать {CREDENTIALS_FILE}: {e}")
        sys.exit(1)

    # Service Account
    if isinstance(raw, dict) and raw.get("type") == "service_account":
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = Credentials.from_service_account_info(raw, scopes=scopes)
        return gspread.authorize(creds)

    # OAuth client
    try:
        return gspread.oauth(
            credentials_filename=CREDENTIALS_FILE,
            authorized_user_filename="token.json",
            scopes=["https://www.googleapis.com/auth/spreadsheets"],
        )
    except Exception as e:
        logger.error(f"OAuth аутентификация не удалась: {e}")
        logger.error("Поделитесь таблицей с приложением или используйте service account.")
        sys.exit(1)


def read_offer_ids_from_sheet(gc: gspread.Client, spreadsheet_id: str, sheet_name: str) -> List[str]:
    """
    Читает ВСЕ непустые offer_id из B5:B (игнорируя пустые строки внутри диапазона).
    """
    try:
        sh = gc.open_by_key(spreadsheet_id)
    except APIError as e:
        logger.error(f"Не удалось открыть таблицу: {e}")
        sys.exit(1)
    try:
        ws = sh.worksheet(sheet_name)
    except WorksheetNotFound:
        logger.error(f"Лист '{sheet_name}' не найден в таблице.")
        sys.exit(1)

    col_b = ws.col_values(2)  # столбец B
    col_b_tail = col_b[4:] if len(col_b) > 4 else []  # с 5-й строки
    offer_ids = [v.strip() for v in col_b_tail if v and v.strip()]

    if not offer_ids:
        logger.error("В столбце B5:B не найдено ни одного offer_id.")
        sys.exit(1)

    return offer_ids


def write_metrics_next_to_offers(
    gc: gspread.Client,
    spreadsheet_id: str,
    sheet_name: str,
    metrics_rows: List[Tuple[int, int, str, int, int, str]],
) -> None:
    """
    Записывает метрики в колонки GY…HD напротив каждого offer_id, начиная с 5-й строки.
    Соответствие: (C→GY, D→GZ, E→HA, F→HB, G→HC, H→HD)
    """
    try:
        sh = gc.open_by_key(spreadsheet_id)
        ws = sh.worksheet(sheet_name)
    except Exception as e:
        logger.error(f"Ошибка доступа к листу для записи метрик: {e}")
        return

    start_row = 5
    end_row = start_row + len(metrics_rows) - 1
    if end_row < start_row:
        return

    rng = f"GY{start_row}:HD{end_row}"
    vals = [[c14, c28, e_str, r14, r28, h_str] for (c14, c28, e_str, r14, r28, h_str) in metrics_rows]
    try:
        ws.update(vals, range_name=rng, value_input_option="USER_ENTERED")
    except APIError as e:
        logger.error(f"Не удалось записать метрики в лист '{sheet_name}': {e}")


def write_table_to_output_sheet(
    gc: gspread.Client,
    spreadsheet_id: str,
    rows: List[Dict[str, Any]],
    header_order: List[str],
    out_sheet: str,
) -> None:
    """
    Пишет подробную таблицу в отдельный лист. Не вызывается, если WRITE_OUT_SHEET=False.
    """
    try:
        sh = gc.open_by_key(spreadsheet_id)
        try:
            ws = sh.worksheet(out_sheet)
        except WorksheetNotFound:
            ws = sh.add_worksheet(
                title=out_sheet,
                rows=max(100, len(rows) + 10),
                cols=max(26, len(header_order)),
            )
        ws.clear()

        data = [header_order] + [
            [str(r.get(k, "")) if r.get(k) is not None else "" for k in header_order]
            for r in rows
        ]

        batch_size = 5000
        for i in range(0, len(data), batch_size):
            part = data[i: i + batch_size]
            start_row = 1 + i
            start_cell = f"A{start_row}"
            ws.update(part, range_name=start_cell, value_input_option="USER_ENTERED")
    except APIError as e:
        logger.error(f"Ошибка записи таблицы в '{out_sheet}': {e}")


# ================== MAIN ==================
def main():
    # Окно последних WINDOW_DAYS (UTC)
    now_utc = datetime.now(timezone.utc).replace(microsecond=0)
    time_to_iso = now_utc.isoformat().replace("+00:00", "Z")
    time_from_iso = (now_utc - timedelta(days=WINDOW_DAYS)).isoformat().replace("+00:00", "Z")

    # Чтение настроек
    headers, spreadsheet_id, sheet_name = read_api_file(Path("API.txt"))

    # Google Sheets client
    gc = gs_client()

    # Читаем offer_id из B5:B (все непустые)
    offer_ids = read_offer_ids_from_sheet(gc, spreadsheet_id, sheet_name)

    # HTTP session
    session = make_session()

    # 1) Глобальная пагинация: выгружаем все возвраты за окно дат
    all_items = fetch_all_returns(session, headers, time_from_iso, time_to_iso)

    # 2) Группируем по offer_id
    items_by_offer: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for it in all_items:
        product = it.get("product") or {}
        offer = product.get("offer_id")
        if offer:
            items_by_offer[offer].append(it)

    # 3) (опционально) сохраняем JSON по каждому офферу
    if SAVE_PER_OFFER_JSON:
        out_dir = Path("by_offer")
        out_dir.mkdir(exist_ok=True)
        for offer, items in items_by_offer.items():
            safe_name = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in offer)[:120]
            out_path = out_dir / f"{safe_name}.json"
            out_path.write_text(json.dumps(items, ensure_ascii=False, indent=2), encoding="utf-8")

    # 4) Считаем метрики по каждому офферу, который реально встречается в данных
    metrics_by_offer: Dict[str, Tuple[int, int, str, int, int, str]] = {}
    for offer, items in items_by_offer.items():
        metrics = compute_offer_metrics(items, now_utc)
        metrics_by_offer[offer] = metrics

    # 5) Формируем метрики СТРОГО в порядке offer_ids из таблицы
    per_offer_metrics: List[Tuple[int, int, str, int, int, str]] = [
        metrics_by_offer.get(o, (0, 0, "", 0, 0, "")) for o in offer_ids
    ]

    # 6) Локальные файлы (общий JSON/CSV) — отключены по умолчанию
    if SAVE_LOCAL_AGGREGATES:
        json_path = Path("returns_last30.json")
        json_path.write_text(json.dumps(all_items, ensure_ascii=False, indent=2), encoding="utf-8")

    rows = [flatten_for_csv(x) for x in all_items]
    if SAVE_LOCAL_AGGREGATES and rows:
        csv_path = Path("returns_last30.csv")
        with csv_path.open("w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)

    # 7) Запись метрик (по офферам) в GY…HD
    write_metrics_next_to_offers(gc, spreadsheet_id, sheet_name, per_offer_metrics)

    # 8) Подробная таблица на отдельный лист (если включено)
    if WRITE_OUT_SHEET and rows:
        header_order = list(rows[0].keys())
        write_table_to_output_sheet(gc, spreadsheet_id, rows, header_order, OUT_SHEET)


if __name__ == "__main__":
    start_ts = time.time()
    exit_code = 0
    try:
        main()
    except Exception:
        # все детали ошибки — только в script.log
        logger.exception("Необработанная ошибка в main()")
        exit_code = 1
    finally:
        elapsed = time.time() - start_ts
        secs = int(round(elapsed))
        print(f"Время выполнения: {secs} сек")
        sys.exit(exit_code)
