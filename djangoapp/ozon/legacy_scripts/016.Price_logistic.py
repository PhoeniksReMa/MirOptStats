import json
import math
import time
from typing import Dict, List, Tuple, Any, Iterable

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import gspread
from oauth2client.service_account import ServiceAccountCredentials


# =========================
# Конфиг и утилиты
# =========================

OZON_PRICE_URL = "https://api-seller.ozon.ru/v5/product/info/prices"
OZON_CHUNK_LIMIT = 1000  # лимит API по ids в одном запросе

SHEET_START_ROW = 5      # HP5 — первая строка для записи
SHEET_START_COL = "HP"
SHEET_END_COL = "IM"     # последний столбец записи на листе
EXPECTED_COLS = 24       # HP..IM = 24 столбца

API_CREDENTIALS_FILE = "API.txt"     # 4 строки: client_id, api_key, spreadsheet_id, worksheet_name
GDRIVE_CREDENTIALS_FILE = "credentials.json"

COLOR_INDEX_MAP = {
    "WITHOUT_INDEX": "НЕТ",
    "GREEN": "ХОРОШИЙ",
    "YELLOW": "СРЕДНИЙ",
    "RED": "ПЛОХОЙ",
}

EXCLUDED_ACTIONS: set = {
    "Рассрочка 0-0-6 на всё РФ товары",
    "WOW-БЭК_Кэшбэк на покупку Ozon Fashion списание 2.0",
    "ВАУ баллы 50% 9 волна 3-я волна (основная)",
    "Ozon Fashion + Jardin 500 вау баллов  списание",
    "Ozon Fashion + Jardin 1000 списание",
    "[Ozon Fashion + Jardin 10 000 списание",
    "Ozon Fashion + Jardin списание 1 млн",
    "Ozon Fashion + Jardin списание 1 млн вторая",
    "Ozon Fashion + Jardin_Запасная акция 500 вау баллов списание",
    "Ozon Fashion + Jardin_Запасная акция 1000 вау баллов списание",
    "Ozon Fashion + Jardin_Запасная акция 10 000 вау баллов списание",
    "Ozon x Ростикс / Вкусная игра Номинал ВАУ-баллов - 200, количество - 25 000 шт. списание",
    "Ozon x Ростикс / Вкусная игра Номинал ВАУ-баллов - 1000, количество - 15 500 шт. списание",
    "Ozon x Ростикс / Вкусная игра Номинал ВАУ-баллов - 100 000, количество - 10 шт. списание",
    "Ozon x Ростикс / Вкусная игра Номинал ВАУ-баллов - 1 000 000, количество - 1 шт. списание",
    "Ozon Fashion + Jardin 10 000 списание",
    "Промокоды для интеграции в НГ акцию от t2 // ВАУ-баллы списание 200",
    "Промокоды для интеграции в НГ акцию от t2 // ВАУ-баллы списание 500",
    "Промокоды для интеграции в НГ акцию от t2 // ВАУ-баллы списание 500",
    "Промокоды для интеграции в НГ акцию от t2 // ВАУ-баллы списание 200",
    "Промокоды для интеграции в НГ акцию от t2 // ВАУ-баллы списание 500",
    "РК. Честная рассрочка 0-0-6",
    "РК. Честная рассрочка 0-0-12",
    "Товары со скидкой на платном хранении",
    "Рассрочка Беларусь для теста на 5% клиентов. Хайлайт Людвига",
    "РК.Рассрочка 0-0-12 до 31.01.2026",
    "Скидка 10% для сотрудников НГ товары + Книги",
    "РК.Рассрочка 0-0-6 до 31.01.3031",
    "Дополнительные промокоды для интеграции в НГ акцию от t2 // ВАУ-баллы 200 списание",
    "Дополнительные промокоды для интеграции в НГ акцию от t2 // ВАУ-баллы 500 списание",
    "Дополнительные промокоды для интеграции в НГ акцию от t2 // ВАУ-баллы 500 списание",
    "Промокоды для интеграции в акцию \"Обмен минут и ГБ\" от t2 // ВАУ-баллы (бюджет коммерции) списание 100 баллов",
    "Промокоды для интеграции в акцию \"Обмен минут и ГБ\" от t2 // ВАУ-баллы (бюджет коммерции)  200 ВАУ-баллов х 100 000 шт списание",
    "Промокоды для интеграции в акцию \"Обмен минут и ГБ\" от t2 // ВАУ-баллы (бюджет коммерции)  500 ВАУ-баллов х 50 000 шт",
}


def safe_float(v: Any) -> float:
    try:
        return float(v) if v not in (None, "") else 0.0
    except (ValueError, TypeError):
        return 0.0


def ceil_num(v: Any) -> int:
    return int(math.ceil(safe_float(v)))


def chunked(iterable: List[int], size: int) -> Iterable[List[int]]:
    for i in range(0, len(iterable), size):
        yield iterable[i: i + size]


# =========================
# Авторизация и клиенты
# =========================

def read_api_credentials(file_path: str) -> Tuple[str, str, str, str]:
    """Файл содержит 4 строки: client_id, api_key, spreadsheet_id, worksheet_name"""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            lines = [line.strip() for line in f if line.strip()]
        if len(lines) < 4:
            raise ValueError("API file must contain 4 lines")
        return lines[0], lines[1], lines[2], lines[3]
    except Exception as e:
        raise ValueError(f"Error reading API file: {e}") from e


def get_gs_client(credentials_file: str) -> gspread.Client:
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.file",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_file, scope)
    return gspread.authorize(creds)


def make_session_with_retries(total: int = 5, backoff: float = 0.6) -> requests.Session:
    """HTTP-сессия с ретраями на 429/5xx и обрывах соединения."""
    retry = Retry(
        total=total,
        read=total,
        connect=total,
        status=total,
        backoff_factor=backoff,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["POST", "GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=50)
    s = requests.Session()
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


# =========================
# Работа с Google Sheets
# =========================

def open_worksheet(gs_client: gspread.Client, spreadsheet_id: str, worksheet_name: str) -> gspread.Worksheet:
    sh = gs_client.open_by_key(spreadsheet_id)
    return sh.worksheet(worksheet_name)


def read_inputs(worksheet: gspread.Worksheet) -> Tuple[List[int], float]:
    """Считываем product_ids (колонка L, начиная с L5) и HP2 за 1 проход."""
    ranges = worksheet.batch_get(["L5:L", "HP2:HP2"])
    ids_range = ranges[0] if ranges else []
    dq1_range = ranges[1] if len(ranges) > 1 else []

    product_ids: List[int] = []
    for row in ids_range:
        if not row:
            continue
        cell = row[0].strip()
        if cell.isdigit():
            product_ids.append(int(cell))

    dq1_value = safe_float(dq1_range[0][0]) if dq1_range and dq1_range[0] else 0.0
    return product_ids, dq1_value


def clear_output_range(worksheet: gspread.Worksheet, rows_count: int) -> None:
    if rows_count <= 0:
        return
    end_row = SHEET_START_ROW + rows_count - 1
    rng = f"{SHEET_START_COL}{SHEET_START_ROW}:{SHEET_END_COL}{end_row}"
    worksheet.batch_clear([rng])


def write_rows(worksheet: gspread.Worksheet, rows: List[List[Any]]) -> None:
    if not rows:
        return
    end_row = SHEET_START_ROW + len(rows) - 1
    rng = f"{SHEET_START_COL}{SHEET_START_ROW}:{SHEET_END_COL}{end_row}"
    worksheet.update(range_name=rng, values=rows, value_input_option="USER_ENTERED")


def write_ab_joined(worksheet: gspread.Worksheet, start_row: int, rows_count: int) -> None:
    """В AB заполняем формулы вида =HS{r}&" - "&HX{r} для каждой строки."""
    if rows_count <= 0:
        return
    end_row = start_row + rows_count - 1
    ab_range = f"AB{start_row}:AB{end_row}"
    formulas = [[f'=HS{r}&" - "&HX{r}'] for r in range(start_row, end_row + 1)]
    worksheet.update(range_name=ab_range, values=formulas, value_input_option="USER_ENTERED")


# =========================
# Ozon API
# =========================

def get_ozon_prices(session: requests.Session, client_id: str, api_key: str, product_ids: List[int]) -> Dict[int, Dict[str, Any]]:
    headers = {
        "Client-Id": str(client_id),
        "Api-Key": str(api_key),
        "Content-Type": "application/json",
    }

    all_prices: Dict[int, Dict[str, Any]] = {}

    for ids_chunk in chunked(product_ids, OZON_CHUNK_LIMIT):
        payload = {"filter": {"product_id": ids_chunk}, "limit": OZON_CHUNK_LIMIT}

        try:
            resp = session.post(OZON_PRICE_URL, headers=headers, json=payload, timeout=30)
            if resp.status_code == 429:
                time.sleep(1.0)
            resp.raise_for_status()
            result = resp.json() if resp.content else {}

            items = result.get("items") if isinstance(result, dict) else None
            if not items:
                print(f"[WARN] Empty/Unexpected response for chunk of size {len(ids_chunk)}")
                continue

            for item in items:
                if not item or "product_id" not in item:
                    continue
                pid = item["product_id"]
                all_prices[pid] = item

        except requests.RequestException as e:
            print(f"[ERROR] Chunk failed ({len(ids_chunk)} ids): {e}")

    return all_prices


# =========================
# Подготовка данных для листа
# =========================

def prepare_rows(data_by_id: Dict[int, Dict[str, Any]], product_ids: List[int], dq1_value: float) -> List[List[Any]]:
    rows: List[List[Any]] = []

    for pid in product_ids:
        item = data_by_id.get(pid)
        if not item:
            rows.append([""] * EXPECTED_COLS)
            continue

        price_data = item.get("price") or {}
        commissions = item.get("commissions") or {}
        price_indexes = item.get("price_indexes") or {}
        marketing_actions = (item.get("marketing_actions") or {}).get("actions") or []

        auto_action = "🔥" if price_data.get("auto_action_enabled", False) else "🔕"
        old_price = safe_float(price_data.get("old_price"))
        min_price = safe_float(price_data.get("min_price"))
        price = safe_float(price_data.get("price"))
        marketing_seller_price = safe_float(price_data.get("marketing_seller_price"))
        marketing_price = safe_float(price_data.get("marketing_price"))
        net_price = safe_float(price_data.get("net_price"))

        color_index = COLOR_INDEX_MAP.get(price_indexes.get("color_index", "WITHOUT_INDEX"), "НЕТ")

        acquiring = ceil_num(item.get("acquiring"))
        sales_percent_fbo = safe_float(commissions.get("sales_percent_fbo"))
        sales_percent_fbs = safe_float(commissions.get("sales_percent_fbs"))

        fbo_transport = ceil_num(commissions.get("fbo_direct_flow_trans_max_amount"))
        fbs_transport = ceil_num(commissions.get("fbs_direct_flow_trans_max_amount"))
        fbo_delivery = ceil_num(commissions.get("fbo_deliv_to_customer_amount"))
        fbs_delivery = ceil_num(commissions.get("fbs_deliv_to_customer_amount"))

        dr_value = math.ceil((marketing_seller_price * sales_percent_fbo) / 100) if marketing_seller_price and sales_percent_fbo else 0
        ds_value = math.ceil((marketing_seller_price * sales_percent_fbs) / 100) if marketing_seller_price and sales_percent_fbs else 0
        dt_value = math.ceil(acquiring + dr_value + fbo_transport + fbo_delivery)
        du_value = math.ceil(acquiring + fbs_transport + fbs_delivery + ds_value + dq1_value)

        action_titles: List[str] = []
        actions_count = 0
        for action in marketing_actions:
            if not isinstance(action, dict):
                continue
            title = (action.get("title") or "").strip()
            if title and title not in EXCLUDED_ACTIONS:
                action_titles.append(f"[{title}]")
                actions_count += 1
        action_title = " ".join(action_titles)

        row = [
            acquiring,                      # HP
            sales_percent_fbo,              # HQ
            dr_value,                       # HR
            fbo_transport,                  # HS
            fbo_delivery,                   # HT
            safe_float(commissions.get("fbo_return_flow_amount")),  # HU
            sales_percent_fbs,              # HV
            ds_value,                       # HW
            fbs_transport,                  # HX
            fbs_delivery,                   # HY
            safe_float(commissions.get("fbs_return_flow_amount")),  # HZ
            dt_value,                       # IA
            du_value,                       # IB
            "",                             # IC (резерв)
            auto_action,                    # ID
            old_price,                      # IE
            min_price,                      # IF
            price,                          # IG
            marketing_seller_price,         # IH
            marketing_price,                # II
            color_index,                    # IJ
            action_title,                   # IK
            actions_count,                  # IL
            net_price                       # IM
        ]

        # Нормализуем ширину
        if len(row) < EXPECTED_COLS:
            row += [""] * (EXPECTED_COLS - len(row))
        elif len(row) > EXPECTED_COLS:
            row = row[:EXPECTED_COLS]

        rows.append(row)

    return rows


# =========================
# Основной сценарий
# =========================

def main():
    start_time = time.perf_counter()
    try:
        print("Starting script...")
        client_id, api_key, spreadsheet_id, worksheet_name = read_api_credentials(API_CREDENTIALS_FILE)

        gs_client = get_gs_client(GDRIVE_CREDENTIALS_FILE)
        ws = open_worksheet(gs_client, spreadsheet_id, worksheet_name)

        product_ids, dq1_value = read_inputs(ws)
        if not product_ids:
            raise RuntimeError("No product_ids to process (column L from row 5 is empty).")

        session = make_session_with_retries()

        data = get_ozon_prices(session, client_id, api_key, product_ids)
        if not data:
            raise RuntimeError("No data received from Ozon API.")

        rows = prepare_rows(data, product_ids, dq1_value)

        clear_output_range(ws, len(rows))
        write_rows(ws, rows)

        # Заполняем столбец AB формулами "HS - HX"
        write_ab_joined(ws, SHEET_START_ROW, len(rows))

        print(f"Successfully updated {len(rows)} rows")

        elapsed = time.perf_counter() - start_time
        print(f"Script completed successfully in {elapsed:.2f} seconds")

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()