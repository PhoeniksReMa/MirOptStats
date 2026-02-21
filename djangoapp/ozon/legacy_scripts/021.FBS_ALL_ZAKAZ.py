# ====== Смена CWD на iPhone ДО любых импортов ======
import os, sys, tempfile
if sys.platform == "ios":  # Pyto (iOS)
    SAFE_DIR = os.path.expanduser("~/Documents")
    try:
        os.makedirs(SAFE_DIR, exist_ok=True)
    except Exception:
        pass
    try:
        os.chdir(SAFE_DIR)
    except Exception:
        os.chdir(tempfile.gettempdir())
else:
    SAFE_DIR = os.path.expanduser("~/Documents")  # для совместимости на ПК

# ====== Основные импорты ======
import re
import time
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional
from zoneinfo import ZoneInfo

import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Папка со скриптом (на ПК пригодится)
try:
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
except NameError:
    SCRIPT_DIR = os.getcwd()

# ====== iCloud-aware выбор папки вывода на iOS (оставляем для поиска файлов) ======
def _looks_like_icloud_path(path: str) -> bool:
    low = path or ""
    return ("Mobile Documents" in low) or ("iCloud~" in low)

def _pyto_icloud_docs() -> str:
    return os.path.expanduser("~/Library/Mobile Documents/iCloud~org.python.pyto/Documents")

if sys.platform == "ios":
    if _looks_like_icloud_path(SCRIPT_DIR):
        OUTPUT_DIR = SCRIPT_DIR
    else:
        icloud_docs = _pyto_icloud_docs()
        OUTPUT_DIR = icloud_docs if os.path.isdir(icloud_docs) else SAFE_DIR
else:
    OUTPUT_DIR = SCRIPT_DIR

try:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
except Exception:
    pass

# ====== Настройки ======
DAYS_BACK = 5  # календарно от 00:00 по МСК (см. iter_ozon_postings_last_ndays)

# Лист — жестко зашит
SHEET_NAME = "🟦Заказы_FBS🚛"

START_ROW = 5
HEADER_ROW = 4
LIMIT = 1000
TIMEOUT = 30
MAX_RETRIES = 5
BACKOFF_BASE = 0.5

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("ozon-sync")

# ====== Читаем Client ID / Api-key / Spreadsheet ID из API.txt ======
def _load_api_txt() -> tuple[str, str, str]:
    """
    API.txt без заголовков, 3 строки:
    1) Client ID
    2) Api-key
    3) ID таблицы (SPREADSHEET_ID)
    """
    candidates = [
        os.path.join(SCRIPT_DIR, "API.txt"),
        os.path.join(OUTPUT_DIR, "API.txt"),
        os.path.join(SAFE_DIR, "API.txt"),
    ]
    api_path = next((p for p in candidates if os.path.isfile(p)), None)
    if not api_path:
        raise FileNotFoundError(
            "Не найден API.txt.\n"
            "Положи API.txt рядом со скриптом (или в OUTPUT_DIR / Documents).\n\n"
            "Формат (3 строки):\n"
            "1) Client ID\n2) Api-key\n3) Spreadsheet ID"
        )

    with open(api_path, "r", encoding="utf-8") as f:
        lines = [line.strip() for line in f if line.strip()]

    if len(lines) < 3:
        raise ValueError(
            "В API.txt должно быть минимум 3 непустые строки:\n"
            "1) Client ID\n2) Api-key\n3) Spreadsheet ID"
        )

    return lines[0], lines[1], lines[2]

CLIENT_ID, API_KEY, SPREADSHEET_ID = _load_api_txt()

# ====== Google авторизация: ТОЛЬКО из файла credentials.json ======
def _find_credentials_json() -> str:
    candidates = [
        os.path.join(SCRIPT_DIR, "credentials.json"),
        os.path.join(OUTPUT_DIR, "credentials.json"),
        os.path.join(SAFE_DIR, "credentials.json"),
    ]
    path = next((p for p in candidates if os.path.isfile(p)), None)
    if not path:
        raise FileNotFoundError(
            "Не найден credentials.json.\n"
            "Положи credentials.json рядом со скриптом (или в OUTPUT_DIR / Documents)."
        )
    return path

def get_gspread_client():
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    cred_path = _find_credentials_json()
    creds = ServiceAccountCredentials.from_json_keyfile_name(cred_path, scope)
    return gspread.authorize(creds)

# ====== Данные/константы ======
HEADER_TRANSLATIONS = {
    'Posting Number': 'Номер отправления',
    'Status': 'Статус',
    'Offer ID': 'Артикул',
    'Product Quantity': 'Количество',
    'In Process At': 'Дата создания',
    'Shipment Date': 'Дата отгрузки',
    'Product Name': 'Наименование',
    'Cluster To': 'Кластер отправки',
    'Fin Price': 'Цена',
    'Fin Actions': 'Акции',
}
FULL_HEADERS_EXTENDED = (
    'Posting Number','Status','Offer ID','Product Quantity',
    'In Process At','Shipment Date','Product Name','Cluster To',
    'Fin Price','Fin Actions',
)

# ВАЖНО: для FBS LIST валиден awaiting_deliver, а awaiting_delivery — НЕТ.
STATUS_MAP = {
    'awaiting_packaging': 'Ожидает упаковки',
    'awaiting_deliver': 'Ожидает отгрузки',
    'cancelled': 'Отменен',
}

MONTHS_RU = {1:"Янв",2:"Фев",3:"Мар",4:"Апр",5:"Май",6:"Июн",7:"Июл",8:"Авг",9:"Сен",10:"Окт",11:"Ноя",12:"Дек"}
TZ_MSK = ZoneInfo("Europe/Moscow")

def clean_value(v):
    if v is None:
        return ''
    if isinstance(v, (dict, list)):
        return json.dumps(v, ensure_ascii=False)
    if isinstance(v, str):
        return STATUS_MAP.get(v, v)
    return str(v)

def iso_to_moscow_str(dt_str: Optional[str]) -> str:
    if not dt_str:
        return ''
    try:
        if isinstance(dt_str, str) and dt_str.endswith('Z'):
            dt = datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
        else:
            dt = datetime.fromisoformat(dt_str)
        dt = dt.astimezone(TZ_MSK)
        return f"{dt.day:02d} {MONTHS_RU[dt.month]} {dt:%H:%M}"
    except Exception:
        return clean_value(dt_str)

def make_final_headers() -> List[str]:
    return [HEADER_TRANSLATIONS.get(h, h) for h in FULL_HEADERS_EXTENDED]

# ====== Ozon API ======
class OzonClient:
    BASE_URL = "https://api-seller.ozon.ru"

    def __init__(self, client_id: str, api_key: str, timeout: int = TIMEOUT):
        self.s = requests.Session()
        self.s.headers.update({
            "Client-Id": client_id,
            "Api-Key": api_key,
            "Content-Type": "application/json"
        })
        self.timeout = timeout

    def post(self, path: str, payload: dict) -> dict:
        url = f"{self.BASE_URL}{path}"
        last_exc = None

        for attempt in range(MAX_RETRIES):
            try:
                r = self.s.post(url, json=payload, timeout=self.timeout)

                # 4xx — нет смысла ретраить, покажем тело ответа
                if 400 <= r.status_code < 500 and r.status_code != 429:
                    raise requests.HTTPError(
                        f"{r.status_code} {r.reason}. Response: {r.text}"
                    )

                if r.status_code in (429, 500, 502, 503, 504):
                    raise requests.HTTPError(f"{r.status_code} {r.text}")

                r.raise_for_status()
                return r.json()

            except Exception as e:
                last_exc = e
                msg = str(e)

                # если это 4xx (кроме 429) — не ретраим
                if (" 400 " in msg) or (" 401 " in msg) or (" 403 " in msg) or (" 404 " in msg):
                    break

                time.sleep(min(30, (2 ** attempt) * BACKOFF_BASE) + 0.1 * attempt)

        raise RuntimeError(f"Ошибка запроса к API Ozon: {last_exc}")

def _fmt_ozon_utc(dt: datetime) -> str:
    # RFC3339 + миллисекунды + Z
    return dt.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

def iter_ozon_postings_last_ndays(
    ozon: OzonClient,
    days_back: int,
    status: str = "awaiting_packaging"
) -> Iterable[dict]:
    """
    "days_back" календарно от 00:00 по МСК, чтобы заказы на границе не выпадали.
    """
    now_msk = datetime.now(TZ_MSK)

    # с 00:00 МСК days_back дней назад
    since_msk = (now_msk - timedelta(days=days_back)).replace(hour=0, minute=0, second=0, microsecond=0)

    since_dt = since_msk.astimezone(timezone.utc)
    to_dt = now_msk.astimezone(timezone.utc)

    offset = 0
    while True:
        payload = {
            "dir": "ASC",
            "filter": {
                "since": _fmt_ozon_utc(since_dt),
                "to": _fmt_ozon_utc(to_dt),
                "status": status
            },
            "limit": LIMIT,
            "offset": offset,
            "translit": True,
            "with": {"analytics_data": True, "financial_data": True}
        }

        data = ozon.post("/v3/posting/fbs/list", payload)
        res = data.get("result") or {}
        posts = res.get("postings") or []
        for p in posts:
            yield p

        if not res.get("has_next"):
            break
        offset += LIMIT

# ✅ получаем несколько статусов и дедуплицируем по posting_number
def fetch_postings_multi_status(
    ozon: OzonClient,
    days_back: int,
    statuses: Iterable[str],
) -> List[dict]:
    by_number: Dict[str, dict] = {}
    for st in statuses:
        for p in iter_ozon_postings_last_ndays(ozon, days_back, status=st):
            num = str(p.get("posting_number") or "").strip()
            if num:
                by_number[num] = p
    return list(by_number.values())

def _parse_iso(dt_str: str):
    try:
        if isinstance(dt_str, str) and dt_str.endswith("Z"):
            return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        return datetime.fromisoformat(dt_str)
    except Exception:
        return datetime.min.replace(tzinfo=timezone.utc)

# ====== Построение строк ======
def build_posting_fields(posting: dict) -> Dict[str, str]:
    return {
        'Posting Number': clean_value(posting.get('posting_number')),
        'In Process At': iso_to_moscow_str(posting.get('in_process_at')),
        'Shipment Date': iso_to_moscow_str(posting.get('shipment_date')),
        'Status': clean_value(posting.get('status')),
    }

def build_product_fields(product: Optional[dict]) -> Dict[str, str]:
    if not product:
        return {'Product Name': '', 'Offer ID': '', 'Product Quantity': ''}
    return {
        'Product Name': clean_value(product.get('name')),
        'Offer ID': clean_value(product.get('offer_id')),
        'Product Quantity': clean_value(product.get('quantity')),
    }

# --- НОРМАЛИЗАЦИЯ ACTIONS ---
ACTIONS_EXCLUDE = {
    "Округление",
    "Товарная скидка на доставку (Сквозная экономика 5)",
    "Системная виртуальная скидка селлера Россия (RUB)",
    "Акция на списание индивидуальных бонусов для селлера 160517 от 11.10.23 13:18:34",
    "OA by AI benefit system (Mesh)", "Скидка за счет Ozon",
    "DD by AI benefit system (Mesh)", "Скидка за счет Ozon",
    "Скидка (за счет Озон) - DD by AI benefit system (Mesh)",
    "Скидка (за счет Озон) - OA by AI benefit system (Mesh)",
    "Скидка (за счет Озон) - DD by AI benefit system (CIS-Benefit1) v2",
    "[Оплата Баллами] Стандартные условия (до 25%)",
}

ACTIONS_MAP_CONTAINS = [
    ("Бустинг 25% (ранее — «Бустинг х4»)", "Бустинг 25%"),
    ("Бустинг 15% (ранее — «Бустинг х3»)", "Бустинг 15%"),
    ("Эластичный бустинг. Без ограничения срока действия", "Эластичный бустинг"),
    ("Дополнительные баллы за скидки от Озон и Максимальный бустинг",
     "Доп. баллы за скидки от Озон и Максимальный бустинг"),
]

_PAREN_RE = re.compile(r"\s*\(ранее\s+—\s+«[^»]+»\)\s*$", re.IGNORECASE)

def _normalize_single_action(a: str) -> Optional[str]:
    if a is None:
        return None
    t = str(a).strip()
    if not t:
        return None
    if t in ACTIONS_EXCLUDE:
        return None

    low = t.lower()
    for needle, repl in ACTIONS_MAP_CONTAINS:
        if needle.lower() in low:
            t = repl
            break

    t = _PAREN_RE.sub("", t).strip()
    return t or None

def _join_actions(a) -> str:
    if not a:
        return ''
    if isinstance(a, (list, tuple)):
        out, seen = [], set()
        for x in a:
            nx = _normalize_single_action(x)
            if not nx:
                continue
            if nx not in seen:
                seen.add(nx)
                out.append(nx)
        return ", ".join(out)
    nx = _normalize_single_action(a)
    return nx or ''

def _find_financial_for_product(financial_data: dict, product: Optional[dict]) -> dict:
    """
    Находим соответствующую запись financial_data.products[*] для товара:
    матч по SKU ↔ product_id, иначе если единственная запись — берём её.
    """
    if not financial_data or not isinstance(financial_data, dict):
        return {}
    fin_products = financial_data.get('products') or []
    if not fin_products:
        return {}

    sku = None
    if product and isinstance(product, dict):
        sku = product.get('sku') or product.get('SKU')

    match = None
    if sku is not None:
        for fp in fin_products:
            try:
                if int(fp.get('product_id', -1)) == int(sku):
                    match = fp
                    break
            except Exception:
                continue

    if match is None and len(fin_products) == 1:
        match = fin_products[0]

    return match or {}

def build_financial_fields_per_product(fin_prod: dict, financial_data: dict) -> Dict[str, str]:
    cluster_to = (financial_data or {}).get('cluster_to')
    return {
        'Cluster To': clean_value(cluster_to),
        'Fin Price': clean_value(fin_prod.get('price')),
        'Fin Actions': _join_actions(fin_prod.get('actions')),
    }

def build_full_row_dict(posting: dict, product: Optional[dict]) -> Dict[str, str]:
    d = {}
    d.update(build_posting_fields(posting))
    d.update(build_product_fields(product))

    fin = posting.get('financial_data') or {}
    fin_prod = _find_financial_for_product(fin, product)
    d.update(build_financial_fields_per_product(fin_prod, fin))

    for k in FULL_HEADERS_EXTENDED:
        d.setdefault(k, '')
    return d

def row_to_final_order(d: Dict[str, str]) -> List[str]:
    return [d.get(h, '') for h in FULL_HEADERS_EXTENDED]

def process_postings(postings: Iterable[dict]) -> List[List[str]]:
    rows = []
    for p in postings:
        prods = p.get('products') or []
        if not prods:
            rows.append(row_to_final_order(build_full_row_dict(p, None)))
        else:
            for pr in prods:
                if isinstance(pr, dict):
                    rows.append(row_to_final_order(build_full_row_dict(p, pr)))
    return rows

# ====== Google Sheets ======
def _col_letter(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

def get_sheet():
    client = get_gspread_client()
    spreadsheet = client.open_by_key(SPREADSHEET_ID)
    return spreadsheet.worksheet(SHEET_NAME)

def clear_header_cells(sheet):
    try:
        sheet.batch_clear(["A1:A3", "D3", "H3"])
        log.info("Ячейки A1:A3, D3 и H3 очищены")
    except Exception as e:
        log.error(f"Ошибка при очистке ячеек: {e}")

def update_google_sheet_batch(sheet, headers: List[str], rows: List[List[str]]):
    last_col_letter = _col_letter(len(FULL_HEADERS_EXTENDED))
    last_row = HEADER_ROW if not rows else START_ROW + len(rows) - 1

    try:
        sheet.batch_clear([f"A{HEADER_ROW}:{last_col_letter}{max(last_row, START_ROW + 5000)}"])
    except Exception as e:
        log.warning(f"Не удалось очистить диапазон: {e}")

    updates = [{"range": f"A{HEADER_ROW}:{last_col_letter}{HEADER_ROW}", "values": [headers]}]
    if rows:
        updates.append({"range": f"A{START_ROW}:{last_col_letter}{last_row}", "values": rows})
    sheet.batch_update(updates)

def update_first_last_shipment(sheet, rows: List[List[str]]):
    if not rows:
        log.info("Нет данных для D3/H3")
        return
    nums = [r[0] for r in rows if r and str(r[0]).strip()]
    if not nums:
        log.info("Нет номеров отправлений для D3/H3")
        return
    sheet.batch_update([
        {"range": "D3", "values": [[nums[0]]]},
        {"range": "H3", "values": [[nums[-1]]]},
    ])
    log.info(f"Обновлены D3={nums[0]}, H3={nums[-1]}")

def update_header_info(sheet, total_postings: int):
    now = datetime.now(TZ_MSK)
    a1 = f"Обновлено {now:%d.%m|%H:%M}"
    a2 = f"Отправлений {total_postings}"
    a3 = f"Этикеток создано 0"
    sheet.batch_update([
        {"range": "A1", "values": [[a1]]},
        {"range": "A2", "values": [[a2]]},
        {"range": "A3", "values": [[a3]]},
    ])
    log.info("A1/A2/A3 обновлены")

# ====== Основной сценарий ======
def main():
    ozon = None
    try:
        sheet = get_sheet()

        log.info("Очистка служебных ячеек...")
        clear_header_cells(sheet)

        log.info("Получение данных из Ozon...")
        ozon = OzonClient(CLIENT_ID, API_KEY)

        # ВАЖНО: awaiting_delivery УБРАЛИ — он невалиден для метода list
        statuses = ["awaiting_packaging", "awaiting_deliver"]
        postings = fetch_postings_multi_status(ozon, DAYS_BACK, statuses)

        postings.sort(key=lambda p: _parse_iso(p.get("in_process_at") or ""))

        total = len(postings)
        log.info(f"Получено отправлений: {total}")

        log.info("Формирование строк...")
        rows = process_postings(postings)
        log.info(f"Сформировано строк: {len(rows)}")

        log.info("Обновление Google-таблицы...")
        update_google_sheet_batch(sheet, make_final_headers(), rows)
        update_first_last_shipment(sheet, rows)

        log.info("Обновление заголовков...")
        update_header_info(sheet, total_postings=total)

        log.info("Готово!")
    except Exception as e:
        log.error(f"Ошибка: {e}")
    finally:
        try:
            if ozon and ozon.s:
                ozon.s.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()
