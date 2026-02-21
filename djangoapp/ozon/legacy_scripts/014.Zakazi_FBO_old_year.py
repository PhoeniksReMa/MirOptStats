import logging
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Tuple

import gspread
import pytz
import requests
from dateutil.relativedelta import relativedelta
from oauth2client.service_account import ServiceAccountCredentials
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ────────────────────────── ЛОГИ ──────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)

MSK = pytz.timezone("Europe/Moscow")


# ───────────────────── ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ─────────────────────
def read_api_credentials(file_path: str) -> Tuple[str, str, str, str]:
    """Читает client_id, api_key, spreadsheet_id, worksheet_name из 4 строк файла."""
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = [line.strip() for line in f if line.strip()]
    if len(lines) < 4:
        raise ValueError("Файл должен содержать 4 строки: client_id, API ключ, ID таблицы и название листа")
    return lines[0], lines[1], lines[2], lines[3]


def to_rfc3339_utc(dt: datetime) -> str:
    """Конвертация aware-даты в UTC с суффиксом Z."""
    if dt.tzinfo is None:
        raise ValueError("Ожидается timezone-aware datetime")
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def last_year_month_range(month_offset: int) -> Tuple[datetime, datetime]:
    """
    Возвращает (start, end) для месяца прошлого года со смещением.
    Пример: сегодня = сентябрь 2025
      offset=1 -> октябрь 2024, offset=2 -> ноябрь 2024, offset=3 -> декабрь 2024
    """
    today = datetime.now(MSK)
    base = today.replace(day=1) + relativedelta(months=month_offset)  # следующий(ие) месяцы в этом году
    target = base.replace(year=today.year - 1)                        # тот же месяц, но прошлый год
    start = target
    end = (target + relativedelta(months=1)) - timedelta(seconds=1)
    return start, end


# ──────────────────────────── OZON API ────────────────────────────
class OzonAPI:
    BASE_URL = "https://api-seller.ozon.ru"

    def __init__(self, client_id: str, api_key: str, timeout: int = 30) -> None:
        self.client_id = client_id
        self.api_key = api_key
        self.timeout = timeout

        # Сессия + ретраи на 429/5xx
        self.session = requests.Session()
        retry = Retry(
            total=5,
            backoff_factor=0.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("POST",),
            raise_on_status=False
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

        self.headers = {
            "Client-Id": self.client_id,
            "Api-Key": self.api_key,
            "Content-Type": "application/json",
        }

    def get_fbo_posting_list(self, payload: dict) -> dict:
        url = f"{self.BASE_URL}/v2/posting/fbo/list"
        resp = self.session.post(url, headers=self.headers, json=payload, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    def iter_delivered_postings(
        self,
        since_utc: str,
        to_utc: str,
        limit: int = 1000
    ) -> Iterable[dict]:
        """
        Эффективный пагинатор по доставленным FBO-постингам.
        Сужаем фильтр полем status='delivered' и не тянем лишние блоки.
        """
        offset = 0
        while True:
            payload = {
                "dir": "DESC",
                "filter": {
                    "since": since_utc,
                    "to": to_utc,
                    "status": "delivered",
                },
                "limit": limit,
                "offset": offset,
                "translit": False,
                "with": {
                    "analytics_data": False,
                    "financial_data": False
                }
            }
            data = self.get_fbo_posting_list(payload)
            result = data.get("result", [])
            # API иногда возвращает либо список, либо объект с "postings"
            postings = result if isinstance(result, list) else result.get("postings", [])
            if not postings:
                break
            yield from postings
            if len(postings) < limit:
                break
            offset += limit


def get_delivered_count_by_sku(api: OzonAPI, start_dt: datetime, end_dt: datetime) -> Dict[str, int]:
    """Считает количество доставленных единиц по offer_id (SKU) за период. Логирует время загрузки."""
    logging.info(
        "Загрузка постингов %s – %s",
        start_dt.strftime("%d.%m.%Y"),
        end_dt.strftime("%d.%m.%Y"),
    )
    t0 = time.perf_counter()

    counts: Dict[str, int] = defaultdict(int)
    since_utc = to_rfc3339_utc(start_dt)
    to_utc = to_rfc3339_utc(end_dt)

    total_postings = 0
    total_products = 0

    for posting in api.iter_delivered_postings(since_utc, to_utc):
        total_postings += 1
        for product in posting.get("products", []):
            total_products += 1
            counts[product.get("offer_id", "")] += int(product.get("quantity", 0))

    t1 = time.perf_counter()
    logging.info(
        "Получено постингов: %d (товаров в постингах: %d) за %.2f сек",
        total_postings, total_products, t1 - t0
    )
    return counts


# ───────────────────────── GOOGLE SHEETS ─────────────────────────
def authorize_sheets(creds_path: str):
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
    client = gspread.authorize(creds)
    return client


def write_columns_batch(
    sheet,
    skus: List[str],
    col_letter: str,
    counts: Dict[str, int],
    month_name: str,
    date_range_str: str,
) -> List[dict]:
    """
    Готовит блоки данных для одного столбца:
      - Заголовок месяца (2 ячейки в ряд)
      - Строка диапазона дат
      - Столбец значений по SKU (одним диапазоном)
    Возвращает элементы для gspread.batch_update().
    """
    updates = []

    # HH2:HI2 / HJ2:HK2 / HL2:HM2 — продублированное имя месяца в две ячейки
    header_ranges = {
        "HH": "HH2:HI2",
        "HJ": "HJ2:HK2",
        "HL": "HL2:HM2",
    }
    if col_letter in header_ranges:
        updates.append({
            "range": header_ranges[col_letter],
            "values": [[month_name, month_name]]
        })

    # HH3 / HJ3 / HL3 — строка диапазона дат
    updates.append({
        "range": f"{col_letter}3",
        "values": [[date_range_str]]
    })

    # Значения по SKU в один проход: HH5:HH{N} / HJ5:HJ{N} / HL5:HL{N}
    start_row = 5
    end_row = start_row + len(skus) - 1
    if end_row >= start_row:
        values = [[int(counts.get(sku, 0))] for sku in skus]
        updates.append({
            "range": f"{col_letter}{start_row}:{col_letter}{end_row}",
            "values": values
        })

    return updates


# ───────────────────────────── MAIN ─────────────────────────────
def main():
    try:
        total_start = time.perf_counter()  # старт общего таймера

        logging.info("Чтение конфигурации и подключение к Google Sheets...")
        client_id, api_key, sheet_id, sheet_name = read_api_credentials("API.txt")
        api = OzonAPI(client_id, api_key)

        gclient = authorize_sheets("credentials.json")
        sheet = gclient.open_by_key(sheet_id).worksheet(sheet_name)

        # SKU (offer_id) находятся в столбце B начиная с 5 строки
        skus = sheet.col_values(2)[4:]   # B5:B...
        logging.info("Всего SKU к обработке: %d", len(skus))

        # Очищаем старые данные в нужных столбцах
        logging.info("Очистка старых данных в столбцах HH, HJ, HL...")
        sheet.batch_clear([
            "HH5:HH", "HJ5:HJ", "HL5:HL",
            "HH2:HI2", "HJ2:HK2", "HL2:HM2",
            "HH3:HH3", "HJ3:HJ3", "HL3:HL3"
        ])

        # Периоды для столбцов (соответствие колонке)
        periods = {
            "HH": last_year_month_range(1),
            "HJ": last_year_month_range(2),
            "HL": last_year_month_range(3),
        }

        month_names = {
            1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель",
            5: "Май", 6: "Июнь", 7: "Июль", 8: "Август",
            9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь"
        }

        # Считаем и готовим обновления по каждому столбцу
        batch_updates: List[dict] = []
        for col, (start_dt, end_dt) in periods.items():
            step_start = time.perf_counter()
            logging.info("Обработка столбца %s: %s %d", col, month_names[start_dt.month], start_dt.year)

            counts = get_delivered_count_by_sku(api, start_dt, end_dt)

            date_range_str = f"{start_dt.strftime('%d.%m.%y')} {end_dt.strftime('%d.%m.%y')}"
            month_name = month_names[start_dt.month]

            batch_updates.extend(
                write_columns_batch(
                    sheet=sheet,
                    skus=skus,
                    col_letter=col,
                    counts=counts,
                    month_name=month_name,
                    date_range_str=date_range_str,
                )
            )
            step_end = time.perf_counter()
            per_sku = (step_end - step_start) / max(len(skus), 1)
            logging.info("Столбец %s обработан за %.2f сек (≈ %.4f сек/sku)", col, step_end - step_start, per_sku)

        logging.info("Запись данных в Google Sheets одним батчем...")
        write_start = time.perf_counter()
        sheet.batch_update(batch_updates, value_input_option="USER_ENTERED")
        write_end = time.perf_counter()
        logging.info("Запись в Google Sheets заняла %.2f сек", write_end - write_start)

        total_end = time.perf_counter()
        logging.info("Готово! Общее время выполнения: %.2f сек", total_end - total_start)

    except requests.HTTPError as e:
        logging.error("HTTP ошибка OZON API: %s", e, exc_info=True)
    except Exception as e:
        logging.error("Необработанная ошибка: %s", e, exc_info=True)


if __name__ == "__main__":
    main()
