#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Ozon → Google Sheets (без кэша), батчами.
Берёт CLIENT_ID, API_KEY, spreadsheet_id, worksheet_name из API.txt (без заголовков),
читает product_id из L начиная с 5-й строки, пишет очищенное описание в JE
и формирует SEO-промт в JF, подставляя описание (JE) и ключевые слова (JD).
В 4-й строке ставит заголовки: JE4="Описание", JF4="SEO +".
Если ключевые слова (JD) отсутствуют — в JF ставится знак ❌.
"""

import os, re, sys, time, threading, logging
from logging.handlers import RotatingFileHandler  # можно оставить, но не используется
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple, Optional, Dict
import requests, gspread
from google.oauth2.service_account import Credentials
from gspread.cell import Cell

# =========================
#        НАСТРОЙКИ
# =========================
API_FILE            = "API.txt"
GOOGLE_CREDENTIALS  = "credentials.json"
OZON_URL            = "https://api-seller.ozon.ru/v1/product/info/description"

# Google Sheets
COL_IN_A1           = "L"     # откуда берём product_id
COL_OUT_A1          = "JE"    # куда пишем описание
COL_KEYWORDS_A1     = "JD"    # столбец с ключевыми словами
COL_PROMPT_A1       = "JF"    # куда пишем SEO-промт
HEADER_ROW          = 4       # строка заголовков
START_ROW           = 5       # с какой строки читать product_id

# Производительность
BATCH_SIZE          = 50
MAX_WORKERS         = 8
RATE_PER_SEC        = 5.0
TIMEOUT_SEC         = 30
RETRIES             = 4
BACKOFF_BASE        = 1.8

# Логи (НЕ СОЗДАЁМ ПАПКУ logs, логируем только в консоль)
LOG_LEVEL           = "INFO"

# Прочее
SHEETS_CELL_CHAR_LIMIT = 49000  # перестраховка под лимит ячейки


# =========================
#        УТИЛИТЫ
# =========================

def a1_to_index(col_a1: str) -> int:
    col_a1 = col_a1.strip().upper()
    num = 0
    for ch in col_a1:
        if not ('A' <= ch <= 'Z'):
            raise ValueError(f"Некорректное имя колонки: {col_a1}")
        num = num * 26 + (ord(ch) - ord('A') + 1)
    return num

def read_api_txt(path: str) -> Tuple[str, str, str, str]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Файл не найден: {path}")
    with open(path, "r", encoding="utf-8") as f:
        lines = [x.strip() for x in f if x.strip()]
    if len(lines) < 4:
        raise ValueError("API.txt должен содержать 4 строки.")
    return lines[0], lines[1], lines[2], lines[3]

def setup_logger(level: str = LOG_LEVEL):
    """
    Логируем только в stdout. НИКАКИХ директорий/файлов не создаём.
    """
    logger = logging.getLogger("ozon_to_sheets")
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)

    # Сбрасываем возможные старые хендлеры, чтобы не дублировать вывод
    if logger.handlers:
        for h in list(logger.handlers):
            logger.removeHandler(h)

    logger.addHandler(sh)
    logger.propagate = False
    return logger

def strip_html(html: str) -> str:
    html = re.sub(r"(?i)<\s*br\s*/?\s*>", "\n", html)
    html = re.sub(r"(?i)</\s*li\s*>", "\n", html)
    html = re.sub(r"(?i)</\s*p\s*>", "\n\n", html)
    text = re.sub(r"<[^>]+>", "", html)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return text[:SHEETS_CELL_CHAR_LIMIT]

def gs_open(spreadsheet_id: str, worksheet_name: str):
    if not os.path.exists(GOOGLE_CREDENTIALS):
        raise FileNotFoundError("Не найден credentials.json с сервисным аккаунтом Google.")
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(GOOGLE_CREDENTIALS, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(spreadsheet_id)
    return sh.worksheet(worksheet_name)

def read_rows_and_ids(ws, col_in_idx: int, from_row: int, logger: logging.Logger) -> List[Tuple[int, int]]:
    values = ws.col_values(col_in_idx)
    out: List[Tuple[int, int]] = []
    for idx, raw in enumerate(values, start=1):
        if idx < from_row or not raw:
            continue
        s = str(raw).strip()
        if not re.fullmatch(r"\d+", s):
            logger.warning(f"Строка {idx}: не число ({s!r}), пропуск.")
            continue
        out.append((idx, int(s)))
    return out

class RateLimiter:
    def __init__(self, rate_per_sec: float):
        self.rate = max(rate_per_sec, 0.1)
        self.lock = threading.Lock()
        self.last = 0.0
    def wait(self):
        with self.lock:
            now = time.time()
            delay = 1.0 / self.rate - (now - self.last)
            if delay > 0:
                time.sleep(delay)
            self.last = time.time()

def ozon_fetch_description(
    client_id: str,
    api_key: str,
    product_id: int,
    limiter: RateLimiter,
    logger: logging.Logger,
    timeout: int = TIMEOUT_SEC,
    retries: int = RETRIES,
    backoff: float = BACKOFF_BASE
) -> Optional[str]:
    headers = {"Client-Id": client_id, "Api-Key": api_key, "Content-Type": "application/json"}
    payload = {"product_id": product_id}
    for attempt in range(1, retries + 1):
        try:
            limiter.wait()
            r = requests.post(OZON_URL, headers=headers, json=payload, timeout=timeout)
            if r.status_code == 429:
                retry = float(r.headers.get("Retry-After", backoff ** attempt))
                logger.warning(f"[{product_id}] 429 Too Many Requests, ждём {retry:.1f} сек.")
                time.sleep(retry)
                continue
            if r.status_code >= 500:
                logger.warning(f"[{product_id}] {r.status_code} серверная ошибка, попытка {attempt}/{retries}")
                time.sleep(backoff ** attempt)
                continue
            data = r.json()
        except Exception as e:
            logger.error(f"[{product_id}] Ошибка запроса: {e} (попытка {attempt})")
            time.sleep(backoff ** attempt)
            continue
        if "result" in data:
            html = (data["result"] or {}).get("description") or ""
            return strip_html(html)
        logger.error(f"[{product_id}] Некорректный ответ: {str(data)[:200]}")
    return None

def write_pairs(ws, col_out_idx: int, pairs: List[Tuple[int, str]], logger: logging.Logger):
    if not pairs:
        logger.info("Нет данных для записи.")
        return
    cells = [Cell(row=r, col=col_out_idx, value=v) for r, v in pairs]
    for i in range(0, len(cells), 500):
        ws.update_cells(cells[i:i+500], value_input_option="RAW")
    logger.info(f"Записано {len(pairs)} строк в столбец #{col_out_idx}.")

def build_seo_prompt(desc: str, keywords: str) -> str:
    """Формирует промт по шаблону, если ключевые слова есть, иначе возвращает ❌"""
    if not keywords.strip():
        return "❌"

    template = (
        "У меня есть товар для маркетплейса.\n"
        "Описание товара:\n"
        "{desc}\n"
        "Ключевые слова:\n"
        "{kw}\n"
        "Задача:\n"
        "Напиши SEO-оптимизированное описание для маркетплейса (Ozon / Wildberries / Яндекс.Маркет), "
        "органично интегрировав все ключевые слова.\n\n"
        "Требования:\n"
        "Тон текста — [выбери: продающий / информативный / нейтральный / эмоциональный];\n"
        "Объем: [примерно 700–1000 символов];\n\n"
        "Используй ключевые слова естественно, без повторов и спама;\n"
        "Сделай текст убедительным и полезным для покупателя;\n"
        "В начале — 1–2 предложения, цепляющих внимание;\n"
        "Затем — преимущества и особенности товара;\n"
        "В конце — призыв к действию (например: «Закажите прямо сейчас!»).\n\n"
        "Формат вывода:\n"
        "Заголовок:\n"
        "Основное описание:\n"
        "Краткие преимущества (списком):\n"
    )
    text = template.format(desc=(desc or "").strip(), kw=(keywords or "").strip())
    return text[:SHEETS_CELL_CHAR_LIMIT]


# =========================
#         MAIN
# =========================

def main():
    start_time = time.time()
    logger = setup_logger(LOG_LEVEL)

    client_id, api_key, spreadsheet_id, worksheet_name = read_api_txt(API_FILE)
    ws = gs_open(spreadsheet_id, worksheet_name)

    col_in_idx   = a1_to_index(COL_IN_A1)
    col_out_idx  = a1_to_index(COL_OUT_A1)      # JE
    col_kw_idx   = a1_to_index(COL_KEYWORDS_A1) # JD
    col_prom_idx = a1_to_index(COL_PROMPT_A1)   # JF

    # Заголовки в 4-й строке
    ws.update_cell(HEADER_ROW, col_out_idx, "Описание")
    ws.update_cell(HEADER_ROW, col_prom_idx, "SEO +")

    rows_and_ids = read_rows_and_ids(ws, col_in_idx, START_ROW, logger)
    if not rows_and_ids:
        logger.warning("Не найдено product_id.")
        return

    logger.info(f"Найдено строк с product_id: {len(rows_and_ids)}")
    unique_ids = sorted(set(pid for _, pid in rows_and_ids))
    logger.info(f"Уникальных product_id: {len(unique_ids)}")

    limiter = RateLimiter(RATE_PER_SEC)
    fetched: Dict[int, str] = {}

    total = len(unique_ids)
    batches = (total + BATCH_SIZE - 1) // BATCH_SIZE

    for b in range(batches):
        start = b * BATCH_SIZE
        end   = min(start + BATCH_SIZE, total)
        batch = unique_ids[start:end]
        logger.info(f"Пакет {b+1}/{batches}: {len(batch)} товаров (параллельно={MAX_WORKERS}, rate≈{RATE_PER_SEC}/сек)")

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = {pool.submit(ozon_fetch_description, client_id, api_key, pid, limiter, logger): pid for pid in batch}
            done = 0
            for fut in as_completed(futures):
                pid = futures[fut]
                desc = fut.result()
                if desc:
                    fetched[pid] = desc
                else:
                    logger.warning(f"[{pid}] описание не получено.")
                done += 1
                if done % 10 == 0 or done == len(batch):
                    logger.info(f"Пакет {b+1}: готово {done}/{len(batch)}")
        logger.info(f"Пакет {b+1}/{batches} завершён.")

    # Запишем описания (JE)
    to_write_desc = [(row, fetched[pid]) for row, pid in rows_and_ids if pid in fetched]
    write_pairs(ws, col_out_idx, to_write_desc, logger)

    # Прочитаем столбец JD (ключевые слова)
    kw_col_values = ws.col_values(col_kw_idx)
    def keywords_for_row(r: int) -> str:
        return kw_col_values[r-1] if r-1 < len(kw_col_values) else ""

    # Сформируем и запишем промты (JF)
    prompt_pairs: List[Tuple[int, str]] = []
    for row, pid in rows_and_ids:
        if pid not in fetched:
            continue
        desc = fetched[pid]
        kw = keywords_for_row(row)
        prompt_text = build_seo_prompt(desc, kw)
        prompt_pairs.append((row, prompt_text))

    write_pairs(ws, col_prom_idx, prompt_pairs, logger)

    skipped = [row for row, pid in rows_and_ids if pid not in fetched]
    if skipped:
        logger.warning(f"Пропущено строк (нет данных): {len(skipped)}. Примеры: {skipped[:10]}")

    elapsed = time.time() - start_time
    logger.info("Готово.")
    logger.info(f"Время выполнения: {elapsed:.2f} сек.")


if __name__ == "__main__":
    main()
