import time
import json
import uuid
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Tuple, Set, Optional

import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# -------------------- LOGGING --------------------
LOG_FILE = "script.log"

logger = logging.getLogger("OzonScript")
logger.setLevel(logging.DEBUG)

for h in list(logger.handlers):
    logger.removeHandler(h)

# В ФАЙЛ — ТОЛЬКО CRITICAL (в консоль вообще ничего не выводим)
file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
file_handler.setLevel(logging.CRITICAL)
file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(file_handler)

# Запрещаем проброс в root logger (на всякий случай)
logger.propagate = False


def col_to_index(col: str) -> int:
    """A->0, Z->25, AA->26 ..."""
    col = (col or "").strip().upper()
    n = 0
    for ch in col:
        if not ("A" <= ch <= "Z"):
            raise ValueError(f"Bad column: {col!r}")
        n = n * 26 + (ord(ch) - ord("A") + 1)
    return n - 1


def chunked(seq, size: int):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


# -------------------- CONFIG --------------------
CLUSTER_LIST_TYPES = ("CLUSTER_TYPE_OZON", "CLUSTER_TYPE_CIS")
CREDENTIALS_FILE = "credentials.json"


def read_api_config(filename: str = "API.txt") -> tuple:
    """
    API.txt (строго 4 строки):
    1) Client-Id
    2) Api-Key
    3) SHEET_ID
    4) SHEET_NAME
    """
    with open(filename, "r", encoding="utf-8") as f:
        lines = [line.strip() for line in f.readlines() if line.strip()]

    if len(lines) < 4:
        raise ValueError("API.txt должен содержать 4 непустые строки: Client-Id, Api-Key, SheetId, SheetName")

    return lines[0], lines[1], lines[2], lines[3]


CLIENT_ID, API_KEY, SHEET_ID, SHEET_NAME = read_api_config()

HEADERS = {
    "Client-Id": CLIENT_ID,
    "Api-Key": API_KEY,
    "Content-Type": "application/json",
    "Accept": "application/json",
}

VALID_STATUSES = [
    "DATA_FILLING",
    "READY_TO_SUPPLY",
    "ACCEPTED_AT_SUPPLY_WAREHOUSE",
    "IN_TRANSIT",
    "ACCEPTANCE_AT_STORAGE_WAREHOUSE",
    "REPORTS_CONFIRMATION_AWAITING",
]

STATUS_TO_COLUMN = {
    "DATA_FILLING": "BW",
    "READY_TO_SUPPLY": "BX",
    "ACCEPTED_AT_SUPPLY_WAREHOUSE": "BY",
    "IN_TRANSIT": "BZ",
    "ACCEPTANCE_AT_STORAGE_WAREHOUSE": "CA",
    "REPORTS_CONFIRMATION_AWAITING": "CB",
}
STATUS_COLS = list(STATUS_TO_COLUMN.values())


# ---------- НОРМАЛИЗАЦИЯ ИМЕН ----------
NAME_REPLACEMENTS: Dict[str, str] = {
    "РОСТОВ_НА_ДОНУ_2": "Ростов-на-Дону",
}

RU_LOWER_WORDS = {
    "и", "в", "во", "на", "к", "ко", "о", "об", "от", "до", "за", "из", "с", "со",
    "у", "по", "при", "для", "над", "под", "без", "про"
}


def clean_wh_name(name: str) -> str:
    name = (name or "").strip()
    suffixes = ["_РФЦ", " РФЦ", "-РФЦ", "- РФЦ"]
    for suf in suffixes:
        if name.endswith(suf):
            name = name[: -len(suf)].strip()
            break
    return name


def apply_name_replacements(name: str) -> str:
    name = (name or "").strip()
    key = name.upper()
    return NAME_REPLACEMENTS.get(key, name)


def _cap_ru_word(word: str, force_capital: bool = False) -> str:
    w = (word or "").strip()
    if not w:
        return w
    if w.isdigit():
        return w
    if w.isupper() and len(w) <= 3:
        return w

    lw = w.lower()
    if (not force_capital) and (lw in RU_LOWER_WORDS):
        return lw
    return lw[:1].upper() + lw[1:]


def smart_title_ru(text: str) -> str:
    s = (text or "").strip()
    if not s:
        return s

    s = " ".join(s.replace("_", " ").split())
    words = s.split(" ")
    out_words: List[str] = []

    for wi, word in enumerate(words):
        parts = word.split("-")
        out_parts: List[str] = []
        for pi, part in enumerate(parts):
            force = (wi == 0 and pi == 0)
            out_parts.append(_cap_ru_word(part, force_capital=force))
        out_words.append("-".join(out_parts))

    return " ".join(out_words).strip()


def normalize_display_name(name: str) -> str:
    name = clean_wh_name(name)
    replaced = apply_name_replacements(name)
    if replaced != name:
        return replaced.strip()
    return smart_title_ru(replaced).strip()


# -------------------- OZON PROCESSOR --------------------
class OzonDataProcessor:
    def __init__(self, cluster_list_types: Tuple[str, ...] = CLUSTER_LIST_TYPES):
        self.cluster_list_types = tuple([ct for ct in (cluster_list_types or CLUSTER_LIST_TYPES) if str(ct).strip()])
        if not self.cluster_list_types:
            self.cluster_list_types = CLUSTER_LIST_TYPES

        self.session = self._create_retry_session()
        self.session.headers.update(HEADERS)

        self._unknown_warehouses: Set[str] = set()

        self.URL_SUPPLY_LIST_V3 = "https://api-seller.ozon.ru/v3/supply-order/list"
        self.URL_SUPPLY_GET_V3 = "https://api-seller.ozon.ru/v3/supply-order/get"
        self.URL_BUNDLE_V1 = "https://api-seller.ozon.ru/v1/supply-order/bundle"
        self.URL_CLUSTER_LIST_V1 = "https://api-seller.ozon.ru/v1/cluster/list"
        self.URL_WAREHOUSE_LIST_V2 = "https://api-seller.ozon.ru/v2/warehouse/list"

    def _create_retry_session(
        self,
        retries: int = 3,
        backoff_factor: float = 1.0,
        status_forcelist=(429, 500, 502, 503, 504),
    ) -> requests.Session:
        session = requests.Session()
        retry = Retry(
            total=retries,
            read=retries,
            connect=retries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist,
            allowed_methods=["POST", "GET"],
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def _post_json(self, url: str, payload: dict, timeout: int = 60, tag: str = "") -> Optional[dict]:
        rid = f"{tag}-{uuid.uuid4().hex[:8]}" if tag else uuid.uuid4().hex[:8]
        resp = None
        try:
            resp = self.session.post(url, json=payload, timeout=timeout)
            resp.raise_for_status()
            return resp.json() if resp.content else {}
        except Exception:
            # не шумим в консоль; только CRITICAL наружу не пишем (это не критично)
            return None

    # -------------------- GOOGLE SHEETS --------------------
    def _init_google_resources(self):
        attempt = 0
        while attempt < 3:
            try:
                scope = [
                    "https://www.googleapis.com/auth/spreadsheets",
                    "https://www.googleapis.com/auth/drive",
                ]
                creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
                client = gspread.authorize(creds)
                ws = client.open_by_key(SHEET_ID).worksheet(SHEET_NAME)

                sku_list = ws.col_values(11)[4:]  # K, с 5-й строки
                clean_skus = [str(s).strip() for s in sku_list if str(s).strip()]
                return ws, clean_skus
            except Exception as e:
                attempt += 1
                time.sleep(2 * attempt)
                if attempt == 3:
                    logger.critical("Google Sheets init failed: %r", e, exc_info=True)
                    raise

    # -------------------- OZON: v3 supply-order/list --------------------
    def fetch_supply_order_ids(self) -> List[int]:
        all_ids: List[int] = []
        last_id = ""
        limit = 100
        max_pages = 300

        for _ in range(max_pages):
            payload = {
                "filter": {"states": VALID_STATUSES},
                "limit": limit,
                "sort_by": "ORDER_CREATION",
                "sort_dir": "DESC",
            }
            if last_id:
                payload["last_id"] = last_id

            data = self._post_json(self.URL_SUPPLY_LIST_V3, payload, timeout=60, tag="supply_list")
            if not data:
                break

            batch = data.get("order_ids") or []
            if not batch:
                break

            all_ids.extend(batch)
            new_last_id = data.get("last_id") or ""
            if (not new_last_id) or (new_last_id == last_id) or (len(batch) < limit):
                break

            last_id = new_last_id
            time.sleep(0.05)

        seen = set()
        uniq = []
        for x in all_ids:
            if x in seen:
                continue
            seen.add(x)
            uniq.append(x)
        return uniq

    # -------------------- OZON: v3 supply-order/get --------------------
    def fetch_supply_orders(self) -> Tuple[List[Dict], Dict[str, str]]:
        order_ids = self.fetch_supply_order_ids()
        if not order_ids:
            return [], {}

        orders: List[Dict] = []
        warehouse_names: Dict[str, str] = {}

        def extract_warehouses_from_response(data: Dict) -> None:
            cand_lists = []
            if isinstance(data.get("warehouses"), list):
                cand_lists.append(data["warehouses"])
            if isinstance(data.get("result"), dict) and isinstance(data["result"].get("warehouses"), list):
                cand_lists.append(data["result"]["warehouses"])

            for wh_list in cand_lists:
                for wh in wh_list or []:
                    if not isinstance(wh, dict):
                        continue
                    wid = wh.get("warehouse_id") or wh.get("id") or wh.get("warehouseId")
                    name = wh.get("name") or wh.get("warehouse_name") or ""
                    if wid is None or not name:
                        continue
                    warehouse_names[str(wid)] = str(name)

        batch_size = 100
        for i in range(0, len(order_ids), batch_size):
            batch = order_ids[i: i + batch_size]
            data = self._post_json(self.URL_SUPPLY_GET_V3, {"order_ids": batch}, timeout=60, tag="supply_get")
            if not data:
                continue

            part_orders = []
            if isinstance(data.get("orders"), list):
                part_orders = data["orders"]
            elif isinstance(data.get("result"), dict) and isinstance(data["result"].get("orders"), list):
                part_orders = data["result"]["orders"]

            orders.extend(part_orders)
            extract_warehouses_from_response(data)

            for o in part_orders:
                for s in (o.get("supplies") or []):
                    st = s.get("storage_warehouse") or {}
                    if isinstance(st, dict):
                        wid = s.get("storage_warehouse_id") or st.get("warehouse_id") or st.get("id")
                        nm = st.get("name")
                        if wid is not None and nm:
                            warehouse_names[str(wid)] = str(nm)

        return orders, warehouse_names

    # -------------------- OZON: v1 supply-order/bundle --------------------
    def fetch_bundle_items(self, bundle_id: str) -> List[Dict]:
        items: List[Dict] = []
        last_id = ""
        has_next = True

        while has_next:
            payload = {
                "bundle_ids": [bundle_id],
                "is_asc": True,
                "limit": 100,
                "query": "",
                "sort_field": "UNSPECIFIED",
            }
            if last_id:
                payload["last_id"] = last_id

            data = self._post_json(self.URL_BUNDLE_V1, payload, timeout=60, tag=f"bundle_{bundle_id}")
            if not data:
                break

            items.extend(data.get("items", []) or [])
            has_next = bool(data.get("has_next", False))
            last_id = data.get("last_id", "") or ""

        return items

    # -------------------- OZON: v1 cluster/list --------------------
    def fetch_cluster_info(self) -> Tuple[Dict[str, str], Dict[str, str], Dict[str, str]]:
        cluster_names: Dict[str, str] = {}
        warehouse_to_cluster: Dict[str, str] = {}
        warehouse_names_from_cluster: Dict[str, str] = {}

        for ct in self.cluster_list_types:
            payload = {"cluster_type": ct, "cluster_ids": []}
            data = self._post_json(self.URL_CLUSTER_LIST_V1, payload, timeout=60, tag=f"cluster_list_{ct}")
            if not data:
                continue

            clusters = data.get("clusters") or (data.get("result") or {}).get("clusters") or []
            for cluster in clusters or []:
                if not isinstance(cluster, dict):
                    continue

                cname = cluster.get("name") or ""
                cid = cluster.get("id")
                if cid is not None and cname:
                    cluster_names[str(cid)] = str(cname)

                for log_cluster in (cluster.get("logistic_clusters") or []):
                    if not isinstance(log_cluster, dict):
                        continue
                    for wh in (log_cluster.get("warehouses") or []):
                        if not isinstance(wh, dict):
                            continue
                        wid = wh.get("warehouse_id") or wh.get("id") or wh.get("warehouseId")
                        if wid is None:
                            continue
                        wid_s = str(wid)

                        if cname:
                            warehouse_to_cluster.setdefault(wid_s, str(cname))

                        wh_name = wh.get("name") or wh.get("warehouse_name")
                        if wh_name:
                            warehouse_names_from_cluster.setdefault(wid_s, str(wh_name))

        return cluster_names, warehouse_to_cluster, warehouse_names_from_cluster

    # -------------------- OZON: v1 warehouse/list --------------------
    def fetch_warehouse_list_v1(self) -> Dict[str, str]:
        result: Dict[str, str] = {}
        cursor = ""
        limit = 200

        while True:
            payload = {"limit": limit}
            if cursor:
                payload["cursor"] = cursor

            data = self._post_json(self.URL_WAREHOUSE_LIST_V2, payload, timeout=60, tag="warehouse_list")
            if not data:
                break

            warehouses = data.get("warehouses") or []
            if not isinstance(warehouses, list):
                break

            for wh in warehouses:
                if not isinstance(wh, dict):
                    continue
                wid = wh.get("warehouse_id") or wh.get("id")
                name = wh.get("name") or ""
                if wid is None or not name:
                    continue
                result[str(wid)] = str(name)

            # Проверяем наличие следующей страницы
            has_next = data.get("has_next")
            if not has_next:
                break
            cursor = data.get("cursor") or ""
            if not cursor:
                break

        return result

    # -------------------- PROCESSING --------------------
    @staticmethod
    def _extract_warehouse_id_from_supply(supply: Dict) -> str:
        storage_obj = supply.get("storage_warehouse") or {}
        candidates = [
            supply.get("storage_warehouse_id"),
            supply.get("storageWarehouseId"),
            supply.get("warehouse_id"),
            supply.get("warehouseId"),
            supply.get("dropoff_warehouse_id"),
            supply.get("dropoffWarehouseId"),
        ]
        if isinstance(storage_obj, dict):
            candidates.extend([storage_obj.get("warehouse_id"), storage_obj.get("id")])

        for c in candidates:
            if c is None:
                continue
            s = str(c).strip()
            if s:
                return s
        return ""

    def process_ozon_data(
        self,
    ) -> Tuple[Dict[str, Dict[str, int]], Dict[str, Dict[str, Dict[str, int]]], Dict[str, str]]:
        orders, warehouse_names_from_supply = self.fetch_supply_orders()

        sku_counts: Dict[str, Dict[str, int]] = {}
        sku_status_warehouses: Dict[str, Dict[str, Dict[str, int]]] = {}

        tasks = []
        with ThreadPoolExecutor(max_workers=4) as executor:
            for order in orders:
                status = order.get("state")
                if status not in STATUS_TO_COLUMN:
                    continue

                for supply in (order.get("supplies") or []):
                    if not isinstance(supply, dict):
                        continue

                    bundle_id = supply.get("bundle_id") or supply.get("bundleId")
                    if not bundle_id:
                        continue

                    warehouse_id = self._extract_warehouse_id_from_supply(supply)

                    st = supply.get("storage_warehouse") or {}
                    if warehouse_id and isinstance(st, dict) and st.get("name"):
                        warehouse_names_from_supply[warehouse_id] = str(st["name"])

                    tasks.append(
                        executor.submit(self._process_single_bundle, str(bundle_id), str(status), warehouse_id)
                    )

            for future in as_completed(tasks):
                status, warehouse_id, items = future.result()
                if not items:
                    continue

                for item in items:
                    sku = str(item.get("sku") or "").strip()
                    if not sku:
                        continue
                    try:
                        qty = int(item.get("quantity", 0) or 0)
                    except (TypeError, ValueError):
                        continue
                    if qty <= 0:
                        continue

                    sku_counts.setdefault(sku, {})
                    sku_counts[sku][status] = sku_counts[sku].get(status, 0) + qty

                    if warehouse_id:
                        sku_status_warehouses.setdefault(sku, {})
                        sku_status_warehouses[sku].setdefault(status, {})
                        sku_status_warehouses[sku][status][warehouse_id] = (
                            sku_status_warehouses[sku][status].get(warehouse_id, 0) + qty
                        )

        return sku_counts, sku_status_warehouses, warehouse_names_from_supply

    def _process_single_bundle(self, bundle_id: str, status: str, warehouse_id: str):
        time.sleep(0.1)
        return status, warehouse_id, self.fetch_bundle_items(bundle_id)

    def _build_cell_text_and_note(
        self,
        wh_counts: Dict[str, int],
        warehouse_names: Dict[str, str],
        warehouse_to_cluster: Dict[str, str],
    ) -> Tuple[str, str]:
        """
        Значение ячейки: "Склад: qty" (по убыванию qty).
        Note: уникальные кластеры В ТОМ ЖЕ ПОРЯДКЕ, в котором они впервые встретились
        при проходе складов в ячейке (то есть порядок соответствует value).
          🔹 Имя кластера
        """
        if not wh_counts:
            return "", ""

        items_sorted = sorted(wh_counts.items(), key=lambda x: (-x[1], x[0]))

        value_lines: List[str] = []

        # ВАЖНО: сохраняем порядок первого появления кластера (в порядке складов)
        clusters_ordered: List[str] = []
        clusters_seen: Set[str] = set()

        for wh_id, qty in items_sorted:
            wh_name = warehouse_names.get(wh_id)
            if wh_name:
                wh_name = normalize_display_name(wh_name)
            else:
                self._unknown_warehouses.add(wh_id)
                wh_name = f"Склад {wh_id}"

            value_lines.append(f"{wh_name}: {qty}")

            cluster_name = warehouse_to_cluster.get(wh_id)
            if cluster_name:
                cluster_name = normalize_display_name(cluster_name)
                if cluster_name not in clusters_seen:
                    clusters_seen.add(cluster_name)
                    clusters_ordered.append(cluster_name)

        value_text = "\n".join(value_lines)
        if not clusters_ordered:
            return value_text, ""

        note_text = "\n".join([f"🔹 {c}" for c in clusters_ordered])
        return value_text, note_text

    def update_sheet(
        self,
        sheet,
        sku_list: List[str],
        sku_counts: Dict[str, Dict[str, int]],
        sku_status_warehouses: Dict[str, Dict[str, Dict[str, int]]],
        warehouse_names: Dict[str, str],
        warehouse_to_cluster: Dict[str, str],
    ):
        last_row = len(sku_list) + 4

        # чистим значения; заметки перезапишем updateCells (пустыми тоже, чтобы удалить старые)
        sheet.batch_clear(
            [
                "BW3:CB3",
                f"BP5:BP{last_row}",
                f"BW5:BW{last_row}",
                f"BX5:BX{last_row}",
                f"BY5:BY{last_row}",
                f"BZ5:BZ{last_row}",
                f"CA5:CA{last_row}",
                f"CB5:CB{last_row}",
            ]
        )

        # BP и суммы по колонкам
        plain_updates = []
        column_sums: Dict[str, int] = {col: 0 for col in STATUS_TO_COLUMN.values()}

        # (row, col) -> (value_text, note_text)
        cell_map: Dict[Tuple[int, str], Tuple[str, str]] = {}

        for row_idx, sku in enumerate(sku_list, start=5):
            counts_for_sku = sku_counts.get(sku, {})
            wh_for_sku = sku_status_warehouses.get(sku, {})

            row_total = 0

            for status, col in STATUS_TO_COLUMN.items():
                total_for_status = int(counts_for_sku.get(status, 0) or 0)
                if total_for_status <= 0:
                    continue

                wh_counts = wh_for_sku.get(status, {}) or {}
                value_text, note_text = self._build_cell_text_and_note(wh_counts, warehouse_names, warehouse_to_cluster)
                if value_text or note_text:
                    cell_map[(row_idx, col)] = (value_text, note_text)

                column_sums[col] += total_for_status
                row_total += total_for_status

            if row_total > 0:
                plain_updates.append({"range": f"BP{row_idx}", "values": [[row_total]]})

        for col, total in column_sums.items():
            if total != 0:
                plain_updates.append({"range": f"{col}3", "values": [[total]]})

        if plain_updates:
            sheet.batch_update(plain_updates)

        # Статусные колонки: VALUE + NOTE одним batchUpdate. Пустым ставим note="" → удаляет старые заметки.
        sheet_id = sheet.id
        start_row = 5
        end_row_incl = last_row

        start_col_idx = col_to_index(STATUS_COLS[0])          # BW
        end_col_idx_excl = col_to_index(STATUS_COLS[-1]) + 1 # CB + 1

        requests_api = []
        max_rows_per_request = 200

        all_rows = list(range(start_row, end_row_incl + 1))
        for rows_chunk in chunked(all_rows, max_rows_per_request):
            chunk_start = rows_chunk[0]
            chunk_end_incl = rows_chunk[-1]

            row_datas: List[dict] = []
            for r in rows_chunk:
                values: List[dict] = []
                for col in STATUS_COLS:
                    value_text, note_text = cell_map.get((r, col), ("", ""))
                    values.append({
                        "userEnteredValue": {"stringValue": value_text or ""},
                        "note": note_text or "",
                    })
                row_datas.append({"values": values})

            requests_api.append({
                "updateCells": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": chunk_start - 1,
                        "endRowIndex": chunk_end_incl,  # exclusive
                        "startColumnIndex": start_col_idx,
                        "endColumnIndex": end_col_idx_excl,
                    },
                    "rows": row_datas,
                    "fields": "userEnteredValue,note",
                }
            })

        if requests_api:
            sheet.spreadsheet.batch_update({"requests": requests_api})

    def run(self):
        start_time = time.time()
        try:
            with ThreadPoolExecutor(max_workers=3) as ex:
                future_google = ex.submit(self._init_google_resources)
                future_ozon = ex.submit(self.process_ozon_data)
                future_clusters = ex.submit(self.fetch_cluster_info)

                sheet, sku_list = future_google.result()
                sku_counts, sku_status_warehouses, warehouse_names_from_supply = future_ozon.result()
                _, warehouse_to_cluster, warehouse_names_from_cluster = future_clusters.result()

            warehouse_names_from_v1 = self.fetch_warehouse_list_v1()

            warehouse_names: Dict[str, str] = {}
            warehouse_names.update(warehouse_names_from_v1)
            warehouse_names.update(warehouse_names_from_cluster)
            warehouse_names.update(warehouse_names_from_supply)

            self.update_sheet(
                sheet,
                sku_list,
                sku_counts,
                sku_status_warehouses,
                warehouse_names,
                warehouse_to_cluster,
            )

        except Exception as e:
            logger.critical("Critical error in main process: %r", e, exc_info=True)
        finally:
            elapsed = time.time() - start_time
            print(f"Время выполнения: {elapsed:.2f} сек.")


if __name__ == "__main__":
    OzonDataProcessor(cluster_list_types=CLUSTER_LIST_TYPES).run()
