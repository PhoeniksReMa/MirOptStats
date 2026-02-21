# -*- coding: utf-8 -*-
import re
import time
import logging
import requests
import gspread
from typing import List, Dict, Optional, Tuple
from oauth2client.service_account import ServiceAccountCredentials
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib3.util import Retry
from requests.adapters import HTTPAdapter
from time import perf_counter
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation

DIGITS_RE = re.compile(r"\d+")


class OzonAnalyticsStocks:
    """
    Экспорт остатков с параллельной выборкой по кластерам.
    Пакетные запросы: объединяем до N cluster_id в одном HTTP-запросе и
    раскладываем ответ по группам на основе item['cluster_id'] (если поле присутствует).

    Диапазоны столбцов:
    - AC..AK — суммы по всем складам
    - AL — резерв
    - AM..BF — суммы available_stock_count по cluster_ids из <КОЛОНКА>4
    - BG..CC — пусто (пропуск)
    - CD — ads (общий ответ; округлено до 2 знаков, ROUND_HALF_UP)
    - CE — days_without_sales (общий ответ)
    - CF — idc (общий ответ)
    - CG — turnover_grade (общий ответ; перевод по словарю)
    - CH..CK — параметры кластера из CK2 (ads_cluster, days_without_sales_cluster, idc_cluster, turnover_grade_cluster)
    - CL..CO — параметры кластера из CO2
    - CP..CS — параметры кластера из CS2
    - CT..CW — параметры кластера из CW2
    - CX..DA — параметры кластера из DA2
    - DB..DE — параметры кластера из DE2
    - DF..DI — параметры кластера из DI2
    """

    SHEET_RANGE_CLEAR = "AC5:DI1000"
    CLUSTER_COLS = [
        "AM", "AN", "AO", "AP", "AQ", "AR", "AS",
        "AT", "AU", "AV", "AW", "AX", "AY", "AZ",
        "BA", "BB", "BC", "BD", "BE", "BF"
    ]

    SINGLE_CLUSTER_CELLS_ORDERED = [
        ("CK2", "CH..CK"),
        ("CO2", "CL..CO"),
        ("CS2", "CP..CS"),
        ("CW2", "CT..CW"),
        ("DA2", "CX..DA"),
        ("DE2", "DB..DE"),
        ("DI2", "DF..DI"),
    ]

    TURNOVER_GRADE_MAP = {
        "TURNOVER_GRADE_NONE": "нет статуса ликвидности.",
        "DEFICIT": "Хватит до 28 дней",
        "POPULAR": "Хватит на 28–56 дней",
        "ACTUAL": "Хватит на 56–120 дней",
        "SURPLUS": "Продаётся медленно, хватит > 120 дней",
        "NO_SALES": "Без продаж последние 28 дней",
        "WAS_NO_SALES": "Без продаж и остатков последние 28 дней",
        "RESTRICTED_NO_SALES": "Запрет FBO",
        "COLLECTING_DATA": "Сбор данных",
        "WAITING_FOR_SUPPLY": "Сделайте поставку для сбора данных",
        "WAS_DEFICIT": "Был дефицитным последние 56 дней",
        "WAS_POPULAR": "Был очень популярным последние 56 дней",
        "WAS_ACTUAL": "Был популярным последние 56 дней",
        "WAS_SURPLUS": "Был избыточным последние 56 дней",
    }

    def __init__(
        self,
        credentials_path: str = "credentials.json",
        batch_size: int = 100,
        max_workers: int = 12,
        cluster_workers: int = 6,
        single_cluster_workers: int = 5,
        clusters_pack_size: Optional[int] = 12,  # увеличили до 12
        request_timeout: int = 30,
        retry_total: int = 5,
        retry_backoff: float = 0.5,
        delay_between_batches: float = 0.0,
        write_zeroes: bool = False,
        log_level: int = logging.INFO,
    ):
        self._setup_logging(level=log_level)

        with open("API.txt", "r", encoding="utf-8") as f:
            lines = [line.strip() for line in f.readlines() if line.strip()]
        if len(lines) < 4:
            raise ValueError(
                "Файл API.txt должен содержать 4 строки (client_id, api_key, spreadsheet_id, sheet_name)"
            )

        self.client_id = lines[0]
        self.api_key = lines[1]
        self.spreadsheet_id = lines[2]
        self.sheet_name = lines[3]

        self.base_url = "https://api-seller.ozon.ru"
        self.headers = {
            "Client-Id": self.client_id,
            "Api-Key": self.api_key,
            "Content-Type": "application/json",
        }

        self.session = self._make_session(
            request_timeout=request_timeout,
            retry_total=retry_total,
            retry_backoff=retry_backoff,
        )
        self.request_timeout = request_timeout
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.cluster_workers = cluster_workers
        self.single_cluster_workers = single_cluster_workers
        self.delay_between_batches = delay_between_batches
        self.write_zeroes = write_zeroes
        self.clusters_pack_size = clusters_pack_size

        self.gc = self._init_google_sheets(credentials_path)
        self._worksheet = None

        self.log.info(
            "Config: batch_size=%s, max_workers=%s, cluster_workers=%s, single_cluster_workers=%s, clusters_pack_size=%s, delay_between_batches=%.3f, write_zeroes=%s",
            self.batch_size, self.max_workers, self.cluster_workers, self.single_cluster_workers,
            self.clusters_pack_size, self.delay_between_batches, self.write_zeroes
        )

    # ------------------------------- Logging

    def _setup_logging(self, level: int):
        fmt = "[%(asctime)s] %(levelname)s %(name)s: %(message)s"
        handlers = [logging.StreamHandler()]
        logging.basicConfig(level=level, format=fmt, handlers=handlers)
        self.log = logging.getLogger("OzonAnalyticsStocks")

    # ------------------------------- Infra

    def _init_google_sheets(self, credentials_path: str):
        scope = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_path, scope)
        return gspread.authorize(creds)

    def _get_worksheet(self):
        if self._worksheet is None:
            sh = self.gc.open_by_key(self.spreadsheet_id)
            self._worksheet = sh.worksheet(self.sheet_name)
        return self._worksheet

    def _make_session(self, request_timeout: int, retry_total: int, retry_backoff: float) -> requests.Session:
        s = requests.Session()
        retry = Retry(
            total=retry_total,
            read=retry_total,
            connect=retry_total,
            backoff_factor=retry_backoff,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET", "POST"]),
            raise_on_status=False,
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=100, pool_maxsize=100)
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        s.request_timeout = request_timeout
        return s

    # ------------------------------- Utils

    @staticmethod
    def _as_int_or_none(v) -> Optional[int]:
        if v is None:
            return None
        try:
            return int(v)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _round_two_half_up(value) -> Optional[str]:
        if value is None:
            return None
        try:
            s = str(value).replace(",", ".").strip()
            d = Decimal(s)
            q = d.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            return format(q, "f")
        except (InvalidOperation, ValueError):
            return None

    # ------------------------------- Data fetching

    def get_sku_list_from_sheet(self) -> List[str]:
        ws = self._get_worksheet()
        values = ws.get("K5:K")
        skus = [str(row[0]).strip() for row in values if row and str(row[0]).strip()]
        self.log.info("Получено %d SKU из таблицы", len(skus))
        return skus

    def collect_cluster_ids_map_batch(self) -> Dict[str, Optional[List[int]]]:
        ws = self._get_worksheet()
        result: Dict[str, Optional[List[int]]] = {col: None for col in self.CLUSTER_COLS}
        try:
            rng = f"{self.CLUSTER_COLS[0]}4:{self.CLUSTER_COLS[-1]}4"  # AM4:BF4
            rows = ws.batch_get([rng], major_dimension="ROWS")
            row = rows[0][0] if rows and rows[0] else []
            for i, col in enumerate(self.CLUSTER_COLS):
                raw = row[i] if i < len(row) else ""
                if raw:
                    ids = [int(x) for x in DIGITS_RE.findall(str(raw))]
                    seen = set(); uniq = []
                    for cid in ids:
                        if cid not in seen:
                            seen.add(cid); uniq.append(cid)
                    result[col] = uniq if uniq else None
                self.log.info("AM..BF batch: %s4 -> %s", col, result[col])
            return result
        except Exception as e:
            self.log.error("batch_get AM4:BF4 не удался: %s", e)
            return result

    def get_single_cluster_ids_batch(self, cells: List[str]) -> Dict[str, Optional[int]]:
        ws = self._get_worksheet()
        res = {c: None for c in cells}
        try:
            values_list = ws.batch_get(cells, major_dimension="ROWS")
            for idx, cell in enumerate(cells):
                vals = values_list[idx] if idx < len(values_list) else []
                raw = vals[0][0] if (vals and vals[0]) else None
                if raw:
                    m = DIGITS_RE.search(str(raw))
                    if m:
                        res[cell] = int(m.group(0))
                self.log.info("batch singles: %s -> %s (raw=%r)", cell, res[cell], raw)
            return res
        except Exception as e:
            self.log.error("batch_get для %s не удался: %s", cells, e)
            return res

    def _fetch_batch(self, batch: List[str], batch_idx: int, cluster_ids: Optional[List[int]] = None) -> Tuple[int, List[dict]]:
        url = f"{self.base_url}/v1/analytics/stocks"
        payload = {"skus": batch, "warehouse_ids": [], "limit": len(batch)}
        if cluster_ids:
            payload["cluster_ids"] = cluster_ids

        started = perf_counter()
        try:
            resp = self.session.post(url, headers=self.headers, json=payload, timeout=self.request_timeout)
            code = resp.status_code
            data = {}
            try:
                data = resp.json() if code == 200 else {}
            except ValueError:
                self.log.warning("Батч #%d: невалидный JSON, HTTP %d", batch_idx, code)
            items = data.get("items", []) if isinstance(data, dict) else []
            dur = perf_counter() - started
            self.log.debug(
                "Батч #%d: %d SKU, HTTP %d, items=%d, %.3fs (cluster_ids=%s)",
                batch_idx, len(batch), code, len(items), dur, cluster_ids or "-"
            )
            if code != 200:
                self.log.warning("Батч #%d: HTTP %d, тело (усечено): %s", batch_idx, code, str(data)[:500])
            return batch_idx, items
        except requests.exceptions.RequestException as e:
            dur = perf_counter() - started
            self.log.error("Батч #%d: ошибка %s (%.3fs)", batch_idx, repr(e), dur)
            return batch_idx, []

    def get_stocks_data(
        self,
        skus: List[str],
        cluster_ids: Optional[List[int]] = None,
        max_workers_override: Optional[int] = None
    ) -> Dict:
        """
        Параллельный запрос по батчам SKU (и для 'all', и для наборов/пачек cluster_ids).
        Простая адаптация под ошибки: лёгкий локальный бэкофф на пустые/ошибочные ответы.
        """
        batches = [skus[i:i + self.batch_size] for i in range(0, len(skus), self.batch_size)]
        eff_workers = max_workers_override or self.max_workers
        self.log.info(
            "Всего батчей: %d (batch_size=%d, max_workers=%d, cluster_ids=%s)",
            len(batches), self.batch_size, eff_workers, cluster_ids or "-"
        )

        all_items: List[dict] = []
        started = perf_counter()
        errors_soft = 0

        def run_one(batch, idx):
            nonlocal errors_soft
            _, items = self._fetch_batch(batch, idx, cluster_ids)
            if not items:
                errors_soft += 1
                # локальный микробэкофф, чтобы разгладить пики
                time.sleep(0.05)
            return idx, items

        with ThreadPoolExecutor(max_workers=eff_workers) as ex:
            futures = []
            for idx, batch in enumerate(batches, start=1):
                futures.append(ex.submit(run_one, batch, idx))
                if self.delay_between_batches > 0:
                    time.sleep(self.delay_between_batches)
                else:
                    time.sleep(0.01)  # мягкий джиттер ~10 мс

            for fut in as_completed(futures):
                _, items = fut.result()
                if items:
                    all_items.extend(items)

        total_dur = perf_counter() - started
        self.log.info("Получено всего items=%d за %.3fs (cluster_ids=%s)",
                      len(all_items), total_dur, cluster_ids or "-")
        return {"items": all_items}

    # ------------------------------- Transform

    def _sum_all_warehouses(self, items_all: List[dict]) -> Dict[str, Dict[str, int]]:
        sums_all: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
        params = (
            "available_stock_count", "other_stock_count", "requested_stock_count",
            "return_from_customer_stock_count", "return_to_seller_stock_count",
            "stock_defect_stock_count", "transit_defect_stock_count",
            "transit_stock_count", "valid_stock_count"
        )
        for it in items_all:
            sku = it.get("sku")
            if sku is None:
                continue
            ssku = str(sku)
            tgt = sums_all[ssku]
            for p in params:
                v = it.get(p)
                if v:
                    try:
                        tgt[p] += int(v)
                    except (TypeError, ValueError):
                        pass
        return sums_all

    def _collect_params_from_items(
        self,
        items_all: List[dict],
        *,
        cluster: bool = False
    ) -> Dict[str, Dict[str, Optional[str]]]:
        postfix = "_cluster" if cluster else ""
        f_ads = f"ads{postfix}"
        f_days = f"days_without_sales{postfix}"
        f_idc = f"idc{postfix}"
        f_grade = f"turnover_grade{postfix}"
        tmap = self.TURNOVER_GRADE_MAP

        params_by_sku: Dict[str, Dict[str, Optional[str]]] = {}
        for it in items_all:
            sku = it.get("sku")
            if sku is None:
                continue
            ssku = str(sku)
            p = params_by_sku.setdefault(
                ssku,
                {"ads": None, "days_without_sales": None, "idc": None, "turnover_grade": None}
            )

            if p["ads"] is None:
                av = it.get(f_ads)
                if av is not None:
                    rounded = self._round_two_half_up(av)
                    if rounded is not None:
                        p["ads"] = rounded

            if p["days_without_sales"] is None:
                dv = it.get(f_days)
                if dv is not None:
                    p["days_without_sales"] = dv

            if p["idc"] is None:
                iv = it.get(f_idc)
                if iv is not None:
                    p["idc"] = iv

            if p["turnover_grade"] is None:
                gv = it.get(f_grade)
                if gv is not None:
                    p["turnover_grade"] = tmap.get(gv, gv)

        return params_by_sku

    def _sum_by_cluster_col(self, cluster_items_by_col: Dict[str, List[dict]]) -> Dict[str, Dict[str, int]]:
        cache: Dict[int, Dict[str, int]] = {}
        sums_by_col: Dict[str, Dict[str, int]] = {}

        for col, items in cluster_items_by_col.items():
            key = id(items)
            if key not in cache:
                sums: Dict[str, int] = defaultdict(int)
                for it in (items or []):
                    sku = it.get("sku")
                    if sku is None:
                        continue
                    ssku = str(sku)
                    v = it.get("available_stock_count")
                    if v:
                        try:
                            sums[ssku] += int(v)
                        except (TypeError, ValueError):
                            pass
                cache[key] = sums
            sums_by_col[col] = cache[key]
        return sums_by_col

    # ------------------------------- Packing helpers

    def _dedupe_cluster_sets(self, cluster_ids_map: Dict[str, Optional[List[int]]]) -> Dict[Tuple[int, ...], List[str]]:
        groups: Dict[Tuple[int, ...], List[str]] = {}
        for col, ids in cluster_ids_map.items():
            if not ids:
                continue
            key = tuple(ids)
            groups.setdefault(key, []).append(col)
        return groups

    def _pack_groups(self, groups: Dict[Tuple[int, ...], List[str]], pack_size: int) -> List[List[Tuple[int, ...]]]:
        if not groups:
            return []
        keys_sorted = sorted(
            groups.keys(),
            key=lambda k: (len(groups[k]), len(k), k),
            reverse=True
        )

        packs: List[List[Tuple[int, ...]]] = []
        cur_pack: List[Tuple[int, ...]] = []
        cur_ids: set = set()

        for key in keys_sorted:
            key_ids = set(key)
            if not cur_pack or len(cur_ids | key_ids) <= pack_size:
                cur_pack.append(key)
                cur_ids |= key_ids
            else:
                packs.append(cur_pack)
                cur_pack = [key]
                cur_ids = set(key_ids)
        if cur_pack:
            packs.append(cur_pack)
        return packs

    def _fetch_clusters_parallel(self, skus: List[str], groups: Dict[Tuple[int, ...], List[str]]) -> Dict[Tuple[int, ...], List[dict]]:
        results: Dict[Tuple[int, ...], List[dict]] = {}
        if not groups:
            return results

        self.log.info("Уникальных наборов cluster_ids: %d", len(groups))
        started = perf_counter()
        with ThreadPoolExecutor(max_workers=self.cluster_workers) as ex:
            future_map = {
                ex.submit(self.get_stocks_data, skus, list(key), 10): key
                for key in groups.keys()
            }
            for fut in as_completed(future_map):
                key = future_map[fut]
                try:
                    resp = fut.result()
                    results[key] = resp.get("items", [])
                    self.log.info("Группа %s -> items=%d (колонки: %s)",
                                  list(key), len(results[key]), ", ".join(groups[key]))
                except Exception as e:
                    self.log.error("Ошибка в группе %s: %s", key, e)
                    results[key] = []
        self.log.info("Запросы по кластерам (AM..BF) завершены за %.3fs", perf_counter() - started)
        return results

    def _fetch_clusters_packed_parallel(
        self,
        skus: List[str],
        groups: Dict[Tuple[int, ...], List[str]],
        pack_size: int
    ) -> Dict[Tuple[int, ...], List[dict]]:
        results: Dict[Tuple[int, ...], List[dict]] = {k: [] for k in groups.keys()}
        packs = self._pack_groups(groups, pack_size)
        if not packs:
            return results

        self.log.info("Пачек запросов по кластерам: %d (размер пачки ≤ %d)", len(packs), pack_size)

        missing_cluster_field = False

        def _run_pack(keys: List[Tuple[int, ...]]) -> Tuple[List[Tuple[int, ...]], List[dict], bool]:
            union_ids: List[int] = sorted({cid for key in keys for cid in key})
            resp = self.get_stocks_data(skus, cluster_ids=union_ids, max_workers_override=10)
            items = resp.get("items", [])
            has_field = bool(items) and any("cluster_id" in it for it in items)
            return keys, items, (not has_field)

        with ThreadPoolExecutor(max_workers=self.cluster_workers) as ex:
            futmap = {ex.submit(_run_pack, keys): keys for keys in packs}
            for fut in as_completed(futmap):
                keys, items, pack_missing = fut.result()
                missing_cluster_field = missing_cluster_field or pack_missing
                if pack_missing:
                    continue

                by_cid: Dict[int, List[dict]] = defaultdict(list)
                for it in items:
                    cid = it.get("cluster_id")
                    if cid is None:
                        continue
                    try:
                        by_cid[int(cid)].append(it)
                    except (TypeError, ValueError):
                        continue

                for key in keys:
                    acc: List[dict] = []
                    for cid in key:
                        bucket = by_cid.get(int(cid))
                        if bucket:
                            acc.extend(bucket)
                    results[key] = acc

        if missing_cluster_field:
            self.log.warning("Ответ API не содержит 'cluster_id' в items — переключаемся на поштучные запросы по наборам.")
            return {}

        return results

    # ---- Пакетный *_cluster для CK/CO/CS/CW/DA/DE/DI (с фолбэком)
    def _fetch_single_cluster_blocks_parallel(
        self,
        skus: List[str],
        id_by_cell: Dict[str, Optional[int]]
    ) -> List[Dict[str, Dict[str, Optional[str]]]]:
        ordered_cells = [cell for cell, _ in self.SINGLE_CLUSTER_CELLS_ORDERED]
        tasks = [(cell, id_by_cell.get(cell)) for cell in ordered_cells if id_by_cell.get(cell) is not None]
        if not tasks:
            return [{} for _ in ordered_cells]

        union_ids = sorted({cid for _, cid in tasks})
        self.log.info("Пакетный запрос *_cluster (single-blocks): %s", ", ".join(map(str, union_ids)))
        resp = self.get_stocks_data(skus, cluster_ids=union_ids, max_workers_override=8)
        items = resp.get("items", [])

        has_cluster_id = any("cluster_id" in it for it in items)
        has_cluster_fields = any(
            ("ads_cluster" in it) or
            ("days_without_sales_cluster" in it) or
            ("idc_cluster" in it) or
            ("turnover_grade_cluster" in it)
            for it in items
        )

        if has_cluster_id:
            by_cid: Dict[int, List[dict]] = defaultdict(list)
            for it in items:
                cid = it.get("cluster_id")
                if cid is None:
                    continue
                try:
                    by_cid[int(cid)].append(it)
                except (TypeError, ValueError):
                    continue

            result_maps: Dict[str, Dict[str, Dict[str, Optional[str]]]] = {cell: {} for cell in ordered_cells}
            for cell, cid in tasks:
                bucket = by_cid.get(int(cid), [])
                if not bucket:
                    result_maps[cell] = {}
                else:
                    result_maps[cell] = self._collect_params_from_items(bucket, cluster=has_cluster_fields)
            return [result_maps.get(cell, {}) for cell in ordered_cells]

        # Фолбэк: по одному
        self.log.warning("Пакетный *_cluster не применим (нет cluster_id в ответе) — выполняем по одному запросу на cid.")
        results_by_cell: Dict[str, Dict[str, Dict[str, Optional[str]]]] = {cell: {} for cell in ordered_cells}
        started = perf_counter()
        with ThreadPoolExecutor(max_workers=self.single_cluster_workers) as ex:
            future_map = {
                ex.submit(self.get_stocks_data, skus, [cid], 8): (cell, cid) for cell, cid in tasks
            }
            for fut in as_completed(future_map):
                cell, cid = future_map[fut]
                try:
                    r = fut.result()
                    its = r.get("items", [])
                    hc = any(("ads_cluster" in it) or ("days_without_sales_cluster" in it) or
                             ("idc_cluster" in it) or ("turnover_grade_cluster" in it) for it in its)
                    results_by_cell[cell] = self._collect_params_from_items(its, cluster=hc)
                    self.log.info("cell %s (cid=%s) -> items=%d (cluster_fields=%s)",
                                  cell, cid, len(its), "yes" if hc else "no")
                except Exception as e:
                    self.log.error("Ошибка *_cluster для %s (cid=%s): %s", cell, cid, e)
                    results_by_cell[cell] = {}
        self.log.info("Запросы *_cluster (фолбэк) завершены за %.3fs", perf_counter() - started)
        return [results_by_cell.get(cell, {}) for cell in ordered_cells]

    # ------------------------------- Export rows

    def build_export_rows(
        self,
        skus: List[str],
        items_all: List[dict],
        cluster_items_by_col: Dict[str, List[dict]],
        cluster_params_blocks: Optional[List[Dict[str, Dict[str, Optional[str]]]]] = None,
    ) -> List[List]:
        sums_all = self._sum_all_warehouses(items_all)
        sums_by_col = self._sum_by_cluster_col(cluster_items_by_col)
        params_by_sku = self._collect_params_from_items(items_all, cluster=False)

        export_rows: List[List] = []
        for sku in skus:
            sa = sums_all.get(sku, {})
            p = params_by_sku.get(sku, {})

            row = [
                sa.get("available_stock_count", 0) if sa.get("available_stock_count", 0) != 0 else 0,  # AC
                sa.get("other_stock_count", 0) or "",                                                   # AD
                sa.get("requested_stock_count", 0) or "",                                               # AE
                sa.get("return_from_customer_stock_count", 0) or "",                                    # AF
                sa.get("return_to_seller_stock_count", 0) or "",                                        # AG
                sa.get("stock_defect_stock_count", 0) or "",                                            # AH
                sa.get("transit_defect_stock_count", 0) or "",                                          # AI
                sa.get("transit_stock_count", 0) or "",                                                 # AJ
                sa.get("valid_stock_count", 0) or "",                                                   # AK
                "",                                                                                     # AL
            ]

            for col in self.CLUSTER_COLS:
                v = sums_by_col.get(col, {}).get(sku, 0)
                row.append(v if (v != 0 or self.write_zeroes) else "")

            row.extend([""] * 23)  # BG..CC пусто

            row.extend([
                p.get("ads", "") if p.get("ads") is not None else "",
                p.get("days_without_sales", "") if p.get("days_without_sales") is not None else "",
                p.get("idc", "") if p.get("idc") is not None else "",
                p.get("turnover_grade", "") if p.get("turnover_grade") is not None else "",
            ])

            if cluster_params_blocks:
                for cp_map in cluster_params_blocks:
                    cp = (cp_map or {}).get(sku, {})
                    row.extend([
                        cp.get("ads", "") if cp.get("ads") is not None else "",
                        cp.get("days_without_sales", "") if cp.get("days_without_sales") is not None else "",
                        cp.get("idc", "") if cp.get("idc") is not None else "",
                        cp.get("turnover_grade", "") if cp.get("turnover_grade") is not None else "",
                    ])

            export_rows.append(row)

        self.log.info("Сформировано %d строк (кластерные столбцы: %s)", len(export_rows), ", ".join(self.CLUSTER_COLS))
        return export_rows

    # ------------------------------- Export (2 диапазона, без очистки)

    def export_data_to_sheet(self, data: List[List]):
        ws = self._get_worksheet()
        # Ускорение: не очищаем, сразу перезаписываем 2 блока

        if not data:
            self.log.info("Данных для экспорта нет — пропускаем запись")
            return

        bottom_row = 4 + len(data)
        left_range = f"AC5:BF{bottom_row}"
        left_block = [row[:30] for row in data]

        # Правый блок расширен до DI
        right_range = f"CD5:DI{bottom_row}"
        right_block = [row[53:] for row in data]

        t0 = perf_counter()
        ws.batch_update([
            {"range": left_range, "values": left_block},
            {"range": right_range, "values": right_block},
        ], value_input_option='RAW')
        self.log.info(
            "Экспорт %d строк (двумя диапазонами) за %.3fs: %s и %s",
            len(data), perf_counter() - t0, left_range, right_range
        )

    # ------------------------------- Pipeline

    def process(self) -> bool:
        t0 = perf_counter()
        try:
            self.log.info("1) Получение SKU из таблицы…")
            skus = self.get_sku_list_from_sheet()
            if not skus:
                self.log.warning("Не найдены SKU в таблице")
                return False

            self.log.info("1a) Читаем cluster_ids по колонкам AM..BF одним batch_get…")
            cluster_ids_map = self.collect_cluster_ids_map_batch()

            self.log.info("2) Все склады (без фильтров)…")
            # локально ограничим до 8 — на практике часто быстрее из-за троттлинга
            items_all = self.get_stocks_data(skus, cluster_ids=None, max_workers_override=8).get("items", [])

            # 2a) *_cluster блоки
            cells = [cell for cell, _ in self.SINGLE_CLUSTER_CELLS_ORDERED]
            self.log.info("2a) Чтение одиночных cluster_id одним batch_get: %s", ", ".join(cells))
            id_by_cell = self.get_single_cluster_ids_batch(cells)
            cluster_params_blocks = self._fetch_single_cluster_blocks_parallel(skus, id_by_cell)

            # 2b) AM..BF — пакетирование наборов
            groups = self._dedupe_cluster_sets(cluster_ids_map)
            if self.clusters_pack_size and self.clusters_pack_size > 0:
                group_results = self._fetch_clusters_packed_parallel(skus, groups, self.clusters_pack_size)
                if not group_results and groups:
                    group_results = self._fetch_clusters_parallel(skus, groups)
            else:
                group_results = self._fetch_clusters_parallel(skus, groups)

            cluster_items_by_col: Dict[str, List[dict]] = {col: [] for col in self.CLUSTER_COLS}
            for key, cols in groups.items():
                items = group_results.get(key, [])
                for col in cols:
                    cluster_items_by_col[col] = items

            self.log.info("3) Подготовка строк…")
            export_rows = self.build_export_rows(
                skus=skus,
                items_all=items_all,
                cluster_items_by_col=cluster_items_by_col,
                cluster_params_blocks=cluster_params_blocks
            )

            self.log.info("4) Экспорт в таблицу…")
            self.export_data_to_sheet(export_rows)

            self.log.info("Готово! Всего %.3fs", perf_counter() - t0)
            return True

        except Exception as e:
            self.log.exception("Критическая ошибка: %s", str(e))
            return False


if __name__ == "__main__":
    analyzer = OzonAnalyticsStocks(
        batch_size=100,
        max_workers=12,
        cluster_workers=6,
        single_cluster_workers=5,
        clusters_pack_size=12,   # можно 10/12 — что быстрее по месту
        request_timeout=30,
        retry_total=5,
        retry_backoff=0.5,
        delay_between_batches=0.0,
        write_zeroes=False,
        log_level=logging.INFO,
    )
    ok = analyzer.process()
    if not ok:
        print("Произошли ошибки при выполнении — подробности в логах.")
