from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple

from ozon.services.ozon_client import OzonClient
from ozon.services.reporting import ensure_columns, get_or_create_report, upsert_rows
from shops.models import Shop


def _col_to_index(col: str) -> int:
    col = (col or "").strip().upper()
    n = 0
    for ch in col:
        if not ("A" <= ch <= "Z"):
            raise ValueError(f"Bad column: {col!r}")
        n = n * 26 + (ord(ch) - ord("A") + 1)
    return n - 1


def _col_order(col: str) -> int:
    return (_col_to_index(col) + 1) * 10


def _col_range(start: str, end: str) -> List[str]:
    start_idx = _col_to_index(start)
    end_idx = _col_to_index(end)
    return [_index_to_col(i) for i in range(start_idx, end_idx + 1)]


def _index_to_col(idx: int) -> str:
    if idx < 0:
        raise ValueError("Index must be >= 0")
    s = ""
    n = idx + 1
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(ord("A") + r) + s
    return s


def _monitor_columns() -> List[Tuple[str, str, int, str]]:
    columns: List[Tuple[str, str, int, str]] = []
    base_columns: Dict[str, Tuple[str, str]] = {
        "A": ("Картинка", "text"),
        "B": ("Артикул", "text"),
        "C": ("Наименование", "text"),
        "D": ("Super", "text"),
        "E": ("Контент-рейтинг", "number"),
        "K": ("SKU", "text"),
        "L": ("Product ID", "number"),
        "M": ("Штрихкод", "text"),
        "N": ("Дата создания", "date"),
        "O": ("Дата обновл.", "date"),
        "P": ("НДС", "text"),
        "Q": ("Категория", "text"),
        "R": ("Подкатегория", "text"),
        "S": ("Тип", "text"),
        "U": ("Длина (мм)", "number"),
        "V": ("Ширина (мм)", "number"),
        "W": ("Высота (мм)", "number"),
        "X": ("Вес (г.)", "number"),
        "Y": ("Объем вес", "number"),
        "Z": ("Объем (л)", "number"),
        "AA": ("Ошибка объема", "text"),
        "BL": ("FBS в наличии", "number"),
        "BM": ("Резерв общий", "number"),
        "BN": ("Резерв FBO", "number"),
        "BO": ("Резерв FBS", "number"),
        "DK": ("Хранение: стоимость", "number"),
        "DL": ("Хранение: платные шт.", "number"),
        "DM": ("Хранение: прогноз 28д", "number"),
        "DN": ("Хранение: складов", "number"),
        "IT": ("Поиск: пользователи", "number"),
        "IU": ("Поиск: позиция", "number"),
        "IV": ("Просмотры: пользователи", "number"),
        "IW": ("Конверсия просмотров", "number"),
        "IX": ("GMV 28д", "number"),
        "IZ": ("Ключевые (строки)", "text"),
        "JA": ("Позиции", "text"),
        "JB": ("Конверсия", "text"),
        "JC": ("Заказы", "text"),
        "JD": ("Ключевые (;)", "text"),
        "JE": ("Описание", "text"),
        "JF": ("SEO +", "text"),
        "JG": ("Дата обновл. (дубль)", "date"),
        "HH": ("FBO прошлый год (1)", "number"),
        "HI": ("FBS прошлый год (1)", "number"),
        "HJ": ("FBO прошлый год (2)", "number"),
        "HK": ("FBS прошлый год (2)", "number"),
        "HL": ("FBO прошлый год (3)", "number"),
        "HM": ("FBS прошлый год (3)", "number"),
        "HN": ("FBS прошлый год (сумма)", "number"),
    }
    for col, (label, dtype) in base_columns.items():
        columns.append((col, label, _col_order(col), dtype))

    number_blocks = set(_col_range("AC", "AK") + _col_range("AM", "BF") + ["BP", "CD", "CE", "CF"]
                        + _col_range("CH", "CJ") + _col_range("CL", "CN") + _col_range("CP", "CR")
                        + _col_range("CT", "CV") + _col_range("CX", "CZ") + _col_range("DB", "DD")
                        + _col_range("DF", "DH"))
    text_overrides = {"CG", "CK", "CO", "CS", "CW", "DA", "DE", "DI"}

    for col in _col_range("AC", "DI"):
        if col in base_columns:
            continue
        dtype = "number" if col in number_blocks else "text"
        if col in text_overrides:
            dtype = "text"
        columns.append((col, col, _col_order(col), dtype))

    for col in _col_range("DS", "ET"):
        columns.append((col, col, _col_order(col), "number"))
    for col in ["FY", "GA", "GC", "GE", "GG"]:
        columns.append((col, col, _col_order(col), "number"))
    for col in ["GO", "GP", "GQ"]:
        columns.append((col, col, _col_order(col), "number"))

    for col in _col_range("EV", "FW"):
        columns.append((col, col, _col_order(col), "number"))
    for col in ["FZ", "GB", "GD", "GF", "GH"]:
        columns.append((col, col, _col_order(col), "number"))
    for col in ["GR", "GS", "GT"]:
        columns.append((col, col, _col_order(col), "number"))
    for col in ["GI", "GJ", "GK", "GL", "GM"]:
        columns.append((col, col, _col_order(col), "number"))
    for col in ["GU", "GV", "GW"]:
        columns.append((col, col, _col_order(col), "number"))

    for col in _col_range("HP", "IM"):
        columns.append((col, col, _col_order(col), "text"))

    return columns


def _monitor_report(shop: Shop):
    return get_or_create_report(shop, "monitor", "Монитор", "Главная таблица Ozon")


def _date_range_28d() -> Tuple[str, str]:
    today_utc_date = datetime.now(timezone.utc).date()
    start_date = today_utc_date - timedelta(days=30)
    end_date = today_utc_date - timedelta(days=3)
    date_from = datetime.combine(start_date, datetime.min.time(), tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
    date_to = datetime.combine(end_date, datetime.max.time(), tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
    return date_from, date_to


def sync_monitor(shop: Shop) -> None:
    if not shop.client_id or not shop.token:
        return
    report = _monitor_report(shop)
    ensure_columns(report, _monitor_columns())

    client = OzonClient(client_id=shop.client_id, api_key=shop.token)

    # 1) Products list -> offer_id, product_id
    products = client.list_products()
    rows: Dict[str, Dict] = {}
    for p in products:
        offer_id = str(p.get("offer_id") or "").strip()
        product_id = p.get("product_id")
        if not offer_id and not product_id:
            continue
        key = offer_id or str(product_id)
        rows[key] = {
            "B": offer_id,
            "L": product_id,
            "sort_key": offer_id,
        }
    upsert_rows(report, rows)

    # build indexes
    report_rows = {r.row_key: r for r in report.rows.all()}
    product_id_to_key: Dict[int, str] = {}
    sku_to_key: Dict[str, str] = {}
    for r in report_rows.values():
        data = r.data
        if data.get("L"):
            try:
                product_id_to_key[int(data["L"])] = r.row_key
            except Exception:
                pass
        if data.get("K"):
            sku_to_key[str(data["K"]).strip()] = r.row_key

    # 2) Product info -> sku, name, dates, vat, volume_weight
    product_ids = list(product_id_to_key.keys())
    if product_ids:
        info_items = client.product_info_list(product_ids)
        info_rows: Dict[str, Dict] = {}
        for it in info_items:
            pid = it.get("id") or it.get("product_id")
            if not pid or pid not in product_id_to_key:
                continue
            key = product_id_to_key[pid]
            barcodes = it.get("barcodes") or []
            barcode = ""
            if isinstance(barcodes, list) and barcodes:
                barcode = barcodes[0]
            primary_image = it.get("primary_image") or ""
            if isinstance(primary_image, list) and primary_image:
                primary_image = primary_image[0]
            info_rows[key] = {
                "K": it.get("sku"),
                "C": it.get("name"),
                "N": it.get("created_at"),
                "O": it.get("updated_at"),
                "P": it.get("vat"),
                "Y": it.get("volume_weight"),
                "D": "✔️" if it.get("is_super") is True else "❌" if it.get("is_super") is False else "",
                "M": barcode,
                "A": primary_image or "",
                "JG": it.get("updated_at"),
            }
        upsert_rows(report, info_rows)

    # refresh sku index
    report_rows = {r.row_key: r for r in report.rows.all()}
    sku_to_key = {}
    for r in report_rows.values():
        if r.data.get("K"):
            sku_to_key[str(r.data["K"]).strip()] = r.row_key

    # 3) Content rating by SKU
    skus = list(sku_to_key.keys())
    if skus:
        ratings_payload = {"sku": [int(s) for s in skus if str(s).isdigit()]}
        try:
            data = client.post("/v1/product/rating-by-sku", ratings_payload)
            items = data.get("result", []) or []
        except Exception:
            items = []
        rating_rows: Dict[str, Dict] = {}
        for it in items:
            sku = str(it.get("sku") or "").strip()
            rating = it.get("rating")
            if sku and sku in sku_to_key:
                rating_rows[sku_to_key[sku]] = {"E": rating}
        upsert_rows(report, rating_rows)

    # 4) Categories from description tree
    if product_ids:
        info_items = client.product_info_list(product_ids)
        tree = client.description_category_tree()

        def find_path_to_desc_cat(roots, desc_cat_id):
            path = []
            def ch(n): return n.get("children", []) or []
            def dfs(n):
                path.append(n)
                if n.get("description_category_id") == desc_cat_id:
                    return True
                for c in ch(n):
                    if dfs(c):
                        return True
                path.pop()
                return False
            for r in roots:
                if dfs(r):
                    return [n for n in path if n.get("description_category_id")]
            return None

        def find_type_name_under(category_node, type_id):
            if not category_node or not type_id:
                return None
            def ch(n): return n.get("children", []) or []
            def dfs(n):
                if n.get("type_id") == type_id:
                    return n.get("type_name") or n.get("title")
                for c in ch(n):
                    r = dfs(c)
                    if r:
                        return r
                return None
            return dfs(category_node)

        cat_rows: Dict[str, Dict] = {}
        path_cache: Dict[int, List[Dict]] = {}
        type_cache: Dict[Tuple[int, int], str] = {}
        for it in info_items:
            pid = it.get("id") or it.get("product_id")
            if not pid or pid not in product_id_to_key:
                continue
            desc_cat_id = it.get("description_category_id")
            type_id = it.get("type_id")
            main = ""
            sub = ""
            type_name = ""
            if desc_cat_id:
                path = path_cache.get(desc_cat_id)
                if path is None:
                    path = find_path_to_desc_cat(tree, desc_cat_id)
                    path_cache[desc_cat_id] = path or []
                if path:
                    main = (path[0].get("category_name") or path[0].get("title") or "")
                    sub = (path[-1].get("category_name") or path[-1].get("title") or "")
                    if len(path) == 1:
                        sub = ""
                    if type_id:
                        key = (int(desc_cat_id), int(type_id))
                        if key in type_cache:
                            type_name = type_cache[key]
                        else:
                            tn = find_type_name_under(path[-1], int(type_id))
                            if tn:
                                type_name = tn
                                type_cache[key] = tn
            cat_rows[product_id_to_key[pid]] = {
                "Q": main,
                "R": sub,
                "S": type_name,
            }
        upsert_rows(report, cat_rows)

    # 5) Dimensions (attributes)
    if product_ids:
        dim_rows: Dict[str, Dict] = {}
        items = client.product_attributes(product_ids)
        for it in items:
            pid = it.get("id") or it.get("product_id")
            if not pid or pid not in product_id_to_key:
                continue
            dim_unit = it.get("dimension_unit")
            depth = it.get("depth")
            width = it.get("width")
            height = it.get("height")
            weight = it.get("weight")
            mul = 10.0 if dim_unit == "cm" else 1.0
            dim_rows[product_id_to_key[pid]] = {
                "U": round(depth * mul, 2) if depth else None,
                "V": round(width * mul, 2) if width else None,
                "W": round(height * mul, 2) if height else None,
                "X": weight,
            }
        upsert_rows(report, dim_rows)

    # 6) Stocks
    if product_ids:
        stock_items = client.product_stocks(product_ids)
        stock_rows: Dict[str, Dict] = {}
        for it in stock_items:
            pid = it.get("product_id")
            if not pid or pid not in product_id_to_key:
                continue
            stocks = it.get("stocks", []) or []
            fbs_present = 0
            total_reserved = 0
            reserved_fbo = 0
            reserved_fbs = 0
            for s in stocks:
                t = s.get("type")
                present = int(s.get("present") or 0)
                reserved = int(s.get("reserved") or 0)
                if t == "fbs":
                    fbs_present += present
                    reserved_fbs += reserved
                elif t == "fbo":
                    reserved_fbo += reserved
                total_reserved += reserved
            stock_rows[product_id_to_key[pid]] = {
                "BL": fbs_present,
                "BM": total_reserved,
                "BN": reserved_fbo,
                "BO": reserved_fbs,
            }
        upsert_rows(report, stock_rows)

    # 7) Analytics base (product-queries 28d)
    if skus:
        date_from, date_to = _date_range_28d()
        query_items = client.product_queries(skus, date_from, date_to)
        sku_data = {str(it.get("sku", "")).strip(): it for it in query_items if it.get("sku")}
        analytics_rows: Dict[str, Dict] = {}
        for sku, key in sku_to_key.items():
            it = sku_data.get(sku)
            if not it:
                continue
            analytics_rows[key] = {
                "IT": it.get("unique_search_users", 0),
                "IU": it.get("position", 0),
                "IV": it.get("unique_view_users", 0),
                "IW": it.get("view_conversion", 0),
                "IX": it.get("gmv", 0),
            }
        upsert_rows(report, analytics_rows)

    # 8) Analytics keywords (IZ:JD)
    if skus:
        date_from, date_to = _date_range_28d()
        keywords_rows: Dict[str, Dict] = {}
        for sku in skus:
            queries = client.product_queries_all(sku, date_from, date_to)
            if not queries:
                continue
            queries.sort(
                key=lambda x: (x.get("order_count", 0) or 0, x.get("gmv", 0) or 0, x.get("view_conversion", 0) or 0),
                reverse=True,
            )
            col_q, col_pos, col_conv, col_ord = [], [], [], []
            for r in queries:
                q = str(r.get("query") or "").replace("\n", " ").strip()
                pos = r.get("position")
                conv = r.get("view_conversion")
                conv_str = f"{float(conv):.2f}%" if isinstance(conv, (int, float)) else ""
                orders = r.get("order_count") or 0
                col_q.append(q)
                col_pos.append("" if pos is None else str(pos))
                col_conv.append(conv_str)
                col_ord.append(str(orders))
            keywords_rows[sku_to_key[sku]] = {
                "IZ": "\n".join([q for q in col_q if q]),
                "JA": "\n".join(col_pos),
                "JB": "\n".join(col_conv),
                "JC": "\n".join(col_ord),
                "JD": "; ".join([q for q in col_q if q]),
            }
        upsert_rows(report, keywords_rows)

    # 9) Descriptions
    if product_ids:
        desc_items = client.product_description(product_ids)
        desc_rows: Dict[str, Dict] = {}
        for it in desc_items:
            pid = it.get("product_id")
            if not pid or pid not in product_id_to_key:
                continue
            desc = it.get("description") or ""
            desc_rows[product_id_to_key[pid]] = {
                "JE": desc,
                "JF": desc,
            }
        upsert_rows(report, desc_rows)

    # 10) Wrong volume -> flag + liters
    if product_ids:
        try:
            data = client.post("/v1/product/info/wrong-volume", {"cursor": "", "limit": 1000})
            wrong_products = data.get("products", []) or []
        except Exception:
            wrong_products = []
        wrong_ids = {str(p.get("product_id")) for p in wrong_products if p.get("product_id")}
        if wrong_ids:
            report_rows = {r.row_key: r for r in report.rows.all()}
            vol_rows: Dict[str, Dict] = {}
            for r in report_rows.values():
                pid = r.data.get("product_id")
                if pid is None:
                    continue
                key = str(pid)
                if key not in wrong_ids:
                    continue
                length_mm = r.data.get("length_mm")
                width_mm = r.data.get("width_mm")
                height_mm = r.data.get("height_mm")
                liters = None
                try:
                    if length_mm and width_mm and height_mm:
                        liters = round((float(length_mm) * float(width_mm) * float(height_mm)) / 1_000_000.0, 2)
                except Exception:
                    liters = None
                vol_rows[r.row_key] = {
                    "AA": "⚠️",
                    "Z": liters,
                }
            upsert_rows(report, vol_rows)


def merge_monitor_reports(shop: Shop) -> None:
    report = _monitor_report(shop)
    ensure_columns(report, _monitor_columns())

    report_rows = {r.row_key: r for r in report.rows.all()}
    sku_to_key: Dict[str, str] = {}
    offer_to_key: Dict[str, str] = {}
    product_id_to_key: Dict[int, str] = {}
    for r in report_rows.values():
        data = r.data
        if data.get("K"):
            sku_to_key[str(data["K"]).strip()] = r.row_key
        if data.get("B"):
            offer_to_key[str(data["B"]).strip()] = r.row_key
        if data.get("L"):
            try:
                product_id_to_key[int(data["L"])] = r.row_key
            except Exception:
                pass

    def _merge_row(row_key: str, data: Dict):
        if row_key not in report_rows:
            report_rows[row_key] = None
        merged.setdefault(row_key, {}).update(data)

    merged: Dict[str, Dict] = {}

    def _merge_report_by_sku(code: str, keys: List[str]):
        rep = shop.ozon_reports.filter(code=code).first()
        if not rep:
            return
        for r in rep.rows.all():
            sku = str(r.data.get("sku") or r.data.get("K") or "").strip()
            if not sku:
                continue
            key = sku_to_key.get(sku)
            if not key:
                continue
            payload = {k: r.data.get(k, "") for k in keys if k in r.data}
            if payload:
                _merge_row(key, payload)

    def _merge_report_by_offer(code: str, keys: List[str], meta_prefix: str):
        rep = shop.ozon_reports.filter(code=code).first()
        if not rep:
            return
        for r in rep.rows.all():
            offer_id = str(r.data.get("offer_id") or r.data.get("B") or "").strip()
            if r.row_key.startswith("__"):
                meta_key = f"{meta_prefix}{r.row_key}"
                payload = {k: r.data.get(k, "") for k in keys if k in r.data}
                payload.setdefault("B", offer_id or r.data.get("offer_id") or "Итого/периоды")
                if "sort_key" in r.data:
                    payload["sort_key"] = r.data["sort_key"]
                _merge_row(meta_key, payload)
                continue
            if not offer_id:
                continue
            key = offer_to_key.get(offer_id)
            if not key:
                continue
            payload = {k: r.data.get(k, "") for k in keys if k in r.data}
            if payload:
                _merge_row(key, payload)

    def _merge_report_by_product(code: str, keys: List[str]):
        rep = shop.ozon_reports.filter(code=code).first()
        if not rep:
            return
        for r in rep.rows.all():
            pid = r.data.get("product_id") or r.data.get("L")
            if pid is None:
                continue
            try:
                pid_int = int(pid)
            except Exception:
                continue
            key = product_id_to_key.get(pid_int)
            if not key:
                continue
            payload = {k: r.data.get(k, "") for k in keys if k in r.data}
            if payload:
                _merge_row(key, payload)

    # AC..DI stocks analytics
    _merge_report_by_sku("stocks_analytics_full", _col_range("AC", "DI"))

    # Supply statuses BW..CB + BP
    _merge_report_by_sku("supply_statuses_full", ["BP", "BW", "BX", "BY", "BZ", "CA", "CB"])

    # Storage DK..DN
    rep_storage = shop.ozon_reports.filter(code="storage").first()
    if rep_storage:
        for r in rep_storage.rows.all():
            sku = str(r.data.get("sku") or "").strip()
            if not sku:
                continue
            key = sku_to_key.get(sku)
            if not key:
                continue
            payload = {
                "DK": r.data.get("cost_total", ""),
                "DL": r.data.get("qty_paid", ""),
                "DM": r.data.get("forecast_28", ""),
                "DN": r.data.get("warehouses_count", ""),
            }
            _merge_row(key, payload)

    # Price & logistics HP..IM
    _merge_report_by_product("price_logistics", _col_range("HP", "IM"))

    # Orders FBO / FBS matrices
    _merge_report_by_offer("orders_fbo_matrix", _col_range("DS", "ET") + ["FY", "GA", "GC", "GE", "GG", "GO", "GP", "GQ"], "__fbo__")
    _merge_report_by_offer("orders_fbs_matrix", _col_range("EV", "FW") + ["FZ", "GB", "GD", "GF", "GH", "GR", "GS", "GT", "GI", "GJ", "GK", "GL", "GM", "GU", "GV", "GW"], "__fbs__")

    # Old year
    _merge_report_by_offer("orders_fbo_old_year", ["HH", "HJ", "HL"], "__fbo_old__")
    _merge_report_by_offer("orders_fbs_old_year", ["HI", "HK", "HM", "HN"], "__fbs_old__")

    if merged:
        upsert_rows(report, merged)
