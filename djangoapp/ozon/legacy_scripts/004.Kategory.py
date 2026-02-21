# ozon_sheet_fill_qrs.py
import json, time
from typing import Any, Dict, List, Optional, Tuple
import requests, gspread
from google.oauth2.service_account import Credentials

API_FILE = "API.txt"
GOOGLE_SA_FILE = "credentials.json"
BASE = "https://api-seller.ozon.ru"

# ------------ helpers ------------
def read_api_file(path=API_FILE):
    with open(path, "r", encoding="utf-8") as f:
        lines = [x.strip() for x in f if x.strip()]
    if len(lines) < 4:
        raise RuntimeError("API.txt: 4 строки — Client-Id, Api-Key, SpreadsheetId, SheetName")
    return lines[0], lines[1], lines[2], lines[3]

def make_headers(client_id, api_key):
    return {"Client-Id": client_id, "Api-Key": api_key, "Content-Type": "application/json"}

def post_json(url, headers, payload, timeout=30):
    r = requests.post(url, headers=headers, json=payload, timeout=timeout)
    r.raise_for_status()
    try:    return r.json()
    except: return json.loads(r.text)

def chunked(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i+n]

# ------------ OZON ------------
def v3_product_info_list(headers, product_ids=None):
    url = f"{BASE}/v3/product/info/list"
    body = {"product_id": [int(x) for x in product_ids]} if product_ids else {}
    data = post_json(url, headers, body)
    return data.get("items") or ((data.get("result") or {}).get("items") or [])

def v1_description_category_tree(headers, language="DEFAULT"):
    url = f"{BASE}/v1/description-category/tree"
    data = post_json(url, headers, {"language": language}, timeout=60)
    return data.get("result") or data.get("categories") or []

# путь от корня до узла с нужным description_category_id (только категорийные узлы)
def find_path_to_desc_cat(roots, desc_cat_id) -> Optional[List[Dict[str, Any]]]:
    def ch(n): return n.get("children", []) or []
    path: List[Dict[str, Any]] = []
    def dfs(n):
        path.append(n)
        if n.get("description_category_id") == desc_cat_id:
            return True
        for c in ch(n):
            if dfs(c): return True
        path.pop()
        return False
    for r in roots:
        if dfs(r):
            return [n for n in path if n.get("description_category_id")]
    return None

# type_name в поддереве категории по type_id
def find_type_name_under(category_node, type_id: Optional[int]) -> Optional[str]:
    if not category_node or not type_id: return None
    def ch(n): return n.get("children", []) or []
    def dfs(n):
        if n.get("type_id") == type_id:
            return n.get("type_name") or n.get("title")
        for c in ch(n):
            r = dfs(c)
            if r: return r
        return None
    return dfs(category_node)

# ------------ Sheets ------------
def open_sheet(spreadsheet_id, sheet_name):
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(GOOGLE_SA_FILE, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(sheet_name)
    return sh, ws

def read_col_L_with_rows(ws):
    values = ws.col_values(12)  # L
    pairs = []
    for idx, raw in enumerate(values, start=1):
        s = str(raw or "").strip()
        if not s: continue
        if s.isdigit():
            pairs.append((idx, int(s)))
        else:
            digits = "".join(ch for ch in s if ch.isdigit())
            if digits:
                try: pairs.append((idx, int(digits)))
                except ValueError: pass
    return pairs

def batch_write_QRS(sh, sheet_name, rows_to_values):
    """
    rows_to_values: (row_idx, q_sub, r_main, s_type)
    Пишем: Q = главная категория, R = подкатегория, S = тип
    """
    if not rows_to_values: return
    data = [{"range": f"{sheet_name}!Q{row}:S{row}", "values": [[r_main, q_sub, s_type]]}
            for (row, q_sub, r_main, s_type) in rows_to_values]
    sh.values_batch_update({"data": data, "valueInputOption": "RAW"})

# ------------ MAIN ------------
def main():
    client_id, api_key, spreadsheet_id, sheet_name = read_api_file()
    H = make_headers(client_id, api_key)
    sh, ws = open_sheet(spreadsheet_id, sheet_name)

    pairs = read_col_L_with_rows(ws)
    if not pairs:
        print("В столбце L нет product_id."); return
    all_pids = [pid for _, pid in pairs]

    # дерево 1 раз
    print("Загружаю дерево описательных категорий...")
    roots = v1_description_category_tree(H)
    print("Дерево получено.")

    # info батчами
    print(f"Запрашиваю /v3/product/info/list для {len(all_pids)} товаров...")
    pid2info: Dict[int, Dict[str, Any]] = {}
    for chunk in chunked(all_pids, 50):
        for it in v3_product_info_list(H, product_ids=chunk):
            pid = it.get("id") or it.get("product_id")
            if pid is None: continue
            pid2info[pid] = {
                "desc_cat_id": it.get("description_category_id"),
                "type_id": it.get("type_id")
            }
        time.sleep(0.1)

    # кэши
    path_cache: Dict[int, Optional[List[Dict[str, Any]]]] = {}
    type_cache: Dict[Tuple[int,int], Optional[str]] = {}

    writes = []
    for row, pid in pairs:
        info = pid2info.get(pid, {})
        desc_cat_id = info.get("desc_cat_id")
        type_id     = info.get("type_id")

        q_subcat = ""   # подкатегория (пойдёт в R)
        r_main   = ""   # главная категория (пойдёт в Q)
        s_type   = ""   # тип (пойдёт в S)

        if desc_cat_id:
            path = path_cache.get(desc_cat_id)
            if path is None:
                path = find_path_to_desc_cat(roots, desc_cat_id)
                path_cache[desc_cat_id] = path

            if path:
                # r_main = верхний category_name, q_subcat = целевая категория (последний в пути)
                r_main = (path[0].get("category_name") or path[0].get("title") or "")
                q_subcat = (path[-1].get("category_name") or path[-1].get("title") or "")
                if len(path) == 1:  # если категория верхнего уровня — подкатегории нет
                    q_subcat = ""

                # s_type = type_name под целевой категорией по type_id
                if type_id:
                    key = (int(desc_cat_id), int(type_id))
                    s_type = type_cache.get(key) or ""
                    if not s_type:
                        cat_node = path[-1]
                        tn = find_type_name_under(cat_node, int(type_id))
                        if tn:
                            s_type = tn
                            type_cache[key] = tn

        writes.append((row, q_subcat, r_main, s_type))

    print(f"Записываю Q(главная категория) / R(подкатегория) / S(тип) для {len(writes)} строк...")
    batch_write_QRS(sh, sheet_name, writes)
    print("Готово ✅")

if __name__ == "__main__":
    main()
