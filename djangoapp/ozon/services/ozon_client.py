import time
from typing import Any, Dict, Iterable, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class OzonClient:
    BASE_URL = "https://api-seller.ozon.ru"

    def __init__(self, client_id: str, api_key: str, timeout: int = 30):
        self.client_id = client_id
        self.api_key = api_key
        self.timeout = timeout

        self.session = requests.Session()
        self.session.headers.update({
            "Client-Id": self.client_id,
            "Api-Key": self.api_key,
            "Content-Type": "application/json",
        })

        retry = Retry(
            total=5,
            backoff_factor=0.6,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["POST"],
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def post(self, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.BASE_URL}{path}"
        resp = self.session.post(url, json=payload, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    def list_products(self, limit: int = 1000) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        last_id = ""
        while True:
            payload = {"filter": {}, "last_id": last_id, "limit": limit}
            data = self.post("/v3/product/list", payload)
            result = data.get("result", {})
            batch = result.get("items", []) or []
            items.extend(batch)
            if len(batch) < limit:
                break
            last_id = result.get("last_id", "")
            if not last_id:
                break
        return items

    def product_info_list(self, product_ids: Iterable[int], limit: int = 1000) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        ids = list(product_ids)
        for i in range(0, len(ids), limit):
            chunk = ids[i:i + limit]
            payload = {"product_id": chunk}
            data = self.post("/v3/product/info/list", payload)
            items.extend(data.get("result", {}).get("items", []) or [])
            time.sleep(0.1)
        return items

    def product_stocks(self, product_ids: Iterable[int], limit: int = 1000) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        ids = list(product_ids)
        for i in range(0, len(ids), limit):
            chunk = ids[i:i + limit]
            payload = {"filter": {"product_id": chunk}, "limit": 1000}
            data = self.post("/v4/product/info/stocks", payload)
            items.extend(data.get("items", []) or [])
            time.sleep(0.1)
        return items

    def product_description(self, product_ids: Iterable[int]) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        ids = list(product_ids)
        for pid in ids:
            payload = {"product_id": int(pid)}
            data = self.post("/v1/product/info/description", payload)
            if isinstance(data, dict):
                res = data.get("result") or {}
                if res:
                    res["product_id"] = pid
                    items.append(res)
            time.sleep(0.1)
        return items

    def product_queries(self, skus: Iterable[str], date_from: str, date_to: str, page_size: int = 1000) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        sku_list = list(skus)
        for i in range(0, len(sku_list), 1000):
            batch = sku_list[i:i + 1000]
            payload = {
                "date_from": date_from,
                "date_to": date_to,
                "skus": batch,
                "page_size": page_size,
                "sort_by": "BY_SEARCHES",
                "sort_dir": "DESCENDING",
            }
            data = self.post("/v1/analytics/product-queries", payload)
            items.extend(data.get("items", []) or [])
            time.sleep(0.2)
        return items

    def product_queries_all(self, sku: str, date_from: str, date_to: str, page_size: int = 1000) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        page = 0
        while True:
            payload = {
                "date_from": date_from,
                "date_to": date_to,
                "skus": [sku],
                "page_size": page_size,
                "page": page,
                "sort_by": "BY_SEARCHES",
                "sort_dir": "DESCENDING",
            }
            data = self.post("/v1/analytics/product-queries", payload)
            batch = data.get("queries") or data.get("items") or []
            if not batch:
                break
            items.extend(batch)
            page_count = data.get("page_count")
            if isinstance(page_count, int) and page >= page_count - 1:
                break
            page += 1
            time.sleep(0.2)
        return items

    def description_category_tree(self, language: str = "DEFAULT") -> List[Dict[str, Any]]:
        data = self.post("/v1/description-category/tree", {"language": language})
        return data.get("result") or data.get("categories") or []

    def analytics_stocks(self, skus: List[str], cluster_ids: Optional[List[int]] = None) -> List[Dict[str, Any]]:
        payload = {"skus": skus, "warehouse_ids": [], "limit": len(skus)}
        if cluster_ids:
            payload["cluster_ids"] = cluster_ids
        data = self.post("/v1/analytics/stocks", payload)
        return data.get("items", []) or []

    def cluster_list(self, cluster_type: str = "CLUSTER_TYPE_OZON") -> List[Dict[str, Any]]:
        payload = {"cluster_ids": [], "cluster_type": cluster_type}
        data = self.post("/v1/cluster/list", payload)
        return data.get("clusters", []) or data.get("result", {}).get("clusters", []) or data.get("result", []) or []

    def warehouse_list(self) -> List[Dict[str, Any]]:
        data = self.post("/v2/warehouse/list", {})
        return data.get("result", []) or []

    def supply_order_list(self, states: Optional[List[str]] = None, limit: int = 100) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        last_id = ""
        while True:
            payload: Dict[str, Any] = {
                "filter": {"states": states or []},
                "limit": limit,
                "sort_by": "ORDER_CREATION",
                "sort_dir": "DESC",
            }
            if last_id:
                payload["last_id"] = last_id
            data = self.post("/v3/supply-order/list", payload)
            batch = data.get("order_ids") or data.get("result", {}).get("order_ids") or []
            if not batch:
                break
            items.extend([{"supply_order_id": x} for x in batch])
            new_last_id = data.get("last_id") or data.get("result", {}).get("last_id") or ""
            if not new_last_id or new_last_id == last_id or len(batch) < limit:
                break
            last_id = new_last_id
        return items

    def supply_order_get(self, order_ids: List[int]) -> Dict[str, Any]:
        data = self.post("/v3/supply-order/get", {"order_ids": order_ids})
        return data

    def returns_list(self, date_from: str, date_to: str, limit: int = 500) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        last_id = 0
        while True:
            payload = {
                "filter": {
                    "visual_status_change_moment": {
                        "time_from": date_from,
                        "time_to": date_to,
                    },
                },
                "limit": limit,
                "last_id": last_id,
            }
            data = self.post("/v1/returns/list", payload)
            batch = data.get("result", {}).get("returns", []) or data.get("returns", []) or []
            if not batch:
                break
            items.extend(batch)
            try:
                last_id = int(batch[-1].get("id") or 0)
            except Exception:
                last_id = 0
            if not last_id:
                break
        return items

    def report_create_placement(self, date_from: str, date_to: str) -> str:
        data = self.post("/v1/report/placement/by-products/create", {"date_from": date_from, "date_to": date_to})
        code = data.get("code")
        if not code:
            raise RuntimeError("No report code returned")
        return code

    def report_info(self, code: str) -> Dict[str, Any]:
        data = self.post("/v1/report/info", {"code": code})
        return data.get("result", {}) or {}

    def product_prices(self, product_ids: List[int]) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        for i in range(0, len(product_ids), 1000):
            chunk = product_ids[i:i + 1000]
            payload = {"filter": {"product_id": chunk}, "limit": 1000}
            data = self.post("/v5/product/info/prices", payload)
            items.extend(data.get("items", []) or [])
            time.sleep(0.1)
        return items

    def product_attributes(self, product_ids: List[int], limit: int = 100) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        for i in range(0, len(product_ids), 1000):
            chunk = product_ids[i:i + 1000]
            last_id = None
            while True:
                payload: Dict[str, Any] = {"filter": {"product_id": chunk}, "limit": limit, "sort_dir": "ASC"}
                if last_id:
                    payload["last_id"] = last_id
                data = self.post("/v4/product/info/attributes", payload)
                if isinstance(data, list):
                    batch = data
                    result = {}
                else:
                    result = data.get("result", {}) if isinstance(data, dict) else {}
                    if isinstance(result, list):
                        batch = result
                    else:
                        batch = result.get("items", []) or (data.get("items", []) if isinstance(data, dict) else []) or []
                items.extend(batch)
                last_id = result.get("last_id") if isinstance(result, dict) else None
                if not last_id or not batch:
                    break
                time.sleep(0.1)
        return items
