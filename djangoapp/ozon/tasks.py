from celery import shared_task

from ozon.services.monitor_sync import merge_monitor_reports, sync_monitor
from ozon.services.orders_sync import (
    sync_orders_fbo,
    sync_orders_fbs,
    sync_orders_fbo_agg,
    sync_orders_fbs_agg,
    sync_orders_fbo_matrix,
    sync_orders_fbs_matrix,
)
from ozon.services.extra_sync import (
    sync_clusters,
    sync_returns,
    sync_storage,
    sync_price_logistics,
    sync_fbo_dynamic,
    sync_orders_fbs_list,
    sync_stocks_analytics,
    sync_supplies_fbo,
    sync_supply_statuses,
    sync_stocks_by_cluster,
    sync_stocks_analytics_full,
    sync_supply_statuses_full,
)
from ozon.services.old_year_sync import sync_orders_fbo_old_year, sync_orders_fbs_old_year
from shops.models import Shop


@shared_task
def sync_all_shops() -> None:
    for shop in Shop.objects.exclude(client_id="").exclude(token=""):
        sync_shop.delay(shop.id)


@shared_task
def sync_shop(shop_id: int) -> None:
    shop = Shop.objects.get(id=shop_id)
    if not shop.client_id or not shop.token:
        return
    sync_monitor(shop)
    sync_orders_fbo(shop)
    sync_orders_fbs(shop)
    sync_orders_fbo_agg(shop)
    sync_orders_fbs_agg(shop)
    sync_orders_fbo_matrix(shop)
    sync_orders_fbs_matrix(shop)
    sync_orders_fbs_list(shop)
    sync_clusters(shop)
    sync_returns(shop)
    sync_storage(shop)
    sync_price_logistics(shop)
    sync_fbo_dynamic(shop)
    sync_stocks_analytics(shop)
    sync_supplies_fbo(shop)
    sync_supply_statuses(shop)
    sync_stocks_by_cluster(shop)
    sync_stocks_analytics_full(shop)
    sync_supply_statuses_full(shop)
    sync_orders_fbo_old_year(shop)
    sync_orders_fbs_old_year(shop)
    merge_monitor_reports(shop)
