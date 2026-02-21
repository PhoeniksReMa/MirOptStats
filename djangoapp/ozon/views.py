from django.contrib.auth.decorators import login_required
from django.shortcuts import get_object_or_404, render

from ozon.models import OzonReport
from shops.models import Shop


@login_required
def report_list_view(request, shop_id):
    shop = get_object_or_404(Shop, id=shop_id)
    reports = shop.ozon_reports.all().order_by("title")
    return render(request, "ozon/report_list.html", {"shop": shop, "reports": reports})


@login_required
def report_detail_view(request, shop_id, report_code):
    shop = get_object_or_404(Shop, id=shop_id)
    report = get_object_or_404(OzonReport, shop=shop, code=report_code)
    columns = list(report.columns.all().order_by("order"))
    rows = list(report.rows.all())
    return render(
        request,
        "ozon/report_detail.html",
        {"shop": shop, "report": report, "columns": columns, "rows": rows},
    )

# Create your views here.
