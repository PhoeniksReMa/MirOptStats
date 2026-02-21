from django.urls import path

from .views import report_detail_view, report_list_view

app_name = "ozon"

urlpatterns = [
    path("<int:shop_id>/", report_list_view, name="report_list"),
    path("<int:shop_id>/<str:report_code>/", report_detail_view, name="report_detail"),
]
