from django.urls import path

from .views import add_employee_view, create_shop_view, shop_list_view

app_name = "shops"

urlpatterns = [
    path("", shop_list_view, name="list"),
    path("create/", create_shop_view, name="create"),
    path("<int:shop_id>/add-employee/", add_employee_view, name="add_employee"),
]
