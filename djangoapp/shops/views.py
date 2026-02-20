from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.shortcuts import get_object_or_404, redirect, render

from .forms import AddEmployeeForm, ShopCreateForm
from .models import Shop


@login_required
def shop_list_view(request):
    shops = Shop.objects.filter(owner=request.user)
    memberships = request.user.shop_memberships.select_related("shop").filter(is_active=True)
    context = {
        "owned_shops": shops,
        "member_shops": [membership.shop for membership in memberships],
    }
    return render(request, "shops/shop_list.html", context)


@login_required
def create_shop_view(request):
    if request.method == "POST":
        form = ShopCreateForm(request.POST)
        if form.is_valid():
            shop = form.save(commit=False)
            shop.owner = request.user
            shop.save()
            messages.success(request, "Shop created")
            return redirect("shops:list")
    else:
        form = ShopCreateForm()

    return render(request, "shops/create_shop.html", {"form": form})


@login_required
def add_employee_view(request, shop_id):
    shop = get_object_or_404(Shop, id=shop_id)
    if not shop.can_manage_staff(request.user):
        messages.error(request, "You don't have permission to manage staff in this shop")
        return redirect("shops:list")

    if request.method == "POST":
        form = AddEmployeeForm(request.POST, shop=shop)
        if form.is_valid():
            form.save(invited_by=request.user)
            messages.success(request, "Employee added or updated")
            return redirect("shops:list")
    else:
        form = AddEmployeeForm(shop=shop)

    memberships = shop.memberships.select_related("user").filter(is_active=True)
    return render(
        request,
        "shops/add_employee.html",
        {
            "form": form,
            "shop": shop,
            "memberships": memberships,
        },
    )
