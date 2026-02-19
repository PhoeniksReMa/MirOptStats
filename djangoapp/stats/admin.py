from django.contrib import admin

from .models import Shop, ShopMembership


@admin.register(Shop)
class ShopAdmin(admin.ModelAdmin):
    list_display = ("id", "name", "owner", "created_at")
    search_fields = ("name", "owner__username", "owner__email")
    list_select_related = ("owner",)


@admin.register(ShopMembership)
class ShopMembershipAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "shop",
        "user",
        "role",
        "is_active",
        "can_view_stats",
        "can_edit_shop",
        "can_manage_staff",
    )
    list_filter = ("role", "is_active", "can_view_stats", "can_edit_shop", "can_manage_staff")
    search_fields = ("shop__name", "user__username", "user__email")
    list_select_related = ("shop", "user", "invited_by")
