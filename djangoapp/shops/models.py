from django.conf import settings
from django.db import models


class Role(models.TextChoices):
    ADMIN = "admin", "Admin"
    MANAGER = "manager", "Manager"
    EMPLOYEE = "employee", "Employee"


class MarketplaceChoice(models.TextChoices):
    OZON = "ozon", "Ozon"
    WILDBERRIES = "wildberries", "Wildberries"
    YANDEX_MARKET = "yandex_market", "Yandex Market"
    OTHER = "other", "Other"


class Shop(models.Model):
    name = models.CharField(max_length=255)
    owner = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="owned_shops",
    )
    marketplace = models.CharField(
        max_length=40,
        choices=MarketplaceChoice.choices,
        default=MarketplaceChoice.OTHER,
    )
    token = models.CharField(
        max_length=40
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["name"]

    def __str__(self) -> str:
        return f"{self.name} (owner: {self.owner.username})"

    def has_access(self, user) -> bool:
        if not user or not user.is_authenticated:
            return False
        return self.owner_id == user.id or self.memberships.filter(user=user, is_active=True).exists()

    def can_manage_staff(self, user) -> bool:
        if not user or not user.is_authenticated:
            return False
        if self.owner_id == user.id:
            return True
        return self.memberships.filter(
            user=user,
            is_active=True,
            can_manage_staff=True,
        ).exists()


class ShopMembership(models.Model):
    shop = models.ForeignKey(
        Shop,
        on_delete=models.CASCADE,
        related_name="memberships",
    )
    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="shop_memberships",
    )
    role = models.CharField(max_length=20, choices=Role.choices, default=Role.EMPLOYEE)
    can_view_stats = models.BooleanField(default=True)
    can_edit_shop = models.BooleanField(default=False)
    can_manage_staff = models.BooleanField(default=False)
    is_active = models.BooleanField(default=True)
    invited_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        related_name="created_memberships",
        null=True,
        blank=True,
    )
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["shop", "user"],
                name="unique_user_membership_per_shop",
            ),
        ]
        ordering = ["shop", "user"]

    def __str__(self) -> str:
        return f"{self.user.username} in {self.shop.name} ({self.role})"
