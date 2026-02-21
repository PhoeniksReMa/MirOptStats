from django.conf import settings
from django.db import models


class Role(models.TextChoices):
    ADMIN = "admin", "Администратор"
    MANAGER = "manager", "Менеджер"
    EMPLOYEE = "employee", "Сотрудник"


class MarketplaceChoice(models.TextChoices):
    OZON = "ozon", "Ozon"
    WILDBERRIES = "wildberries", "Wildberries"
    YANDEX_MARKET = "yandex_market", "Яндекс Маркет"
    OTHER = "other", "Другое"


class Shop(models.Model):
    name = models.CharField(max_length=255, verbose_name="Название магазина")
    owner = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="owned_shops",
        verbose_name="Владелец",
    )
    marketplace = models.CharField(
        max_length=40,
        choices=MarketplaceChoice.choices,
        default=MarketplaceChoice.OZON,
        verbose_name="Маркетплейс",
    )
    token = models.CharField(
        max_length=40,
        verbose_name="Токен",
    )
    client_id = models.CharField(
        max_length=32,
        verbose_name="Client-Id",
        blank=True,
        default="",
    )
    created_at = models.DateTimeField(auto_now_add=True, verbose_name="Создано")
    updated_at = models.DateTimeField(auto_now=True, verbose_name="Обновлено")

    class Meta:
        ordering = ["name"]
        verbose_name = "магазин"
        verbose_name_plural = "магазины"

    def __str__(self) -> str:
        return f"{self.name} (owner: {self.owner.email})"

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
        verbose_name="Магазин",
    )
    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="shop_memberships",
        verbose_name="Пользователь",
    )
    role = models.CharField(
        max_length=20,
        choices=Role.choices,
        default=Role.EMPLOYEE,
        verbose_name="Роль",
    )
    can_view_stats = models.BooleanField(default=True, verbose_name="Доступ к статистике")
    can_edit_shop = models.BooleanField(default=False, verbose_name="Право редактировать магазин")
    can_manage_staff = models.BooleanField(default=False, verbose_name="Право управлять персоналом")
    is_active = models.BooleanField(default=True, verbose_name="Активен")
    invited_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        related_name="created_memberships",
        null=True,
        blank=True,
        verbose_name="Кто добавил",
    )
    created_at = models.DateTimeField(auto_now_add=True, verbose_name="Добавлен")

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["shop", "user"],
                name="unique_user_membership_per_shop",
            ),
        ]
        ordering = ["shop", "user"]
        verbose_name = "сотрудник магазина"
        verbose_name_plural = "сотрудники магазинов"

    def __str__(self) -> str:
        return f"{self.user.email} in {self.shop.name} ({self.role})"
