from django import forms
from django.contrib.auth import get_user_model

from .models import Role, Shop, ShopMembership

User = get_user_model()


class ShopCreateForm(forms.ModelForm):
    class Meta:
        model = Shop
        fields = ("name", "marketplace", "token")
        labels = {
            "name": "Название магазина",
            "marketplace": "Маркетплейс",
            "token": "Токен",
        }


class AddEmployeeForm(forms.Form):
    username = forms.CharField(
        max_length=150,
        label="Логин пользователя",
        help_text="Существующий логин пользователя",
    )
    role = forms.ChoiceField(choices=Role.choices, initial=Role.EMPLOYEE, label="Роль")
    can_view_stats = forms.BooleanField(required=False, initial=True, label="Доступ к статистике")
    can_edit_shop = forms.BooleanField(required=False, initial=False, label="Право редактировать магазин")
    can_manage_staff = forms.BooleanField(required=False, initial=False, label="Право управлять персоналом")

    def __init__(self, *args, **kwargs):
        self.shop = kwargs.pop("shop")
        super().__init__(*args, **kwargs)

    def clean_username(self):
        username = self.cleaned_data["username"].strip()
        try:
            user = User.objects.get(username=username)
        except User.DoesNotExist as exc:
            raise forms.ValidationError("Пользователь с таким логином не найден") from exc

        if self.shop.owner_id == user.id:
            raise forms.ValidationError("Владелец уже закреплен за этим магазином")
        return username

    def save(self, invited_by):
        user = User.objects.get(username=self.cleaned_data["username"])
        membership, _ = ShopMembership.objects.update_or_create(
            shop=self.shop,
            user=user,
            defaults={
                "role": self.cleaned_data["role"],
                "can_view_stats": self.cleaned_data["can_view_stats"],
                "can_edit_shop": self.cleaned_data["can_edit_shop"],
                "can_manage_staff": self.cleaned_data["can_manage_staff"],
                "is_active": True,
                "invited_by": invited_by,
            },
        )
        return membership
