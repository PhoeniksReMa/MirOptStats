from django.db.models.signals import post_save
from django.dispatch import receiver

from ozon.tasks import sync_shop
from .models import Shop


@receiver(post_save, sender=Shop)
def run_ozon_sync_on_create(sender, instance: Shop, created: bool, **kwargs):
    if not created:
        return
    if not instance.client_id or not instance.token:
        return
    sync_shop.delay(instance.id)
