from django.apps import AppConfig


class ShopsConfig(AppConfig):
    name = 'shops'

    def ready(self):
        from . import signals  # noqa: F401
