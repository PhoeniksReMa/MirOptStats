from django.db import models

from shops.models import Shop


class OzonReport(models.Model):
    shop = models.ForeignKey(Shop, on_delete=models.CASCADE, related_name="ozon_reports")
    code = models.CharField(max_length=64)
    title = models.CharField(max_length=255)
    description = models.TextField(blank=True, default="")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "отчет Ozon"
        verbose_name_plural = "отчеты Ozon"
        unique_together = ("shop", "code")

    def __str__(self) -> str:
        return self.title


class OzonReportColumn(models.Model):
    report = models.ForeignKey(OzonReport, on_delete=models.CASCADE, related_name="columns")
    key = models.CharField(max_length=128)
    label = models.CharField(max_length=255)
    order = models.PositiveIntegerField(default=0)
    data_type = models.CharField(max_length=32, default="text")

    class Meta:
        verbose_name = "колонка отчета Ozon"
        verbose_name_plural = "колонки отчета Ozon"
        unique_together = ("report", "key")
        ordering = ["order", "id"]

    def __str__(self) -> str:
        return f"{self.report.code}:{self.key}"


class OzonReportRow(models.Model):
    report = models.ForeignKey(OzonReport, on_delete=models.CASCADE, related_name="rows")
    row_key = models.CharField(max_length=128)
    sort_key = models.CharField(max_length=128, blank=True, default="")
    data = models.JSONField(default=dict)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "строка отчета Ozon"
        verbose_name_plural = "строки отчета Ozon"
        unique_together = ("report", "row_key")
        ordering = ["sort_key", "row_key"]

    def __str__(self) -> str:
        return f"{self.report.code}:{self.row_key}"


class OzonSyncLog(models.Model):
    report = models.ForeignKey(OzonReport, on_delete=models.CASCADE, related_name="sync_logs")
    status = models.CharField(max_length=32, default="ok")
    message = models.TextField(blank=True, default="")
    started_at = models.DateTimeField(auto_now_add=True)
    finished_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "лог синхронизации Ozon"
        verbose_name_plural = "логи синхронизации Ozon"


class OzonClusterSlot(models.Model):
    shop = models.ForeignKey(Shop, on_delete=models.CASCADE, related_name="ozon_cluster_slots")
    code = models.CharField(max_length=8)
    cluster_id = models.IntegerField()

    class Meta:
        verbose_name = "слот кластера Ozon"
        verbose_name_plural = "слоты кластеров Ozon"
        unique_together = ("shop", "code")

    def __str__(self) -> str:
        return f"{self.shop_id}:{self.code}={self.cluster_id}"
