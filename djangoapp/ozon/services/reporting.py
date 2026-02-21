from typing import Dict, Iterable, List, Tuple

from ozon.models import OzonReport, OzonReportColumn, OzonReportRow
from shops.models import Shop


def get_or_create_report(shop: Shop, code: str, title: str, description: str = "") -> OzonReport:
    report, _ = OzonReport.objects.get_or_create(
        shop=shop,
        code=code,
        defaults={"title": title, "description": description},
    )
    if report.title != title or report.description != description:
        report.title = title
        report.description = description
        report.save(update_fields=["title", "description"])
    return report


def ensure_columns(report: OzonReport, columns: List[Tuple[str, str, int, str]]) -> None:
    existing = {c.key: c for c in report.columns.all()}
    for key, label, order, data_type in columns:
        if key in existing:
            col = existing[key]
            updates = []
            if col.label != label:
                col.label = label
                updates.append("label")
            if col.order != order:
                col.order = order
                updates.append("order")
            if col.data_type != data_type:
                col.data_type = data_type
                updates.append("data_type")
            if updates:
                col.save(update_fields=updates)
        else:
            OzonReportColumn.objects.create(
                report=report,
                key=key,
                label=label,
                order=order,
                data_type=data_type,
            )


def upsert_rows(report: OzonReport, rows: Dict[str, Dict]) -> None:
    existing = {r.row_key: r for r in report.rows.all()}
    to_create = []
    to_update = []
    for row_key, data in rows.items():
        if row_key in existing:
            row = existing[row_key]
            row.data = {**row.data, **data}
            if "sort_key" in data:
                row.sort_key = data["sort_key"] or row.sort_key
            to_update.append(row)
        else:
            sort_key = data.get("sort_key", "")
            to_create.append(OzonReportRow(report=report, row_key=row_key, sort_key=sort_key, data=data))
    if to_create:
        OzonReportRow.objects.bulk_create(to_create, batch_size=500)
    if to_update:
        OzonReportRow.objects.bulk_update(to_update, ["data", "sort_key"], batch_size=500)
