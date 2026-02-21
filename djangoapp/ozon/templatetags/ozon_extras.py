from django import template


register = template.Library()


@register.filter
def get_item(data, key):
    if not isinstance(data, dict):
        return ""
    return data.get(key, "")
