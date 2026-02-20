from django.conf import settings
from django.shortcuts import redirect


class LoginRequiredMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response
        self.login_url = settings.LOGIN_URL
        self.exempt_prefixes = (
            self.login_url,
            "/auth/",
            "/admin/",
            settings.STATIC_URL,
        )

    def __call__(self, request):
        path = request.path
        if not request.user.is_authenticated:
            for prefix in self.exempt_prefixes:
                if prefix and path.startswith(prefix):
                    return self.get_response(request)
            return redirect(self.login_url)
        return self.get_response(request)
