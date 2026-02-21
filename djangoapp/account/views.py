import logging

from django.contrib import messages
from django.contrib.auth import get_user_model
from django.contrib.auth.tokens import PasswordResetTokenGenerator
from django.http import Http404
from django.shortcuts import redirect, render
from django.urls import reverse
from django.utils.encoding import force_bytes
from django.utils.http import urlsafe_base64_decode, urlsafe_base64_encode
from django.template.loader import render_to_string

from .forms import RegisterForm
from .tasks import send_verification_email_task

logger = logging.getLogger("email.delivery")


class EmailVerificationTokenGenerator(PasswordResetTokenGenerator):
    def _make_hash_value(self, user, timestamp):
        return f"{user.pk}{timestamp}{user.is_active}{user.email_verified}"


email_verification_token_generator = EmailVerificationTokenGenerator()


def _send_verification_email(request, user):
    uid = urlsafe_base64_encode(force_bytes(user.pk))
    token = email_verification_token_generator.make_token(user)
    verify_url = request.build_absolute_uri(
        reverse("account:verify_email", kwargs={"uidb64": uid, "token": token})
    )
    subject = "Verify your email"
    message = render_to_string(
        "account/email_verification_email.txt",
        {"user": user, "verify_url": verify_url},
    )
    try:
        task = send_verification_email_task.delay(subject, message, user.email)
        logger.info("Verification email task queued: user_id=%s email=%s task_id=%s", user.id, user.email, task.id)
    except Exception:
        logger.exception(
            "Verification email queue failed: user_id=%s email=%s",
            user.id,
            user.email,
        )


def register_view(request):
    if request.user.is_authenticated:
        return redirect("shops:list")

    if request.method == "POST":
        form = RegisterForm(request.POST)
        if form.is_valid():
            user = form.save(commit=False)
            user.is_active = False
            user.save()
            _send_verification_email(request, user)
            messages.success(request, "Check your email to confirm your account.")
            return redirect("account:login")
        logger.warning("Register form is invalid: errors=%s", form.errors.as_json())
    else:
        form = RegisterForm()

    return render(request, "account/register.html", {"form": form})


def verify_email_view(request, uidb64, token):
    user_model = get_user_model()
    try:
        uid = urlsafe_base64_decode(uidb64).decode()
        user = user_model.objects.get(pk=uid)
    except (TypeError, ValueError, OverflowError, user_model.DoesNotExist):
        raise Http404

    if email_verification_token_generator.check_token(user, token):
        user.email_verified = True
        user.is_active = True
        user.save(update_fields=["email_verified", "is_active"])
        messages.success(request, "Email confirmed. You can now sign in.")
        return redirect("account:login")

    messages.error(request, "The verification link is invalid or expired.")
    return redirect("account:login")
