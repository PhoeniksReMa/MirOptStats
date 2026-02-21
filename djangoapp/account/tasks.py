import logging

from celery import shared_task
from django.core.mail import send_mail


logger = logging.getLogger("email.delivery")


@shared_task(bind=True, autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={"max_retries": 5})
def send_verification_email_task(self, subject: str, message: str, to_email: str) -> int:
    sent_count = send_mail(subject, message, None, [to_email], fail_silently=False)
    if sent_count:
        logger.info("Verification email sent async: to=%s sent_count=%s", to_email, sent_count)
    else:
        logger.warning("Verification email not sent async (sent_count=0): to=%s", to_email)
    return sent_count
