from django.contrib.auth.models import AbstractUser


class User(AbstractUser):
    """
    Main user entity.
    Any user can own stores and can also be a staff member in someone else's store.
    """

    class Meta:
        verbose_name = "user"
        verbose_name_plural = "users"
