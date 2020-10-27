import logging

from django.apps import AppConfig

try:
    from .settings import DEBUG, LOGGING
except ImportError:
    DEBUG = None
    LOGGING = None

logger = logging.getLogger(__name__)


class MyAppConfig(AppConfig):
    name = "mainapp"
    verbose_name = "Lynx.MD Back-End"

    def ready(self):
        if DEBUG:
            logger.info("Server is running in DEBUG mode")

        if LOGGING and LOGGING.get("root") and LOGGING["root"].get("level"):
            logger.info(f"Server logging-level is {LOGGING['root']['level']}")
        else:
            logger.info("Could not determine LOGGING root logging level")

        # In practice, signal handlers are usually defined in a signals submodule of the application they relate to.
        # Signal receivers are connected in the ready() method of your application configuration class.
        # If you’re using the receiver() decorator, import the signals submodule inside ready().
        # https://docs.djangoproject.com/en/3.1/topics/signals/
        # noinspection PyUnresolvedReferences
        import mainapp.signals  # unimport:skip
