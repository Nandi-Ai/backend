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
