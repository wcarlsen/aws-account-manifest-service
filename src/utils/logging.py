import logging
import sys
import structlog


# Logging
def init_logging() -> None:
    logging.basicConfig(format="%(message)s", stream=sys.stdout, level=logging.INFO)

    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            # structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer(),
            # structlog.processors.KeyValueRenderer(),
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
