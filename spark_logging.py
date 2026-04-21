import logging

logging.basicConfig(
    level = logging.INFO,
    format = '%(asctime)s [%(levelname)s] %(message)s'
)

logger = logging.getLogger(__name__)

logger.info("Pipeline started")
logger.error("Pipeline failed")
logger.critical("System down")