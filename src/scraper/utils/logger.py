import logging

def get_logger(log_path):
    logger = logging.getLogger("youth-soccer-index")
    logger.setLevel(logging.INFO)

    fh = logging.FileHandler(log_path, mode="a", encoding="utf-8")
    ch = logging.StreamHandler()

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    if not logger.handlers:
        logger.addHandler(fh)
        logger.addHandler(ch)

    return logger
