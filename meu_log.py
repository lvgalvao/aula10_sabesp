from loguru import logger

def log_decorator(func):
    def wrapper(*args, **kwargs):
        logger.info(f"Chamando '{func.__name__}' com {args} e {kwargs}")
        try:
            result = func(*args, **kwargs)
            logger.info(f"'{func.__name__}' retornou {result}")
            return result
        except Exception as e:
            logger.exception(f"'{func.__name__}' lançou uma exceção: {e}")
            raise
    return wrapper