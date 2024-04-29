import functools

from scrapeops_python_logger.exceptions import ScrapeOpsAPIResponseError

def exception_handler(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except ScrapeOpsAPIResponseError as e:
            pass
        except Exception as e:
            pass
    return wrapper


