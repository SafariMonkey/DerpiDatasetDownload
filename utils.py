import logging
import os
import sys
import random
import requests
import time
import traceback
import threading


threadSessionHolder = threading.local()


def thread_local_session():
    if  getattr(threadSessionHolder, 'initialised', None) is None:
        threadSessionHolder.session = requests.Session()
        logging.debug("Made new session")
        threadSessionHolder.initialised = True
    return threadSessionHolder.session


def setup_logging():
    log_level = logging.INFO
    if os.getenv("DEBUG", "false") == "true":
        log_level = logging.DEBUG
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        stream=sys.stderr,
        level=log_level,
    )


def print_success(message, f, *args, **kwargs):
    f(*args, **kwargs)
    logging.debug(message)


def print_before(message, f, *args, **kwargs):
    logging.debug(message)
    f(*args, **kwargs)


def print_exc(f, *args, **kwargs):
    try:
        f(*args, **kwargs)
    except Exception as e:
        formatted = ''.join(traceback.format_exception(type(e), e, e.__traceback__))
        logging.debug(f"Encountered error: {str(e)}: \n{formatted}")


def rate_limit(sleep, tries, f, *args, **kwargs):
    try:
        f(*args, **kwargs)
    except Exception as e:
        if '429' in str(e) and tries > 0:
            random_sleep = sleep * 2 * random.random()
            logging.debug(f"backing off ~{sleep}s ({random_sleep:.2f}s)")
            time.sleep(random_sleep)
            rate_limit(sleep * 2, tries - 1, f, *args, **kwargs)
        else:
            raise

def time_call(message, f, *args, **kwargs):
        start = time.time()
        f(*args, **kwargs)
        end = time.time()
        logging.info(f"Time for {message}: {end - start:.2f}s")


def free_space_left(path):
    statvfs = os.statvfs(path)
    return statvfs.f_frsize * statvfs.f_bavail


def generate_numbers_below(max, count=None):
    iters = 0
    while True:
        number = random.randint(0, max)
        yield number
        if count is not None and not iters < count:
            break
        iters += 1