import logging
import logging.handlers
import time
import os


def init_logging():
    ts = time.gmtime()
    cur_time = time.strftime("_%Y%m%d%H%M%S", ts)
    handler = logging.handlers.WatchedFileHandler(os.environ.get("LOGFILE", "rule_engine/log/log" + cur_time + ".log"))
    formatter = logging.Formatter('%(levelname)s - %(asctime)s - %(filename)s(%(lineno)d): %(message)s')
    handler.setFormatter(formatter)
    log = logging.getLogger('Base-qc-engine')
    log.setLevel(logging.INFO)
    log.addHandler(handler)
    return log


log = init_logging()