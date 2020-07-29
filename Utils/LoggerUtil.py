import json
import logging
from logging.config import dictConfig

import os


class LoggerUtil(object):
    def __init__(self, default_path):
        path = os.path.dirname(__file__) + "/" + "logging.json"
        logging_config = json.load(open(path))
        dictConfig(logging_config)
        log = logging.getLogger()
        self._logger = log

    def get(self):
        return self._logger
