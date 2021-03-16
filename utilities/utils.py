import logging
import json_log_formatter
class CommonUtilities():
    def init_logger(self, log_file):
        formatter = json_log_formatter.JSONFormatter()

        json_handler = logging.FileHandler(filename=log_file)
        json_handler.setFormatter(formatter)

        logger = logging.getLogger('my_json')
        logger.addHandler(json_handler)
        logger.setLevel(logging.INFO)

        return logger

    def clean_string(self, string):
        return string.strip().lower().replace('-', '_').replace(' ', '_')
