import json
import os
import logging
import datetime
from logging.handlers import TimedRotatingFileHandler


def load_config(path):
	with open(path) as f:
		data = f.read()
		return json.loads(data)


class MicroFormatter(logging.Formatter):
	converter=datetime.datetime.fromtimestamp
	def formatTime(self, record, datefmt=None):
		ct = self.converter(record.created)
		if datefmt:
			s = ct.strftime(datefmt)
		else:
			t = ct.strftime("%Y-%m-%d %H:%M:%S")
			s = "%s,%03d" % (t, record.msecs)
		return s

def configure_logging(log_file=None, file_log_level='DEBUG', console_log_level='INFO'):
	log_format = '%(asctime)s|%(levelname).1s|%(message)s\t|%(module)s:%(lineno)s'
	log_datefmt = '%Y-%m-%d %H:%M:%S.%f'
	logger = logging.getLogger('')
	logger.setLevel(file_log_level)
	ch = logging.StreamHandler()
	ch.setLevel(console_log_level)
	ch.setFormatter(MicroFormatter(fmt=log_format, datefmt=log_datefmt))
	logger.addHandler(ch)
	if log_file:
		fh = TimedRotatingFileHandler(log_file, when='midnight')
		fh.setLevel(file_log_level)
		fh.setFormatter(MicroFormatter(fmt=log_format, datefmt=log_datefmt))
		fh.suffix = '%Y-%m-%d.log'
		logger.addHandler(fh)
		fh.doRollover()