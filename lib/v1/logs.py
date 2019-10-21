import logging
import datetime

logger = logging.getLogger(__name__)
# Create handlers
c_handler = logging.StreamHandler()

c_handler.setLevel(logging.WARNING)

# Create formatters and add it to handlers
c_format = logging.Formatter('[%(asctime)s] p%(process)s {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s',
                             '%m-%d %H:%M:%S')

c_handler.setFormatter(c_format)

# Add handlers to the logger
logger.addHandler(c_handler)


def file_logs(file_name):
    file_name = datetime.datetime.now().strftime('{}_%H_%M_%d_%m_%Y.log'.format(file_name))
    f_handler = logging.FileHandler(file_name)
    f_handler.setLevel(logging.ERROR)
    f_format = logging.Formatter('[%(asctime)s] p%(process)s {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s',
                                 '%m-%d %H:%M:%S')
    f_handler.setFormatter(f_format)
    logger.addHandler(f_handler)
