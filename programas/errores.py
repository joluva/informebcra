import logging
import socket
from logging.handlers import SysLogHandler

class ContextFilter(logging.Filter):
    hostname = socket.gethostname()

    def filter(self, record):
        record.hostname = ContextFilter.hostname
        return True
    
syslog=SysLogHandler(address=('logs5.papertrailapp.com',47297))
syslog.addFilter(ContextFilter())
format='%(asctime)s %(hostname)s Bcra-Worldsys: %(message)s'
formatter = logging.Formatter(format, datefmt='%b %d %H:%M:%S')
syslog.setFormatter(formatter)
logger=logging.getLogger()
logger.addHandler(syslog)
logger.setLevel(logging.INFO)
#logger.info('Es un mensaje')