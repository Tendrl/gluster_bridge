import logging

from gluster_bridge.config import TendrlConfig
config = TendrlConfig()


FORMAT = "%(asctime)s - %(levelname)s - %(name)s %(message)s"
log = logging.getLogger('gluster_bridge')
handler = logging.FileHandler(config.get('common', 'log_path'))
handler.setFormatter(logging.Formatter(FORMAT))
log.addHandler(handler)
log.setLevel(logging.getLevelName(config.get('common', 'log_level')))
