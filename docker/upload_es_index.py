from tl.utility.utility import Utility
from api.variable.ethiopia_wikifier import EthiopiaWikifier
import time
import logging

_logger = logging.getLogger(__name__)

es_address = "localhost"
es_port = "9200"
es_server = "http://{}:{}".format(es_address, es_port)

_logger.warning("Waiting Elastic Search ready...")
# wait until es is ready
while not Utility.check_es_ready(es_address, es_port):
    time.sleep(2)
_logger.warning("Elastic Search is ready now!")

wikifier_unit = EthiopiaWikifier(es_server=es_server)
wikifier_unit.upload_to_es("/src/metadata/region-ethiopia-exploded-edges.tsv")
