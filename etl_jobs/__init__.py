# __init.py__

"""Top-level package for the our etl_jobs package"""

import logging
from dotenv import load_dotenv
import os

from .EtlJobForMajor import EtlJobForMajor
#from .EtlJobForICT import EtlJobForICT --- IGNORE ---

# logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)
log.info('Initializing : ' +  __name__ )

# env variables from .env
load_dotenv()
