import numpy as np
import pandas as pd
import pymongo
import os
import errno
import logging
from my_scival import InstitutionSearch, MetricSearch

from urllib import parse, request
from urllib.error import HTTPError

BASE_DIR = os.chdir("..")


logger = logging.getLogger(__name__ + "InstitutionSearch")
logger.setLevel(logging.DEBUG)

# create file handler which logs info messages
fh = logging.FileHandler(os.path.join('logs', 'logs.txt'), 'w', 'utf-8')
fh.setLevel(logging.INFO)

# create console handler with a debug log level
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

# creating a formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)-8s: %(message)s')

# setting handler format
fh.setFormatter(formatter)
ch.setFormatter(formatter)

# add the handlers to the logger
logger.addHandler(fh)
logger.addHandler(ch)


if __name__=="__main__":

    MY_API_KEY = "e53785aedfc1c54942ba237f8ec0f891"

    apiKey = "e53785aedfc1c54942ba237f8ec0f891"

    logger.debug("creating a class")
    a = InstitutionSearch(["Harvard University"], MY_API_KEY)

    logger.debug("sending the request")
    res = a.encode()

    aff_id = 508175
    logger.debug("creating a metrics class")
    m = MetricSearch(MY_API_KEY, aff_id)
