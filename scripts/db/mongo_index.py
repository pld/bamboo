#!/usr/bin/env python

import os
import sys
sys.path.append(os.getcwd())

from pymongo import ASCENDING

from bamboo.config.db import Database
from bamboo.core.frame import DATASET_ID
from bamboo.models.observation import Observation

# The encoded dataset_id will be set to '0'.
ENCODED_DATASET_ID = '0'


def bamboo_index(collection, key):
    ensure_index = collection.__getattribute__('ensure_index')
    ensure_index([(key, ASCENDING)])
    ensure_index([(key, ASCENDING), (Observation.DELETED_AT, ASCENDING)])


def ensure_indexing():
    """Ensure that bamboo models are indexed."""
    db = Database.db()

    # collections
    calculations = db.calculations
    datasets = db.datasets
    observations = db.observations

    # indices
    bamboo_index(datasets, DATASET_ID)
    bamboo_index(observations, ENCODED_DATASET_ID)
    bamboo_index(observations, Observation.ENCODING_DATASET_ID)
    bamboo_index(calculations, DATASET_ID)


if __name__ == '__main__':
    ensure_indexing()
