#!/usr/bin/env python

import os
import sys
sys.path.append(os.getcwd())

from pymongo import ASCENDING

from bamboo.config.db import Database
from bamboo.core.frame import DATASET_ID
from bamboo.models.abstract_model import AbstractModel
from bamboo.models.observation import Observation


def ensure_indexing():
    """Ensure that bamboo models are indexed."""
    db = Database.db()
    calculations = db.calculations
    datasets = db.datasets
    observations = db.observations
    datasets.ensure_index([(DATASET_ID, ASCENDING)])
    # The encoded dataset_id will be set to '0'.
    observations.ensure_index(
        [('0', ASCENDING), (AbstractModel.DELETED_AT, ASCENDING)])
    observations.ensure_index([(Observation.ENCODING_DATASET_ID, ASCENDING)])
    calculations.ensure_index([(DATASET_ID, ASCENDING)])


if __name__ == '__main__':
    ensure_indexing()
