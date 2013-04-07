#!/usr/bin/env python

import os
import sys
sys.path.append(os.getcwd())

from pymongo import ASCENDING

from bamboo.config.db import Database
from bamboo.core.frame import DATASET_ID


def ensure_indexing():
    """Ensure that bamboo models are indexed."""
    db = Database.db()
    calculations = db.calculations
    datasets = db.datasets
    observations = db.observations
    datasets.ensure_index([(DATASET_ID, ASCENDING)])
    # The encoded dataset_id will be set to '0'.
    observations.ensure_index([("0", ASCENDING)])
    calculations.ensure_index([(DATASET_ID, ASCENDING)])


if __name__ == "__main__":
    ensure_indexing()
