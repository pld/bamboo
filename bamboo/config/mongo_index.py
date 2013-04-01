#!/usr/bin/env python

import os
import sys
sys.path.append(os.getcwd())

from pymongo import ASCENDING

from bamboo.config.db import Database


def ensure_indexing():
    """Ensure that bamboo models are indexed."""
    db = Database.db()
    calculations = db.calculations
    datasets = db.datasets
    observations = db.observations
    datasets.ensure_index([("BRK_dataset_id", ASCENDING)])
    observations.ensure_index([("0", ASCENDING)])
    calculations.ensure_index([("BRK_dataset_id", ASCENDING)])


if __name__ == "__main__":
    ensure_indexing()
