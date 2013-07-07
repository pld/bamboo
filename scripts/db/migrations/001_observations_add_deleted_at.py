#!/usr/bin/env python

import os
import sys
sys.path.append(os.getcwd())

from bamboo.models.observation import Observation


def migrate():
    observation = Observation
    record = {Observation.DELETED_AT: 0}
    observation.collection.update({}, {'$set': record}, multi=True)


if __name__ == '__main__':
    migrate()
