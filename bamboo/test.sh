#!/bin/sh
nosetests --with-progressive --with-cov --cov-report term-missing ../bamboo

# for profiling
# nosetests --with-profile tests/models/test_dataset.py --profile-restrict 'models/dataset'
