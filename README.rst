Dependencies
------------

* python
* mongodb

Installation
============

    ./install.sh

Testing
=======

    [install nose testing library]
    
    pip install nose

    [run tests]

    nosetests

Running
=======

    [start mongodb on localhost and standard port]

    python bamboo.py

Example Usage
=============

    curl -X POST -d "url=http://formhub.org/mberg/forms/good_eats/data.csv" http://localhost:8080/datasets
