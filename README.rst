Bamboo
======


.. image:: https://secure.travis-ci.org/modilabs/bamboo.png
  :target: http://travis-ci.org/modilabs/bamboo

Dependencies
------------

* python
* mongodb

Installation
============
    
    ./bin/install.sh

Testing
=======

    [install nose testing requirements]
    
    ``pip install -r requirements-test.pip``

    [run tests]

    ``nosetests --with-progressive --with-cov``

Running
=======

    [start mongodb on localhost and standard port]

    ``python bamboo.py``

Example Usage
=============

    ``curl -X POST -d "url=http://formhub.org/mberg/forms/good_eats/data.csv" http://localhost:8080/datasets``
