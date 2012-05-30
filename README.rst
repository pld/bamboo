Bamboo
======


.. image:: https://secure.travis-ci.org/modilabs/bamboo.png?branch=master
  :target: http://travis-ci.org/modilabs/bamboo

Dependencies
------------

* python
* mongodb

Installation
------------
    
    ``./bin/install.sh``

Running
-------

    start mongodb on localhost and standard port

    ``python bamboo.py``

Testing
-------

    install nose testing requirements
    
    ``pip install -r requirements-test.pip``

    run tests

    ``nosetests --with-progressive --with-cov``

Documentation
-------------

Viewing Documentation
^^^^^^^^^^^^^^^^^^^^^

    The latest docs are available at http://bamboo.modilabs.org/
    
Building Documentation
^^^^^^^^^^^^^^^^^^^^^^

    ``pip install -r requirements-docs.pip``

    ``cd doc``

    ``make html``

Contributing Code
-----------------

To work on the code:

1. fork this github project
2. add tests for your new feature
3. add the code for your new feature
4. ensure all existing tests and your new tests are passing
5. submit a pull request

Example Usage
-------------

    ``curl -X POST -d "url=http://formhub.org/mberg/forms/good_eats/data.csv" http://localhost:8080/datasets``
