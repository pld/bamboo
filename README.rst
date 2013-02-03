`bamboo <http://bamboo.io>`_
============================

.. image:: https://secure.travis-ci.org/modilabs/bamboo.png?branch=master
    :target: http://travis-ci.org/modilabs/bamboo

*bamboo* is an application that systematizes realtime data analysis. *bamboo*
provides an interface for merging, aggregating and adding algebraic
calculations to dynamic datasets.  Clients can interact with *bamboo* through a
a REST web interface and through Python.

*bamboo* supports a simple querying language to build calculations
(e.g. student teacher ratio) and aggregations (e.g. average number of students
per district) from datasets. These are updated as new data is received.

*bamboo* is `open source <https://github.com/modilabs/bamboo>`_ software released
under the 3-clause BSD license, which is also known as the "Modified BSD
License".

Dependencies
------------

* python (tested on version 2.7)
* mongodb

for numpy, pandas, and scipy (in requirements.pip):
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

on Arch Linux: ``# pacman -S blas lapack gcc-fortran``

on Debian based: ``# apt-get install gfortran libatlas-base-dev``

Using as a Python Libary
------------------------

Installation
^^^^^^^^^^^^

.. code-block:: sh

    $ pip install bamboo-data

Python pip package for `bamboo <http://pypi.python.org/pypi/bamboo-data/0.5.4.1>`_.

Usage
^^^^^

.. code-block:: python

    import bamboo as bm
    from bamboo.lib.io import create_dataset_from_url

    bf = bm.BambooFrame([{'date': '2012-12-21'}])
    bff = bf.recognize_dates()
    bff.to_json()

    >>> '[{"date": {"$date": 1356048000000}}]'

    # Turn asyncronous processing off
    bm.set_async(False)

    url = 'http://formhub.org/mberg/forms/good_eats/data.csv'
    dataset = create_dataset_from_url(url)
    dataset.schema

    >>> {u'_gps_altitude': {u'cardinality': 14, u'label': u'_gps_altitude', ...


Installation
------------

.. code-block:: sh

    $ ./scripts/install.sh

Example Usage
-------------

On the remote server
^^^^^^^^^^^^^^^^^^^^

running the example basic commands

.. code-block:: sh

    $ ./scripts/commands.sh

using `bamboo.JS <http://modilabs.github.com/bamboo_js/>`_

.. code-block:: javascript

    var dataset = new bamboo.Dataset({url: 'http://bitly.com/ZfzBwP'});
    bamboo.dataset_exists('nonexistentdataset_id');
    dataset.query_info();
    ...


using `pybamboo <https://github.com/modilabs/pybamboo>`_

.. code-block:: python

    from pybamboo import PyBamboo
    pybamboo = PyBamboo()
    response = pybamboo.store_csv_file('http://formhub.org/mberg/forms/good_eats/data.csv')
    dataset_id = response['id']
    ...

posting a dataset

.. code-block:: sh

    $ curl -X POST -d "url=http://formhub.org/mberg/forms/good_eats/data.csv" http://bamboo.io/datasets

Documentation
-------------

Viewing Documentation
^^^^^^^^^^^^^^^^^^^^^

The latest docs are available at http://bamboo.io/
      
About
-----

Join the `bamboo-dev mailing list <https://groups.google.com/forum/#!forum/bamboo-dev>`_.

*bamboo* is an open source project. The project features, in chronological order,
the combined efforts of

* Peter Lubell-Doughtie
* Mark Johnston

and other developers.

Projects using *bamboo*
-----------------------

* `formhub <http://formhub.org>`_ - Mobile Data Collection made easy
* `AFSIS <http://www.africasoils.net/>`_ - Africa Soil Information Service

Is your project using bamboo? `Let us know <https://groups.google.com/forum/#!forum/bamboo-dev>`_!
