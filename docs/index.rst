Welcome to the *bamboo* documentation!
======================================

*bamboo* is an application that systematizes realtime data analysis. *bamboo*
provides an interface for merging, aggregating and adding algebraic
calculations to dynamic datasets.  Clients can interact with *bamboo* through a
REST web interface and through Python.

*bamboo* supports a simple querying language to build calculations
(e.g. student teacher ratio) and aggregations (e.g. average number of students
per district) from datasets. These are updated as new data is received.

*bamboo* uses `pandas <http://pandas.pydata.org/>`_ for data analysis,
`pyparsing <http://pyparsing.wikispaces.com/>`_ to read formulas, and `mongodb
<http://www.mongodb.org/>`_ to serialize data.

.. image:: https://farm4.staticflickr.com/3363/3419345800_2c6c4133d3_z.jpg?zz=1

*bamboo* is `open source <https://github.com/modilabs/bamboo>`_ software
released under the 3-clause BSD license, which is also known as the "Modified
BSD License".

.. image:: https://secure.travis-ci.org/modilabs/bamboo.png?branch=master
  :target: http://travis-ci.org/modilabs/bamboo

Libraries that connect to *bamboo*
----------------------------------

JavaScript
^^^^^^^^^^

`bamboo.JS <http://modilabs.github.com/bamboo.js/>`_

Python
^^^^^^

`PyBamboo <https://github.com/modilabs/pybamboo>`_

Python Library Usage
--------------------

Dependencies
^^^^^^^^^^^^

* python (tested on version 2.7)
* mongodb

for numpy, pandas, and scipy:
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

On Arch Linux: ``# pacman -S blas lapack gcc-fortran``.

On Debian based: ``# apt-get install gfortran libatlas-base-dev``.


Installation
^^^^^^^^^^^^

.. code-block:: sh

    $ pip install bamboo-server

Python pip package for `bamboo <http://pypi.python.org/pypi/bamboo-data/0.5.4.1>`_.

For creating plots: ``$ pip install matplotlib``.

Usage
^^^^^

.. code-block:: python

    import bamboo as bm

    bf = bm.BambooFrame([{'date': '2012-12-21'}])
    bff = bf.recognize_dates()
    bff.to_json()

    >>> '[{"date": {"$date": 1356048000000}}]'

    # Turn asyncronous processing off
    bm.set_async(False)

    url = 'http://formhub.org/mberg/forms/good_eats/data.csv'
    dataset = bm.Dataset.create()
    dataset.import_from_url(url, na_values=['n/a'])
    dataset.schema

    >>> {u'_gps_altitude': {u'cardinality': 14, u'label': u'_gps_altitude', ...

    # Resample monthly, 'M', aggregating by mean
    date_column = 'submit_date'
    monthly = ds.resample(date_column, 'M', 'mean').set_index(date_column)
    monthly_amounts = monthly.amount.dropna()

    # Plot the amount spent per month
    mothly_amounts.plot()

.. image:: https://raw.github.com/modilabs/bamboo/master/docs/images/amount.png

REST API Usage
--------------

.. toctree::
   :maxdepth: 2

   basic_commands
   manipulating_data
   advanced_commands

Formula Reference
^^^^^^^^^^^^^^^^^

.. toctree::
   :maxdepth: 2

   calculations
   aggregations

Code Structure
--------------

.. toctree::
   :maxdepth: 2

   class_structure

Code Documentation
------------------

.. toctree::
   :maxdepth: 2

   config
   controllers
   core
   lib
   models

Server Setup and Contributing Code
----------------------------------

.. toctree::
   :maxdepth: 2

   server_setup
   contributing

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
