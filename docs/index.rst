Welcome to the *bamboo* documentation!
======================================

.. image:: https://raw.github.com/pld/bamboo/master/docs/images/bamboo_dev2013.png
   :height: 600
   :width: 800

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

*bamboo* is `open source <https://github.com/pld/bamboo>`_ software
released under the 3-clause BSD license, which is also known as the "Modified
BSD License".

**http://bamboo.io** hosts bamboo for demonstration purposes **only**. Do not put
critical data here, it may be deleted without notice.

.. image:: https://secure.travis-ci.org/pld/bamboo.png?branch=master
  :target: http://travis-ci.org/pld/bamboo

Examples
--------

.. toctree::
   :maxdepth: 2

   examples


REST API Usage
--------------

.. toctree::
   :maxdepth: 2

   basic_commands
   plotting
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

Libraries that connect to *bamboo*
----------------------------------

JavaScript
^^^^^^^^^^

`bamboo.JS <http://sel-columbia.github.com/bamboo.js/>`_

Python
^^^^^^

`PyBamboo <https://github.com/sel-columbia/pybamboo>`_

Python Library Usage
--------------------

Dependencies
^^^^^^^^^^^^

* python (tested on version 2.7)
* `mongodb <http://www.mongodb.org/>`_ (make sure to install the latest packages from 10gen, *not what might be in your default pkg manager*):
  
  - `Debian <http://docs.mongodb.org/manual/tutorial/install-mongodb-on-debian/>`_
  - `RedHat/CentOS/Fedora <http://docs.mongodb.org/manual/tutorial/install-mongodb-on-red-hat-centos-or-fedora-linux/>`_
  - `Ubuntu <http://docs.mongodb.org/manual/tutorial/install-mongodb-on-ubuntu/>`_
  - `Other Linux <http://docs.mongodb.org/manual/tutorial/install-mongodb-on-linux/>`_
  - `Mac OSX <http://docs.mongodb.org/manual/tutorial/install-mongodb-on-os-x/>`_
  - `Windows <http://docs.mongodb.org/manual/tutorial/install-mongodb-on-windows/>`_


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

.. image:: https://raw.github.com/pld/bamboo/master/docs/images/amount.png

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
