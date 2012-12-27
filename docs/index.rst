Welcome to the *bamboo* documentation!
======================================

*bamboo* is an application that systematizes realtime data analysis. *bamboo*
provides an interface for merging, aggregating and adding algebraic
calculations to dynamic datasets.  Clients can interact with *bamboo* through a
REST web interface and through Python.

*bamboo* supports a simple querying language to build calculations
(e.g. student teacher ratio) and aggregations (e.g. average number of students
per district) from datasets. These are updated as new data is received.

*bamboo* is `open source <https://github.com/modilabs/bamboo>`_ software
released under the 3-clause BSD license, which is also known as the "Modified
BSD License".

.. image:: https://secure.travis-ci.org/modilabs/bamboo.png?branch=master
  :target: http://travis-ci.org/modilabs/bamboo

General configuration notes:

    - Summarization on factor columns is disallowed based upon the number of
      unique entries.  This is controlled by a parameter in the
      ``core/summary`` module.

Libraries that connect to *bamboo*
----------------------------------

JavaScript
^^^^^^^^^^

`bamboo.JS <http://modilabs.github.com/bamboo_js/>`_

Python
^^^^^^

`PyBamboo <https://github.com/modilabs/pybamboo>`_

Python Library Usage
--------------------

Installation
^^^^^^^^^^^^

.. code-block:: sh

    $ pip install bamboo-data

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

REST API Usage
--------------

.. toctree::
   :maxdepth: 2

   basic_commands
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

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
