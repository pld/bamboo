.. bamboo documentation master file, created by
   sphinx-quickstart on Fri May 25 12:48:30 2012.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to bamboo's documentation!
==================================

Bamboo is a data analysis web service.
Bamboo is `open source <https://github.com/modilabs/bamboo>`_ software.

Bamboo supports a simple querying language to build calculations
(e.g. student teacher ratio) and aggregations (e.g. average number of students
per district) from datasets. These are updated as new data is received.

General configuration notes:

    - Summarization on factor columns is disallowed based upon the number of
      unique entries.  This is controlled by a parameter in the
      ``core/summary`` module.

Contents:

.. toctree::
   :maxdepth: 2

   basic_commands
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

