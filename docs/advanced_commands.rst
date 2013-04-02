Advanced Commands
=================

.. note::

    On the command line some special characters may need to be escaped for
    the commands to function correctly.  E.g. ``&`` as ``\&``, ``?`` as ``\?``,
    ``=`` as ``\=``.

.. note::

    [*SIC*] all spelling errors in the example dataset.

Creating a Dataset from a Schema
--------------------------------

You can create a dataset from a schema. Your schema must be written in the
`Simple Data Format (SDF) <http://www.dataprotocols.org/en/latest/simple-data-format.html>`_.

Below is an example SDF schema file:

.. literalinclude:: ../bamboo/tests/fixtures/good_eats.schema.json
   :language: javascript

Supposing this file is saved locally as ``/home/modilabs/good_eats.schema.json``,
you can create a dataset from this scema using:

.. code-block:: sh

    curl -X POST -F schema=@/home/modilabs/good_eats.schema.json http://bamboo.io/datasets

**returns:**

.. code-block:: javascript

    {
        "id": "8a3d74711475d8a51c84484fe73f24bd151242ea"
    }

Create a Dataset with data from a Schema
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To create a dataset from a schema and upload a CSV file for it, use the
following command:

.. code-block:: sh

    curl -X POST -F schema=@/home/modilabs/good_eats.schema.json -F csv_file=@/home/modilabs/good_eats.csv http://bamboo.io/datasets

And similarly for a JSON file:

.. code-block:: sh

    curl -X POST -F schema=@/home/modilabs/good_eats.schema.json -F json_file=@/home/modilabs/good_eats.json http://bamboo.io/datasets

**returns:**

.. code-block:: javascript

    {
        "id": "8a3d74711475d8a51c84484fe73f24bd151242ea"
    }


Creating Multiple Calculations via JSON
---------------------------------------

You can create multiple calculations by uploading a properly formatted JSON
file to the create calculations endpoint.

Below is an example calculations JSON file:

.. literalinclude:: ../bamboo/tests/fixtures/good_eats_group.calculations.json
   :language: javascript

Supposing this file is saved locally as ``/home/modilabs/good_eats.calculations.json``,
you can create a dataset from this json file using:

.. code-block:: sh

    curl -X POST -F json_file=@/home/modilabs/good_eats.calculations.json http://bamboo.io/calculations/8a3d74711475d8a51c84484fe73f24bd151242ea

**returns:**

.. code-block:: javascript

    {
        "success": "created calculations from JSON for dataset: 8a3d74711475d8a51c84484fe73f24bd151242ea"
    }

.. note::

    The file can also contain a single dictionary, for example:

.. code-block:: javascript

    {
        "name": "in northern hemisphere",
        "formula": "gps_latitude > 0"
    }

Replicating a Calculation Across Groups
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To create the same calculation with multiple groups you may include a
``groups`` key in your JSON file with the value as a list of groups to create
the calculation for.

The ``groups`` key can also be set to a string, in which case bamboo will
create a single calculation with the value as the group.

For example, below the second to third calculation is created with the group
*risk_factor*, and the fourth with the single multi-group *risk_factor* and
*food_type*.  The final calculation is create twice, once for the group
*risk_factor*, and once for the group *food_type*.

.. literalinclude:: ../bamboo/tests/fixtures/good_eats_group.calculations.json
   :language: javascript

Create a "perishable" dataset
-----------------------------

Suppose you want to upload a dataset, perform some calculations, and use the
results, but you do not want your data to be permanently stored.  To
accommodate this use case dataset creation supports on optional ``perish``
parameter.  The ``perish`` parameter is an integer which specifies the number
of seconds after which to delete the dataset.  For example, to create a dataset
that will be deleted after one day:

.. code-block:: sh

    curl -X POST -d "url=http://formhub.org/mberg/forms/good_eats/data.csv&perish=86400" http://bamboo.io/datasets

**returns:**

.. code-block:: javascript

    {
        "id": "8a3d74711475d8a51c84484fe73f24bd151242ea"
    }

Additional dataset query parameters
-----------------------------------

Dataset queries may take the following optional parameters:

- ``select``: This is a required argument, it can be 'all' or a
   MongoDB JSON query
- ``distinct``: A field to return distinct results for.
- ``query``: If passed restrict results to rows matching this query.
- ``limit``: If passed limit the rows to this number.
- ``order_by``: If passed order the result using this column.
- ``format``: Format of output data, 'json' or 'csv'
- ``callback``: A JSONP callback function to wrap the result in.
- ``index``: Boolean, if true also return the index.  Default false.
- ``count``: Boolean, if true returns only the number of results.  Default false.

Export data using the format parameter
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You may use the ``format`` parameter as an alternative to file extension
notation.

To export the data as CSV:

.. code-block:: sh

    curl http://bamboo.io/datasets/8a3d74711475d8a51c84484fe73f24bd151242ea?format=CSV

To export the data as JSON:

.. code-block:: sh

    curl http://bamboo.io/datasets/8a3d74711475d8a51c84484fe73f24bd151242ea?format=JSON

Ordering the results of a query
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ``order_by`` parameter sorts the resulting rows according to a column value
and a sign indicating to order ascending (default) or descending
(the ``-`` sign).

For example:

.. code-block:: sh

    curl http://bamboo.io/datasets/8a3d74711475d8a51c84484fe73f24bd151242ea&order_by=amount
    curl http://bamboo.io/datasets/8a3d74711475d8a51c84484fe73f24bd151242ea&order_by=-amount

Using a JSONP callback
^^^^^^^^^^^^^^^^^^^^^^

If you would like to retrieve your dataset from bamboo.io and process the
result using JavaScript, you can define a JavaScript callback to do so.

Suppose you have defined the JavaScript callback function
``handleBambooDataset(json)`` to process the JSON dataset.

You can pass this to bamboo using the ``json`` parameter as follows:

.. code-block:: sh

    curl http://bamboo.io/datasets/8a3d74711475d8a51c84484fe73f24bd151242ea&jsonp=handleBambooDataset

Updating Dataset Metadata
-------------------------

The following metadata fields can be added to a dataset:

* ``attribution``: attribution and original of this dataset.
* ``description``: a text description of the dataset.
* ``label``: a label or name for this dataset.
* ``license``: the license this dataset is under.

The fieldname is in bold with suggested uses to the right.

To set the metadata on a dataset, make a PUT request to ``info``, this will
also return the update dataset info.

.. code-block:: sh

    curl -X PUT -d "description=Good%20eats%20description&license=public&attribution=mlberg&label=goodeats" http://bamboo.io/datasets/8a3d74711475d8a51c84484fe73f24bd151242ea/info

**returns:**

.. code-block:: javascript

    {
        "attribution": "mlberg",
        "description": "Good eats description",
        "id": "8a3d74711475d8a51c84484fe73f24bd151242ea",
        "label": "goodeats",
        "license": "public",
        "schema": {
            "amount": {
                "label": "Amount",
                "olap_type": "measure",
                "simpletype": "float"
            },
            "rating": {
                "label": "Rating",
                "olap_type": "dimension",
                "simpletype": "string",
                "cardinality": 2
            },
            "food_type": {
                "label": "Food Type",
                "olap_type": "dimension",
                "simpletype": "string",
                "cardinality": 8
            },
            ...
        },
        "created_at": "2012-6-18 14:43:32",
        "updated_at": "2012-6-18 14:43:32",
        "num_rows": "500",
        "num_columns": "30",
        "state": "ready"
    }

Setting the OLAP Type
^^^^^^^^^^^^^^^^^^^^^

By default columns with numeric data are treated as measures and return summary
statistics.  However, if your numeric data represents indentifiers it may be
semantically dimensional.  You can use set OLAP Type to treat numeric data
dimensionally.

Any numeric column can be converted to a dimension and back.  Dimension columns
cannot be modified.  For example, to convert the numeric column ``amount`` to a
dimension:

.. code-block:: sh

    curl -X PUT -d "column=amount&olap_type=dimension" http://bamboo.io/datasets/8a3d74711475d8a51c84484fe73f24bd151242ea/set_olap_type

**returns:**

.. code-block:: javascript

    {"success": "set OLAP Type for column 'amount' to 'dimension'}


Timeseries operations on a dataset
----------------------------------

Resampling a dataset
^^^^^^^^^^^^^^^^^^^^

If your dataset contains any date columns, you can resample numeric columns in
your data based on any of these date columns.

Any options that can be passed to the pandas ``resample`` functions
(`pandas docs <http://pandas.pydata.org/pandas-docs/stable/timeseries.html#time-series-date-functionality>`_)
function can be passed as parameters to bamboo.

The parameters are

* ``date_column``: The date column to resample on.
* ``interval``: A code for the interval to use, any pandas codes are accepted,
    e.g. 'D' for daily, 'W' for weekly, 'M' for monthly.
* ``how``: (Optional) How to calculate the grouped samples.  The default is
    'mean'.
* ``query``: (Optional) A MongoDB query to restrict the dataset, only data
    matching the query will be resampled.
* ``format``: (Optional) Set to 'csv' to return a CSV of the resampled data.

For example, to resample a dataset at monthly intervals by mean use the
following command:

.. code-block:: sh

    curl http://bamboo.io/datasets/8a3d74711475d8a51c84484fe73f24bd151242ea/resample?date_column=submit_date&interval=M&how=mean

**returns:**

.. code-block:: javascript

    [
        {
            '_percentage_complete': 'null',
            'submit_date': {'$date': 1325289600000},
            'gps_alt': 36.78334554038333,
            'amount': 7.107142857142857,
            'gps_latitude': 40.622100085116664,
            '_id': 358490042584000.0,
            'gps_precision': 43.333333333333336,
            'gps_longitude': 29.94870928221667
        },
        {
            '_percentage_complete': 'null',
            'submit_date': {'$date': 1327968000000},
            'gps_alt': 168.11249542212502,
            'amount': 233.5625,
            'gps_latitude': 29.618330361937502,
            '_id': 358490042584000.0,
            'gps_precision': 47.0,
            'gps_longitude': 18.21789164405},
        {
            '_percentage_complete': 'null',
            'submit_date': {'$date': 1330473600000},
            'gps_alt': 0.0,
            'amount': 45.0,
            'gps_latitude': 40.8076961,
            '_id': 358490042584000.0,
            'gps_precision': 588.0,
            'gps_longitude': -73.95805440000001
        },
        {
            '_percentage_complete': 'null',
            'submit_date': {'$date': 1333152000000},
            'gps_alt': 0.0,
            'amount': 12.0,
            'gps_latitude': 49.26994343,
            '_id': 358490042584000.0,
            'gps_precision': 56.0,
            'gps_longitude': -123.15297941
        },
        {
            '_percentage_complete': 'null',
            'submit_date': {'$date': 1335744000000},
            'gps_alt': 281.30000305175,
            'amount': 16.125,
            'gps_latitude': 26.66630736555,
            '_id': 358490042584000.0,
            'gps_precision': 22.5,
            'gps_longitude': 1.530683878799998
        }
    ]

Calculating rolling statistics
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To compute *moving* or *rolling* statistics / moments you can use the
``rolling`` request.

Any options that can be passed to the pandas ``rolling_window`` function
(`pandas docs <http://pandas.pydata.org/pandas-docs/dev/computation.html#moving-rolling-statistics-moments>`_)
can be passed as parameters to bamboo.

Window types are passed as the ``win_type`` parameter. See
`here <https://en.wikipedia.org/wiki/Window_function#Window_examples>`_ for
window type definitions and examples.

For example, to calculating a rolling mean with a window of 3 values, use the
following command:

.. code-block:: sh

    curl http://bamboo.io/datasets/8a3d74711475d8a51c84484fe73f24bd151242ea/rolling?win_type=boxcar&window=3

**returns:**

.. code-block:: javascript

    [
        {
            'gps_longitude': 'null',
            '_percentage_complete': 'null',
            'gps_latitude': 'null',
            'amount': 'null',
            'gps_alt': 'null',
            '_id': 'null',
            'gps_precision': 'null'
        },
        {
            'gps_longitude': 'null',
            '_percentage_complete': 'null',
            'gps_latitude': 'null',
            'amount': 'null',
            'gps_alt': 'null',
            '_id': 'null',
            'gps_precision': 'null'
        },
        {
            'gps_longitude': 28.97413979283333,
            '_percentage_complete': 'null',
            'gps_latitude': 41.018141275299996,
            'amount': 4.583333333333333,
            'gps_alt': 45.60001627606666,
            '_id': 'null', 'gps_precision': 41.666666666666664
        },
        ...
    ]

.. note::

    The first ``window - 1`` rows will be null, because not enough
    data will have been seen to calculate rolling statistics for those rows.
