Advanced Commands
=================

.. note::

    On the command line some special characters may need to be escaped for
    the commands to function correctly.  E.g. ``&`` as ``\&``, ``?`` as ``\?``,
    ``=`` as ``\=``.

.. note::

    [*SIC*] all spelling errors in the example dataset.

Updating your data
------------------

You can add single or multiple row updates to a dataset via JSON data in a PUT
request to the dataset id. The row(s) should be key-value pairs where the key
is the column name. In the example that we have been using here, the dataset
could be updated with a JSON dictionary like this:

.. code-block:: javascript

    {
        "rating": "delectible",
        "amount": 2,
        "food_type": "streat_sweets"
    }

N/A values will be added when the dictionary does not supply a value for a
given column.


.. code-block:: sh

    curl -X PUT -d 'update={"rating":"delectible","amount":2,"food_type":"streat_sweets"}' http://bamboo.io/datasets/8a3d74711475d8a51c84484fe73f24bd151242ea

**returns:**

.. code-block:: javascript

    {"id": "8a3d74711475d8a51c84484fe73f24bd151242ea"}

Merging multiple datasets
-------------------------

To row-wise merge 2 or more datasets into a new dataset use the _merge_ command
. For example, to merge two datasets with IDs 8123 and 9123, use the following
command. This will return the ID of the new merged dataset.  The merge occurs
in the background.  When the dataset status is set to "ready" you can be sure
the data has been merged.

.. code-block:: sh

    curl -X POST -d "datasets=[8123, 9123]" http://bamboo.io/datasets/merge

**returns:**

.. code-block:: javascript

    {"id": "8a3d74711475d8a51c84484fe73f24bd151242ea"}

Joining Multiple Datasets
-------------------------

You can perform column joins between multiple datasets.  The column that is
joined on must be unique in the right hand side dataset and must exist in both
datasets. The left hand side dataset ID is specified in the URL, and the right
hand side is passed as the *other_dataset_id* param.

If the join succeeds the ID of the new dataset is returned as JSON. If the join
fails an error message is returned, also as JSON.

Updates which are subsequently made to either the left hand side or the right
hand side dataset will be propagated to the joined dataset. Updates to the
right hand side that make the join column non-unique will be disallowed.

For example supposing the 'food_type' column is in dataset with ID 8123 and
9123, you can join the two datasets on that column by executing:

.. note::

    * You can not join datasets which have overlapping columns.
    * After you have joined two datasets you can not update the right hand
      (``other_dataset_id``) with data that will make its ``on`` column
      non-unique.
    * Updates to the left hand side (``dataset_id``) will be merged with right
      hand side columns if the update matches an existing ``on`` column,
      otherwise if will have NaN for the other columns.

.. code-block:: sh

    curl -X POST -d "dataset_id=8123&other_dataset_id=9123&on=food_type" http://bamboo.io/datasets/join

**returns:**

.. code-block:: javascript

    {"id": "8a3d74711475d8a51c84484fe73f24bd151242ea"}

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

Note that the file can also contain a single dictionary, for example:

.. code-block:: javascript

    {
        "name": "in northern hemisphere",
        "formula": "gps_latitude > 0"
    }

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
