Advanced Commands
=================

Note: on the command line some special characters may need to be escaped for
the commands to function correctly.  E.g. ``&`` as ``\&``, ``?`` as ``\?``,
``=`` as ``\=``.

Note: [*SIC*] all spelling errors in the example dataset.

Updating your data
------------------

You can add single or multiple row updates to a dataset via JSON data in a PUT
request to the dataset id. The row(s) should be key-value pairs where the key
is the column name. In the example that we have been using here, the dataset
could be updated with a JSON dictionary like this:

**returns:**

.. code-block:: javascript

    {
        "rating": "delectible",
        "amount": 2,
        "food_type": "streat_sweets"
    }

N/A values will be added when the dictionary does not supply a value for a
given column.


``curl -X PUT -H "Accept: application/json" -H "Content-type: application/json" -d '{"rating":"delectible","amount":2,"food_type":"streat_sweets"}' http://bamboo.io/datasets/8a3d74711475d8a51c84484fe73f24bd151242ea``

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

``curl -X POST -d "datasets=[8123, 9123]" http://bamboo.io/datasets/merge``

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

``curl -X POST -d "dataset_id=8123&other_dataset_id=9123&on=food_type" http://bamboo.io/datasets/join``

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

``curl -X POST -F schema=@/home/modilabs/good_eats.schema.json http://bamboo.io/datasets``

**returns:**

.. code-block:: javascript

    {
        "id": "8a3d74711475d8a51c84484fe73f24bd151242ea"
    }
