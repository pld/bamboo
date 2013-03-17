Manipulating Data
=================

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

Modifying your data
-------------------

You can show, edit, and delete a single row in your dataset using the ``row``
endpoint.  To interact with the row endpoint you specify the ``dataset_id`` and
the ``index`` of the row you would like to interact with.

The indices of your dataset can be retrieved by passing ``index=True`` to the
show dataset command.

.. note::

    Dataset indices begin with 0, i.e. the first row will be at index 0.

Retrieving a row by index
^^^^^^^^^^^^^^^^^^^^^^^^^

To retrieve the first row of your dataset:

.. code-block:: sh

    curl http://bamboo.io/datasets/8a3d74711475d8a51c84484fe73f24bd151242ea/row/0

**returns:**

.. code-block:: javascript

    {
        "rating": "delectible",
        "_percentage_complete": "n/a",
        "_xform_id_string": "good_eats",
        "risk_factor": "low_risk",
        "gps_alt": "39.5",
        "food_type": "lunch",
        ...
    }

Deleting a row by index
^^^^^^^^^^^^^^^^^^^^^^^

To delete the first row of your dataset:

.. code-block:: sh

    curl -X DELETE http://bamboo.io/datasets/8a3d74711475d8a51c84484fe73f24bd151242ea/row/0

**returns:**

.. code-block:: javascript

    {
        "success": "Deleted row with index '0'.",
        "id": "8a3d74711475d8a51c84484fe73f24bd151242ea"
    }

Editing a row by index
^^^^^^^^^^^^^^^^^^^^^^

To set the `amount` to `5.0` for the first row of your dataset:

.. code-block:: sh

    curl -X PUT -d 'data={"amount":"5.0"}' http://bamboo.io/datasets/8a3d74711475d8a51c84484fe73f24bd151242ea/row/0

**returns:**

.. code-block:: javascript

    {
        "success": "Updated row with index '0'.",
        "id": "8a3d74711475d8a51c84484fe73f24bd151242ea"
    }

Merging multiple Datasets
-------------------------

To row-wise merge 2 or more datasets into a new dataset use the _merge_ command
. For example, to merge two datasets with IDs 8a3d7 and 2de98,
use the following command. This will return the ID of the new merged dataset.
The merge occurs in the background.  When the dataset status is set to "ready"
you can be sure the data has been merged.

.. code-block:: sh

    curl -X POST -d "dataset_ids=[\"8a3d7\", \"2de98\"]" http://bamboo.io/datasets/merge

**returns:**

.. code-block:: javascript

    {"id": "8a3d74711475d8a51c84484fe73f24bd151242ea"}

Merge with a column mapping
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Merge takes an optional ``mapping`` parameter, which maps columns in the original
dataset to columns in the new merged dataset.

For example, a JSON mapping of:

.. code-block:: javascript

    {
        "8a3d7": {"foodtype": "food_type"},
        "2de98": {"foodType": "food_type"}
    }

will map the "foodtype" column from dataset ID 8a3d7 and the "foodType" column
from dataset 2de98 to a canonical "food_type" column in the merged dataset.

.. code-block:: sh

    curl -X POST -d "datasets=[8a3d7,2de98]&mapping={\"8a3d7\":{\"foodtype\":\"food_type\"},"2de98":{\"foodType\":\"food_type\"}}" http://bamboo.io/datasets/merge

**returns:**

.. code-block:: javascript

    {"id": "8a3d74711475d8a51c84484fe73f24bd151242ea"}

Joining multiple Datasets
-------------------------

You can perform column joins between multiple datasets.  The column that is
joined on must be unique in the right hand side dataset and must exist in both
datasets. The left hand side dataset ID is specified in the URL, and the right
hand side is passed as the *other_dataset_id* parameter.

If the join succeeds the ID of the new dataset is returned as JSON. If the join
fails an error message is returned, also as JSON.

Updates which are subsequently made to either the left hand side or the right
hand side dataset will be propagated to the joined dataset. Updates to the
right hand side that make the join column non-unique will be disallowed.

For example supposing the 'food_type' column is in datasets with IDs 8a3d7 and
2de98, you can join the two datasets on that column by executing:

.. note::

    * You can not join datasets which have overlapping columns.
    * After you have joined two datasets you can not update the right hand
      (``other_dataset_id``) with data that will make its ``on`` column
      non-unique.
    * Updates to the left hand side (``dataset_id``) will be merged with right
      hand side columns if the update matches an existing ``on`` column,
      otherwise if will have NaN for the other columns.

.. code-block:: sh

    curl -X POST -d "dataset_id=8a3d7&other_dataset_id=2de98&on=food_type" http://bamboo.io/datasets/join

**returns:**

.. code-block:: javascript

    {"id": "8a3d74711475d8a51c84484fe73f24bd151242ea"}

Joining on different columns
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To join column ``A`` in the left hand side dataset with column ``B`` in the
right hand side dataset, set the ``on`` parameter to ``A,B``.

Continuing the above example, suppose dataset 8a3d7 has a "food_type" column
but 2de98 has a "foodtype" column, use the command:

.. code-block:: sh

    curl -X POST -d "dataset_id=8a3d7&other_dataset_id=2de98&on=food_type,foodtype" http://bamboo.io/datasets/join

