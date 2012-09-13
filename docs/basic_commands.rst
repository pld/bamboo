Basic Commands
==============

Check the Bamboo version:
-------------------------

``curl http://bamboo.io/version``

Storing data in Bamboo:
-----------------------

upload data from a URL to Bamboo:
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``curl -X POST -d "url=http://formhub.org/mberg/forms/good_eats/data.csv" http://bamboo.io/datasets``

returns::

    {
        "id": "8a3d74711475d8a51c84484fe73f24bd151242ea"
    }

upload data from a file to Bamboo:
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

given the file ``/home/modilabs/good_eats.csv`` exists locally on your
filesystem

``curl -X POST -F csv_file=@/home/modilabs/good_eats.csv http://bamboo.io/datasets``

returns::

    {
        "id": "8a3d74711475d8a51c84484fe73f24bd151242ea"
    }

Retrieve information about a dataset:
-------------------------------------

given the id is ``8a3d74711475d8a51c84484fe73f24bd151242ea``

``curl http://bamboo.io/datasets/8a3d74711475d8a51c84484fe73f24bd151242ea/info``

returns::

    {
        "id": "8a3d74711475d8a51c84484fe73f24bd151242ea",
        "schema": {
            "food_type": {
                "label": "Amount",
                "olap_type": "measure",
                "simpletype": "float"
            },
            "rating": {
                "label": "Rating",
                "olap_type": "dimension",
                "simpletype": "string"
            },
            "food_type": {
                "label": "Food Type",
                "olap_type": "dimension",
                "simpletype": "string"
            },
            ...
        },
        "created_at": "2012-6-18 14:43:32",
        "updated_at": "2012-6-18 14:43:32",
        "num_rows": "500",
        "num_columns": "30"
    }


Retrieve data:
--------------

given the id is ``8a3d74711475d8a51c84484fe73f24bd151242ea``

by ID:
^^^^^^

``curl http://bamboo.io/datasets/8a3d74711475d8a51c84484fe73f24bd151242ea``

This returns the dataset as JSON.

returns::

    [
        {
            "rating": "delectible",
            "_percentage_complete": "n/a",
            "_xform_id_string": "good_eats",
            "risk_factor": "low_risk",
            "gps_alt": "39.5",
            "food_type": "lunch",
            ...
        },
        ...
    ]

by ID with select:
^^^^^^^^^^^^^^^^^^

``curl -g http://bamboo.io/datasets/8a3d74711475d8a51c84484fe73f24bd151242ea?select='{"rating":1}'``

This returns the dataset as JSON given the select, i.e. only the rating
column.

returns::

    [
        {"rating": "epic_eat"},
        {"rating": "delectible"},
        {"rating": "delectible"},
        {"rating": "delectible"},
        {"rating": "epic_eat"},
        {"rating": "delectible"},
        {"rating": "delectible"},
        {"rating": "delectible"},
        {"rating": "delectible"},
        {"rating": "epic_eat"}, 
        {"rating": "epic_eat"}, 
        {"rating": "epic_eat"},
        {"rating": "delectible"}, 
        {"rating": "epic_eat"}, 
        {"rating": "epic_eat"},
        {"rating": "epic_eat"}, 
        {"rating": "delectible"}, 
        {"rating": "delectible"},
        {"rating": "delectible"}, 
        {"rating": "delectible"}, 
        {"rating": "epic_eat"}
    ]


by ID and query:
^^^^^^^^^^^^^^^^

query must be valid MongoDB extended JSON

``curl -g http://bamboo.io/datasets/8a3d74711475d8a51c84484fe73f24bd151242ea?query='{"food_type":"lunch"}'``

This returns the dataset as JSON given the query, i.e. only rows with a
food_type of "lunch".

returns::

    [
        {
            "rating": "delectible",
            "location_name": "Tolga Copsis ",
            "description": "Cotsi ", "_gps_precision": "85.0",
            "submit_date": {"$date": 1325635200000}, 
            "_gps_latitude": "37.951282449999994", 
            "_gps_altitude": "0.0", 
            "submit_data": {"$date": 1325635200000}, 
            "_gps_longitude": "27.3700048", 
            "comments": "n/a", 
            "amount": 8.0, 
            "risk_factor": "low_risk", 
            "imei": 358490042584319, 
            "food_type": "lunch", 
            "gps": "37.951282449999994 27.3700048 0.0 85.0", 
            "location_photo": "1325672494341.jpg", 
            "food_photo": "1325672462974.jpg"
        }, 
        ...
    ]

Retrieve summary statistics for dataset:
----------------------------------------

by ID:
^^^^^^

``curl http://bamboo.io/datasets/8a3d74711475d8a51c84484fe73f24bd151242ea/summary``

This returns a summary of the dataset.  Columns of type float and integer are
show as summary statistics.  Columns of type string and boolean are shown as
counts of unique values.

returns::

    {
        "rating": {
            "summary": {
                "delectible": 12,
                "epic_eat": 10
            }
        },
        "amount": {
            "summary": {
                "count": 22.0,
                "std": 339.16360630207191,
                "min": 2.0,
                "max": 1600.0,
                "50%": 12.0,
                "25%": 4.6875,
                "75%": 19.5,
                "mean": 92.772727272727266
            }
        },
        ...
    }


with a grouping:
^^^^^^^^^^^^^^^^

``curl http://bamboo.io/datasets/8a3d74711475d8a51c84484fe73f24bd151242ea/summary?group=food_type``

Calculation formulas:
---------------------

Calculations are specified by a *name*, which is the label and a *formula*,
which is either calculated by row or aggregated over multiple rows.

The calculation *formula* can contain a combination of integers, floats, and/or
strings which must map to column names, as well as operators and functions
(specified in the Parser).

Calculations that are aggregations can also be specified with a *group* and a
*query*. The dataset will be grouped by the *group* parameter and limited to rows
matching the *query* parameter.

The results of aggregations are stored in a dataset with one column for
the unique groups and another for the result of the *formula*. This dataset is
indexed by the group parameter and unique per dataset ID.

store calculation formula:
^^^^^^^^^^^^^^^^^^^^^^^^^^

``curl -X POST -d "name=amount_less_than_10&formula=amount<10" http://bamboo.io/calculations/8a3d74711475d8a51c84484fe73f24bd151242ea``

retrieve newly calculated column:
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``curl -g http://bamboo.io/datasets/8a3d74711475d8a51c84484fe73f24bd151242ea?select='{"amount_less_than_10":1}'``

store aggregation formula:
^^^^^^^^^^^^^^^^^^^^^^^^^^

``curl -X POST -d "name=sum_of_amount&formula=sum(amount)" http://bamboo.io/calculations/8a3d74711475d8a51c84484fe73f24bd151242ea``

store aggregation formula with group:
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``curl -X POST -d "name=sum_of_amount&formula=sum(amount)&group=food_type" http://bamboo.io/calculations/8a3d74711475d8a51c84484fe73f24bd151242ea``

retrieve aggregated datasets:
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``curl -g http://bamboo.io/datasets/8a3d74711475d8a51c84484fe73f24bd151242ea?mode=related``

Returns a map of groups (included an empty group) to dataset IDs for
aggregation calculations.
