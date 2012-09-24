Basic Commands
==============

Note: on the command line some special characters may need to be escaped for
the commands to function correctly.  E.g. ``&`` as ``\&``, ``?`` as ``\?``,
``=`` as ``\=``.

Check the Bamboo version
------------------------

``curl http://bamboo.io/version``

Storing data in Bamboo
----------------------

upload data from a URL to Bamboo
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``curl -X POST -d "url=http://formhub.org/mberg/forms/good_eats/data.csv" http://bamboo.io/datasets``

returns::

    {
        "id": "8a3d74711475d8a51c84484fe73f24bd151242ea"
    }

upload data from a file to Bamboo
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

given the file ``/home/modilabs/good_eats.csv`` exists locally on your
filesystem

``curl -X POST -F csv_file=@/home/modilabs/good_eats.csv http://bamboo.io/datasets``

returns::

    {
        "id": "8a3d74711475d8a51c84484fe73f24bd151242ea"
    }

Retrieve information about a dataset
------------------------------------

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
        "num_columns": "30"
    }


Retrieve data
-------------

given the id is ``8a3d74711475d8a51c84484fe73f24bd151242ea``

by ID
^^^^^

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

by ID with select
^^^^^^^^^^^^^^^^^

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


by ID and query
^^^^^^^^^^^^^^^

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

query with dates
^^^^^^^^^^^^^^^^

To query with dates use the MongoDB query format and specify dates as Unix
epochs.

``curl -g http://bamboo.io/datasets/8a3d74711475d8a51c84484fe73f24bd151242ea?query='{"submit_date": {"$lt": 1320000000}'``

Returns the rows with a time stamp less than 1320000000, which is October 30th
2011.

Retrieve summary statistics for dataset
---------------------------------------

by ID
^^^^^

``curl http://bamboo.io/datasets/8a3d74711475d8a51c84484fe73f24bd151242ea/summary?select=all``

This returns a summary of the dataset.  Columns of type float and integer are
show as summary statistics.  Columns of type string and boolean are shown as
counts of unique values.

The select argument is required.  It can either be ``all`` or a MongoDB JSON
select query.

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

with a query
^^^^^^^^^^^^^

``curl -g http://bamboo.io/datasets/8a3d74711475d8a51c84484fe73f24bd151242ea/summary?query='{"food_type": "lunch"}'&select=all``

Return the summary restricting to data that matches the Mongo query passed as
*query*.

returns::

    {
        "rating": {
            "summary": {
                "delectible": 5,
                "epic_eat": 2
            }
        },
        "amount": {
            "summary": {
                "count": 7.0,
                "std": 71.321017238959797,
                "min": 4.25,
                "max": 200.0,
                "50%": 12.0,
                "25%": 8.5,
                "75%": 19.0,
                "mean": 38.75
            }
        },
        "risk_factor": {
            "summary": {
                "low_risk": 7
            }
        },
        "food_type": {
            "summary": {
                "lunch": 7
            }
        },
        ...
    }

with a grouping
^^^^^^^^^^^^^^^

``curl http://bamboo.io/datasets/8a3d74711475d8a51c84484fe73f24bd151242ea/summary?select=all&group=food_type``

Return the summary grouping on the value passed as *group*.

returns::

    {
        "food_type": {
            "caffeination": {
                "rating": {
                    "summary": {
                        "epic_eat": 1
                     }
                },
                "description": {
                    "summary": {
                        "Turkish coffee": 1
                    }
                },
                "amount": {
                    "summary": {
                        "count": 1.0, 
                        "std": "null", 
                        "min": 2.5, 
                        "max": 2.5, 
                        "50%": 2.5, 
                        "25%": 2.5, 
                        "75%": 2.5, 
                        "mean": 2.5
                    }
                }, 
                "risk_factor": {
                    "summary": {
                        "low_risk": 1
                    }
                },
                ...
            "deserts": {
                "rating": {
                    "summary": {
                        "epic_eat": 2
                    }
                }, 
                "description": {
                    "summary": {
                        "Baklava": 1,
                        "Rice Pudding ": 1
                    }
                },
                "amount": {
                    "summary": {
                        "count": 2.0,
                        "std": 2.2980970388562794, 
                        "min": 2.75,
                        "max": 6.0,
                        "50%": 4.375,
                        "25%": 3.5625,
                        "75%": 5.1875,
                        "mean": 4.375
                    }
                },
                "risk_factor": {
                    "summary": {
                        "low_risk": 2
                    }
                },
                ...
            }
            ...
        }
    }

with a grouping and a select
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``curl -g http://bamboo.io/datasets/8a3d74711475d8a51c84484fe73f24bd151242ea/summary?select='{"rating":1}'&group=food_type``

Return the summary grouping on the value passed as *group* and only showing the
columns specified by the *select*.

returns::

    {
        "food_type": {
            "caffeination": {
                "rating": {
                    "summary": {
                        "epic_eat": 1
                    }
                }
            },
            "deserts": {
                "rating": {
                    "summary": {
                        "epic_eat": 2
                    }
                }
            },
            ...
        }
    }

with a multi-grouping
^^^^^^^^^^^^^^^^^^^^^

``curl http://bamboo.io/datasets/8a3d74711475d8a51c84484fe73f24bd151242ea/summary?select=all&group=food_type,rating``

returns::

    {
        "food_type,rating": {
            "(u'dinner', u'delectible')": { 
                "rating": {
                    "summary": {
                        "delectible": 2
                    }
                },
                "amount": {
                    "summary": {
                        "count": 2.0,
                        "std": 1.4142135623730951,
                        "min": 12.0,
                        "max": 14.0,
                        "50%": 13.0,
                        "25%": 12.5,
                        "75%": 13.5,
                        "mean": 13.0
                    }
                },
                "risk_factor": {
                    "summary": {
                        "low_risk": 2
                    }
                },
                "food_type": {
                    "summary": {
                        "dinner": 2
                    }
                },
                ...
            }
            "(u'deserts', u'epic_eat')": {
                "rating": {
                    "summary": {
                        "epic_eat": 2
                    }
                }, 
                "amount": {
                    "summary": {
                        "count": 2.0,
                        "std": 2.2980970388562794,
                        "min": 2.75,
                        "max": 6.0,
                        "50%": 4.375,
                        "25%": 3.5625,
                        "75%": 5.1875,
                        "mean": 4.375
                    }
                },
                "risk_factor": {
                    "summary": {
                        "low_risk": 2
                    }
                }, 
                "food_type": {
                    "summary": {
                        "deserts": 2
                    }
                }, 
                ...
            }
            ...
        }
    }


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

returns::

    {
        "formula": "amount<10",
        "group": null,
        "name": "amount_less_than_10"
    }

retrieve newly calculated column:
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``curl -g http://bamboo.io/datasets/8a3d74711475d8a51c84484fe73f24bd151242ea?select='{"amount_less_than_10":1}'``

returns::

    [
        {"amount_less_than_10": true},
        {"amount_less_than_10": false},
        {"amount_less_than_10": false},
        {"amount_less_than_10": true},
        {"amount_less_than_10": true},
        {"amount_less_than_10": true},
        {"amount_less_than_10": true},
        {"amount_less_than_10": false},
        {"amount_less_than_10": true},
        {"amount_less_than_10": false},
        {"amount_less_than_10": false},
        {"amount_less_than_10": false},
        {"amount_less_than_10": true},
        {"amount_less_than_10": false},
        {"amount_less_than_10": false},
        {"amount_less_than_10": false},
        {"amount_less_than_10": true},
        {"amount_less_than_10": true},
        {"amount_less_than_10": false},
        {"amount_less_than_10": false},
        {"amount_less_than_10": true}
    ]

store aggregation formula:
^^^^^^^^^^^^^^^^^^^^^^^^^^

``curl -X POST -d "name=sum_of_amount&formula=sum(amount)" http://bamboo.io/calculations/8a3d74711475d8a51c84484fe73f24bd151242ea``

returns::

    {
        "formula": "sum(amount)",
        "group": null,
        "name": "sum_of_amount"
    }

store aggregation formula with group:
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``curl -X POST -d "name=sum_of_amount&formula=sum(amount)&group=food_type" http://bamboo.io/calculations/8a3d74711475d8a51c84484fe73f24bd151242ea``

returns::

    {
        "formula": "sum(amount)",
         "group": "food_type",
         "name": "sum_of_amount"
    }

store aggregation formula with multi-group:
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``curl -X POST -d "name=sum_of_amount&formula=sum(amount)&group=food_type,rating" http://bamboo.io/calculations/8a3d74711475d8a51c84484fe73f24bd151242ea``

returns::

    {
        "formula": "sum(amount)",
         "group": "food_type,rating",
         "name": "sum_of_amount"
    }

retrieve lists of related datasets for aggregations:
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``curl -g http://bamboo.io/datasets/8a3d74711475d8a51c84484fe73f24bd151242ea?mode=related``

Returns a map of groups (included an empty group) to dataset IDs for
aggregation calculations.

returns::

    {
        "": "9ae0ee32b78d445588742ac818c3d533",
        "food_type": "643eaccb31e74216bfa7c16bfb0e79e5",
        "food_type,rating": "10cedc551e40418caa72495d771703b3"
    }

retrieve the linked datasets that groups on foodtype and rating:
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``curl -g http://bamboo.io/datasets/10cedc551e40418caa72495d771703b3``

Linked dataset are the same as any other dataset.

returns::

    [
        {
            "rating": "epic_eat",
            "food_type": "deserts",
            "sum_of_amount": 8.75
        },
        {
            "rating": "delectible",
            "food_type": "dinner",
            "sum_of_amount": 26.0
        },
        {
            "rating": "epic_eat",
            "food_type": "lunch",
            "sum_of_amount": 22.25
        },
        {
            "rating": "delectible",
            "food_type": "street_meat",
            "sum_of_amount": 2.0
        },
        {
            "rating": "epic_eat",
            "food_type": "caffeination",
            "sum_of_amount": 2.5
        },
        {
            "rating": "epic_eat",
            "food_type": "dinner",
            "sum_of_amount": 1612.0
        },
        {
            "rating": "delectible",
            "food_type": "drunk_food",
            "sum_of_amount": 20.0
        },
        {
            "rating": "epic_eat",
            "food_type": "libations",
            "sum_of_amount": 9.5
        },
        {
            "rating": "delectible",
            "food_type": "lunch",
            "sum_of_amount": 249.0
        },
        {
            "rating": "delectible",
            "food_type": "morning_food",
            "sum_of_amount": 12.0
        },
        {
            "rating": "epic_eat",
            "food_type": "morning_food",
            "sum_of_amount": 28.0
        },
        {
            "rating": "delectible",
            "food_type": "streat_sweets",
            "sum_of_amount": 4.0
        }
    ]
