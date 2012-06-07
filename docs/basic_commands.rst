Basic Commands
==============

Storing data in Bamboo
----------------------

upload data from a URL to Bamboo:
^^^^^^^^^^^^^^^^^^^^^^

``curl -X POST -d "url=http://formhub.org/mberg/forms/good_eats/data.csv" http://localhost:8080/datasets``

upload data from a file to Bamboo:
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

given the file ``/home/modilabs/good_eats.csv`` exists locally on your
filesystem

``curl -X POST -F csv_file=@/home/modilabs/good_eats.csv http://localhost:8080/datasets``


Retrieve data 
-------------

by ID:
^^^^^^^^^^^^^^^^^^^^^^

given the id is ``8a3d74711475d8a51c84484fe73f24bd151242ea``

``curl http://localhost:8080/datasets/8a3d74711475d8a51c84484fe73f24bd151242ea``

by ID with select:
^^^^^^^^^^^^^^^^^^

``curl http://localhost:8080/datasets/8a3d74711475d8a51c84484fe73f24bd151242ea?select='{"rating": 1}'``


by ID and query:
^^^^^^^^^^^^^^^^

query must be valid MongoDB extended JSON

``curl http://localhost:8080/datasets/8a3d74711475d8a51c84484fe73f24bd151242ea?query='{"food_type": "lunch"}'``

Retrieve summary statistics for dataset
---------------------------------------

by ID:
^^^^^^

``curl http://localhost:8080/datasets/8a3d74711475d8a51c84484fe73f24bd151242ea/summary``

by ID and query:
^^^^^^^^^^^^^^^^

``curl http://localhost:8080/datasets/8a3d74711475d8a51c84484fe73f24bd151242ea/summary?query='{"food_type": "lunch"}'``

with a grouping:
^^^^^^^^^^^^^^^^

``curl http://localhost:8080/datasets/8a3d74711475d8a51c84484fe73f24bd151242ea/summary?group=food_type``

Calculation formulas
--------------------

store calculation formula:
^^^^^^^^^^^^^^^^^^^^^^^^^^

``curl http://localhost:8080/calculations/8a3d74711475d8a51c84484fe73f24bd151242ea?name=[NAME]&formula=[FORMULA]``
