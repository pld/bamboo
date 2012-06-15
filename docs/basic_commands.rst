Basic Commands
==============

Check the Bamboo version
------------------------

``curl http://bamboo.io/version``

Storing data in Bamboo
----------------------

upload data from a URL to Bamboo:
^^^^^^^^^^^^^^^^^^^^^^

``curl -X POST -d "url=http://formhub.org/mberg/forms/good_eats/data.csv" http://bamboo.io/datasets``

upload data from a file to Bamboo:
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

given the file ``/home/modilabs/good_eats.csv`` exists locally on your
filesystem

``curl -X POST -F csv_file=@/home/modilabs/good_eats.csv http://bamboo.io/datasets``


Retrieve data
-------------

by ID:
^^^^^^^^^^^^^^^^^^^^^^

given the id is ``8a3d74711475d8a51c84484fe73f24bd151242ea``

``curl http://bamboo.io/datasets/8a3d74711475d8a51c84484fe73f24bd151242ea``

by ID with select:
^^^^^^^^^^^^^^^^^^

``curl -g http://bamboo.io/datasets/8a3d74711475d8a51c84484fe73f24bd151242ea?select='{"rating":1}'``


by ID and query:
^^^^^^^^^^^^^^^^

query must be valid MongoDB extended JSON

``curl -g http://bamboo.io/datasets/8a3d74711475d8a51c84484fe73f24bd151242ea?query='{"food_type":"lunch"}'``

Retrieve summary statistics for dataset
---------------------------------------

by ID:
^^^^^^

``curl http://bamboo.io/datasets/8a3d74711475d8a51c84484fe73f24bd151242ea/summary``

by ID and query:
^^^^^^^^^^^^^^^^

``curl -g http://bamboo.io/datasets/8a3d74711475d8a51c84484fe73f24bd151242ea/summary?query='{"food_type":"lunch"}'``

with a grouping:
^^^^^^^^^^^^^^^^

``curl http://bamboo.io/datasets/8a3d74711475d8a51c84484fe73f24bd151242ea/summary?group=food_type``

Calculation formulas
--------------------

store calculation formula:
^^^^^^^^^^^^^^^^^^^^^^^^^^

``curl -X POST -d "name=amount_less_than_10&formula=amount<10" http://bamboo.io/calculations/8a3d74711475d8a51c84484fe73f24bd151242ea``

retrieve newly calculated column:
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
``curl -g http://bamboo.io/datasets/8a3d74711475d8a51c84484fe73f24bd151242ea?select='{"amount_less_than_10":1}'``
