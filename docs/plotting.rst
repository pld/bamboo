Plotting
--------

REST end-point:

.. code-block:: sh

    http://bamboo.io/datasets/[DATASET ID]/plot
    
View the documentation for the `query parameters <http://bamboo.io/docs/controllers.html#bamboo.controllers.datasets.Datasets.plot>`_
accepted by the ``plot`` end point.

Kenyan school teaching staff by province
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. image:: https://raw.github.com/modilabs/bamboo/master/docs/images/kenya_school_staff_by_district.png
  :target: http://bamboo.io/datasets/f4cf7dfe315146388258702bb4cc95a4/plot?select={%22total_teaching_staff%22:1}&group=province&plot_type=bar

Click for an `interactive version <http://bamboo.io/datasets/f4cf7dfe315146388258702bb4cc95a4/plot?select={%22total_teaching_staff%22:1}&group=province&plot_type=bar>`_.

.. note::

    *Chart parameters*

    * **select** = ``{"total_teaching_staff":1}``

      * Choose only the column ``total_teaching_staff``.

    * **group** = ``province``

      * Group on the dimensional column ``province``.

    * **plot_type** = ``bar``

      * Format the plot as a bar plot.

Meal cost grouped by rating over time
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. image:: https://raw.github.com/modilabs/bamboo/master/docs/images/good_eats_amount_by_rating.png
  :target: http://bamboo.io/datasets/1c0461bdd4eb486ebe5f6f5f3a179790/plot?select={%22amount%22:1}&group=food_type&index=submit_date&query={%22amount%22:{%22$lt%22:400},%22submit_date%22:{%22$gt%22:1355011200}}&plot_type=area
  

Click for an `interactive version <http://bamboo.io/datasets/1c0461bdd4eb486ebe5f6f5f3a179790/plot?select={%22amount%22:1}&group=food_type&index=submit_date&query={%22amount%22:{%22$lt%22:400},%22submit_date%22:{%22$gt%22:"2012-12-09"}}&plot_type=area>`_.

.. note::

    *Chart parameters*

    * **select** = ``{"amount":1}``

      * Choose only the column ``amount``.

    * **group** = ``food_type``

      * Group on the dimensional column ``food_type``.

    * **index** = ``submit_date``

      * Set the date column ``submit_date`` as the temporal index column.

    * **query** = ``{"amount": {"$lt": 400}, "submit_date": {"$gt": "2012-12-09"}}``

      * Restrict the data to dates after December 9th 2012, and to amounts less
        than 400.

    * **plot_type** = ``area``

      * Format the plot as an area plot.
