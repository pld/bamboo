Calculations
============

Below is a list of valid calculations which can be included in a formula.

The arguments to the aggregation are column names, e.g. ``amount``, or formulas
themselves, e.g. ``risk_factor in ["low_risk"]``.

.. note::

    Calculations can also be passed into aggregations.

Arithmetic
----------

.. code-block:: sh

    radius * 2
    length ^ 3
    female_students + male_students
    gross_revenue - liabilities
    net_revenue / 12
    total_students / total_teachers

Precedence
----------

.. code-block:: sh

    (gross_revenue - liabilities) / 12

Comparison
----------

.. code-block:: sh

    female_students > male_students
    average_age == 18

Propositional Formulas
----------------------

.. code-block:: sh

    age > 17 age < 22
    not (month == 7 or month == 12)

Membership
----------

.. code-block:: sh

    risk_factor in ["low_risk", "medium_risk"]
    
Date Arithmetic
---------------

For the time being all date arithmetic is performed in seconds.

.. code-block:: sh

    date("09-04-2012") - submit_date > 21078000

Case Statements
---------------

Analogous to the case or switch statement in other programming languages.

.. code-block:: sh

    case student_teacher_ratio > 0.6: "good", student_teacher_ratio > 0.3: "moderate", default: "poor"

Transformations
---------------

Transformations are functions that convert a scalar into another scalar
based on a vector.

.. code-block:: sh

    percentile(amount)
