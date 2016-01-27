Aggregations
============

Below is a list of valid aggregations which can be included in a formula.

The arguments to the aggregation are column names, e.g. ``amount``, or formulas
themselves, e.g. ``risk_factor in ["low_risk"]``.

``argmax(formula)``
-------------------

Calculate the row index at which the value of formula occurs.  If used with a
group by the index is relative to the ungrouped dataframe.

.. code-block:: sh

    argmax(submit_date)

``count([formula])``
--------------------

Calculate the number of rows in the dataset, or if a formula is passed, the
number of rows in which the formula is true.

.. code-block:: sh

    count()
    count(risk_factor in ["low_risk"])

``max(formula)``
----------------

.. code-block:: sh

    max(amount)

``min(formula)``
----------------

.. code-block:: sh

    min(amount)

``mean(formula)``
-----------------

.. code-block:: sh

    mean(amount)

``median(formula)``
-------------------

.. code-block:: sh

    median(amount)

``newest(index_formula, value_formula)``
----------------------------------------

Calculate the row with the newest (maximum) value of ``index_formula``
(internally using argmax) and return the value of the ``value_formula`` for
that row.

Given :math:`n` is the number of rows, :math:`x` is a vector of the calculated
index formula, and :math:`y` is a vector of the calculated value formula, this
is equivalent to:

.. math::

    newest(x, y) = y_{{\operatorname{argmax}}(x)}

.. code-block:: sh

    newest(submit_date, amount)

``pearson(formula1, formula2)``
----------------------------------------

Calculate the Pearson correlation coefficient and p-value for two columns
defined by `formula1` and `formula2`.  The p-value is stored in a column named
after the original name with the suffix "_pvalue".

For example, we may expect that the number of teachers is correlated with the
number of students:

.. code-block:: sh

    pearson(num_teachers, num_students)

``ratio(numerator_formula, denominator_formula)``
-------------------------------------------------

Calculate the ratio of the sum of values in the numerator divided by the sum of
values in the denominator, where any rows containing a missing value in the
numerator or denominator, or having a denominator of zero, are ignored.  Given
:math:`n` is the number of rows, :math:`x` is a vector of the calculated
numerator, and :math:`y` is a vector of the calculated denominator, this is
equivalent to:

.. math::
    ratio(x, y) = \sum_{i=1}^n \left\{
        \begin{array}{l l}
            0 & \quad \text{if $x_i$ or $y_i$ is NaN, or $y_i$ = 0}\\
            \frac{x_i}{y_i} & \quad \text{otherwise}
        \end{array} \right.

.. code-block:: sh

    ratio(amount, number_of_guests)
    ratio(risk_factor in ["low_risk"], risk_factor in ["low_risk", "medium_risk"])
    ratio(risk_factor in ["low_risk"], 1)

``std(formula)``
-----------------

Standard deviation:

.. code-block:: sh

    std(amount)

``sum(formula)``
----------------

.. code-block:: sh

    sum(amount)
    sum(risk_factor in ["low_risk"])

``var(formula)``
-----------------

Variance:

.. code-block:: sh

    var(amount)
