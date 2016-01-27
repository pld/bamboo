Class Structure
===============

Core
----

Aggregations
^^^^^^^^^^^^

All aggregations inherit from `Aggregations` and by default they return a
single column.  Those that return multiple columns inherit from the aggrgation
`MultiColumnAggregation`.

.. inheritance-diagram:: bamboo.core.aggregations

Frame
^^^^^

`BambooFrame` inherits from Pandas' `DataFrame`.

.. inheritance-diagram:: bamboo.core.frame

Operations
^^^^^^^^^^

All operations inherit from `EvalTerm`.  Those that return an arithmatic result
and take two operands inherit from the operation `EvalBinaryArithOp`. Those
that return a boolean result and take two operands inherit from the operation
`EvalBinaryBooleanOp`.

.. inheritance-diagram:: bamboo.core.operations

Controllers
-----------

All controllers inherit from `AbstractController`.

.. inheritance-diagram:: bamboo.controllers.calculations
.. inheritance-diagram:: bamboo.controllers.datasets
.. inheritance-diagram:: bamboo.controllers.version

Models
------

All models inherit from `AbstractModel`.

.. inheritance-diagram:: bamboo.models.calculation
.. inheritance-diagram:: bamboo.models.dataset
.. inheritance-diagram:: bamboo.models.observation
