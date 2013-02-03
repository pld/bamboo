Contributing Code
=================

To work on the code:

1. fork `bamboo on github <https://github.com/modilabs/bamboo>`_
2. add tests for your new feature
3. add the code for your new feature
4. ensure it is pep8

    .. code-block:: sh

        $ pip install pep8
        $ pep8 bamboo

5. ensure all existing tests and your new tests are passing

    .. code-block:: sh

        $ cd bamboo
        $ ../scripts/test.sh

6. submit a pull request

Testing
-------

1. install nose testing requirements

    .. code-block:: sh

        $ pip install -r requirements-test.pip

2. run tests

    .. code-block:: sh

        $ cd bamboo
        $ ../scripts/test.sh

3. or run the profiler

    .. code-block:: sh

        $ cd bamboo
        $ ../scripts/test.sh -p

Documentation
-------------
      
Building Documentation
^^^^^^^^^^^^^^^^^^^^^^

Install graphviz for class structure diagrams:

on Arch Linux: ``# pacman -S graphviz``

on Debian based: ``# apt-get install graphviz``

.. code-block:: sh

    $ pip install -r requirements-docs.pip
    $ cd docs
    $ make html
