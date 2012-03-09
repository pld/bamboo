Installation
============

    cd virtual_environments

    virtualenv --no-site-packages bamboo

    source bamboo/bin/activate

    git clone git@github.com:pld/bamboo.git

Install the requirements:

    cd bamboo

    pip install -r requirements.pip

Running
=======

    [start mongodb on localhost and standard port]

    python bamboo.py
