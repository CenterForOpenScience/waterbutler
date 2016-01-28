Getting Started
===============

Setting Up
----------

Make sure that you are using >= python3.3 and install invoke for your current python3 version.

.. code-block:: bash

    pip install invoke

Install requirements

.. code-block:: bash

    invoke install

Or for some nicities (like tests)

.. code-block:: bash

    invoke install --develop


Start the server

.. note

    The server is extremely tenacious thanks to stevedore and tornado
    Syntax errors in the :mod:`waterbutler.providers` will not crash the server
    In debug mode the server will automatically reload

.. code-block:: bash

    invoke server

Contributing
------------

See https://github.com/CenterForOpenScience/waterbutler/blob/develop/CONTRIBUTING.md.

Running Tests
-------------

Make sure that you already have dev-requirements

.. code-block:: bash

    invoke test
