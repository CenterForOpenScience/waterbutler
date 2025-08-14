Getting Started
===============

Setting Up
----------

Make sure that you are using >= python3.13 and install poetry to manage dependencies.

.. code-block:: bash

    pip install poetry==2.1.2

Install requirements

.. code-block:: bash

    poetry install

Or for some nicities (like tests)

.. code-block:: bash

    poetry install --with dev


Start the server

.. note

    The server is extremely tenacious thanks to stevedore and tornado
    Syntax errors in the :mod:`waterbutler.providers` will not crash the server
    In debug mode the server will automatically reload

.. code-block:: bash

    poetry run invoke server

Start the celery worker

.. note

    You will need to have rabbitmq installed and rabbitmq-server running.

.. code-block:: bash

    poetry run invoke celery

Contributing
------------

`See CONTRIBUTING.md <https://github.com/CenterForOpenScience/waterbutler/blob/develop/CONTRIBUTING.md>`_.


Known Issues
------------

**Updated, 2018-01-02:** *WB has been updated to work with setuptools==37.0.0, as of WB release v0.37. The following issue should not happen for new installs, but may occur if you downgrade to an older version.* Running ``invoke install -d`` with setuptools v31 or greater can break WaterButler.  The symptom error message is: ``"AttributeError: module 'waterbutler' has no attribute '__version__'"``.  If you encounter this, you will need to remove the file ``waterbutler-nspkg.pth`` from your virtualenv directory, run ``pip install setuptools==30.4.0``, then re-run ``invoke install -d``.

``invoke $command`` results in ``'$command' did not receive all required positional arguments!``: this error message occurs when trying to run WaterButler v0.30.0+ with ``invoke<0.13.0``.  Run ``pip install invoke==0.13.0``, then retry your command.


Running Tests
-------------

Make sure that you already have dev-requirements

.. code-block:: bash

    poetry install --with dev
    poetry run invoke test

