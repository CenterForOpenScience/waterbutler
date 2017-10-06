Testing
---------------------------

**v1 API testing with Postman**

.. _Postman: https://www.getpostman.com/

Postman_ is a popular tool for testing APIs. Postman collections and environments are available in ``tests/postman`` for testing the functionality of the v1 API. This is particularly useful for those updating old, or creating new WaterButler providers. Follow the link for instructions on installing the Postman App or its commandline counterpart Newman.

Quickstart for Newman:

``npm install -g newman``
``newman run tests/postman/collections/copy_files.json  -e copy_file_sample.json``

**Specific collection instructions**

| copy_files:
| copy_folders:
| crud_cases
|
| *copy_files, crud_cases and copy_folders can share the same setup and environment.*
| *crud_cases only requires the first PID to be valid, and does not use the second one.*
|

Setup:

1. Create two projects in OSF. Take note of their IDs. You will need the IDs for the Postman environment file.
#. Setup the provider you wish to test, in each of the two OSF projects. Provider root must be the same in both OSF projects.
#. (Optional) If you wish to use a provider, other then osfstorage, for testing inter provider copies, setup an alternative provider in each of the two OSF projects

Environment file:

1. Make a copy of ``tests/postman/environments/copy_file_sample.json`` and edit as follows.
#. Update ``PID`` and ``PID2`` values with your two project IDs.
#. Update ``provider`` value with the name of the provider you wish to test.
#. Update ``alt_provider`` value with the name of the provider you will use for inter provider copy testing.
#. Update ``basic_auth`` value with the basic auth token representing your login to OSF. This can be found using the Postman App. Open a new request, click on authorization tab, select Basic Auth in Type dropdown. Enter your login and password. Click Update Request. Click on Headers Tab. Take note of the value of Authorization header. The value you are looking for is the rest of the string after "Basic ".
#. ``protocol``, ``host`` and ``port`` can be left as is assuming you have set up your dev environment in the default manner.

Testing:

1. Import the collection you would like to run from ``tests/postman/collections`` and the environment file you just updated into the Postman App.
#. **Note:** Importing your environment may give a few errors. It is most likely fine and should still run.
#. Run the imported collections using the imported environment.
#. **Note:** A failed run may leave files and/or folders behind. You will need to manually remove these before starting another run.
