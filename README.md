<img src=/docs/waterbutler.png?raw=true" width="25%" style="float:left;">
# WaterButler

[![Documentation Status](https://readthedocs.org/projects/waterbutler/badge/?version=latest)](http://waterbutler.readthedocs.org/en/latest/?badge=latest)
[![Code Climate](https://codeclimate.com/github/CenterForOpenScience/waterbutler/badges/gpa.svg)](https://codeclimate.com/github/CenterForOpenScience/waterbutler)

`master` Build Status: [![Build Status](https://travis-ci.org/CenterForOpenScience/waterbutler.svg?branch=master)](https://travis-ci.org/CenterForOpenScience/waterbutler)

`develop` Build Status: [![Build Status](https://travis-ci.org/CenterForOpenScience/waterbutler.svg?branch=develop)](https://travis-ci.org/CenterForOpenScience/waterbutler)

Docs can be found [here](https://waterbutler.readthedocs.org/en/latest/)

### Requirements

In order to run waterbutler, you must have a virtualenv created for waterbutler running python 3.5 .

For MacOSX, you can install the latest version of python3 using:

```bash
brew install python3
```

For Ubuntu users:

```bash
apt-get install python3.5

```

### startup commands

After completing the installation of Python 3.5, you must create the virtual environment, this can be done with the following commands:

```bash
pip install virtualenv
pip install virtualenvwrapper
mkvirtualenv --python=python3.5 waterbutler
pip install invoke
invoke install
invoke server
```

The above code will get the virtualenv up and running for the first time.  After the initial setup, you can run waterbutler by running:

```bash
workon waterbutler
invoke server
```

### testing configuration (optional)

```bash
vim ~/.cos/waterbutler-test.json
```

Python 3.5's asyncio [currently has a bug](https://bugs.python.org/issue25394) that is triggered by turning on debugging. If you are upgrading from the 3.4-based WaterButler, make sure to set `SERVER_CONFIG.DEBUG` to false in your `waterbutler-test.json`, e.g.

```json
{
  "OSFSTORAGE_PROVIDER_CONFIG": {
    "HMAC_SECRET": "changeme"
  },
  "SERVER_CONFIG": {
    "ADDRESS": "localhost",
    "PORT": 7777,
    "DOMAIN": "http://localhost:7777",
    "DEBUG": false,
    "HMAC_SECRET": "changeme"
  },
  "OSF_AUTH_CONFIG": {
      "API_URL": "http://localhost:5000/api/v1/files/auth/"
  }
}
```

### running the tests (optional)
To run all the tests you will need install some requirements, so try running:

```bash
workon waterbutler
invoke install --develop
invoke test
```
