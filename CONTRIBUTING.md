# Contributing
Waterbutler uses [semantic versioning](http://semver.org/) `<major>.<minor>.<patch>`
* Patches are reserved for hotfixes only
* Minor versions are for **adding** new functionality or fields
* Minor versions **will not** contain breaking changes to the existing API
    - Any changes **must** be backwards compatible
* Major versions **may** contain breaking changes to the existing API
    - Ideally REST endpoints will be versioned, ie `/.../v<major>/...`


Waterbutler conforms to [the git flow work flow](http://nvie.com/posts/a-successful-git-branching-model/)
In brief this means
* Feature branches should be branched off of develop or a release branch
    - Before submitting a pull request re-merge the source branch
    - **Do not** merge develop if you are working of a release branch and visa versa
* Hotfixes are to be branched off master
    - Hotfix PR should be names hotfix/brief-description
        * Use `-`'s for spaces not `_`'s
        * A hotfix for an issue involving figshare metadata when empty lists are returned would be`hotfix/figshare-metadata-empty`
    - When hotfixes are merged a new branch will be created bumping the minor version ie `hotfix/0.1.3` and the other PR will be merged into it

Waterbutler expects [pretty pull request, clean commit histories and meaningful commit messages](http://justinhileman.info/article/changing-history/)
* Make sure to rebase, `git rebase -i <commitsha>`, to remove pointless commits
    - Pointless commits include but are not limited to
        * Fix flake errors
        * Fix typo
        * Fix test
        * etc
* Follow the guide lines for commit message in the above
    - Don't worry about new lines between bullet points


All Waterbutler code **must** pass [flake8 linting](https://www.python.org/dev/peps/pep-0008/)
* Max line is set to 100 characters
* Tests are not linted, but don't be terrible

Imports are should be ordered in pep8 style but ordered by line length

```python
import abc
import asyncio
import itertools
from urllib import parse

import furl
import aiohttp

from waterbutler.core import streams
from waterbutler.core import exceptions

# Not

import abc
import asyncio
import itertools
from urllib import parse

import aiohttp
import furl

from waterbutler.core import exceptions
from waterbutler.core import streams

```

Other general guide lines
* Keep it simple and readable
* Do not use synchronous 3rd party libraries
* If you don't need `**kwargs` don't use it
* Docstrings and comments make everything better
* Avoid single letter variable names outside of comprehensions
* Write tests
