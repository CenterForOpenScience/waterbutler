Overview
========

WaterButler is a Python web application for interacting with various file storage services via a single RESTful API.

Authentication to WB can be done with HTTP Basic Authorization or via cookies.

Request lifecycle: User makes request, credentials are requested from auth provider, request is made to storage provider, response is returned to user::

  User                   WaterButler                                    OSF    Provider
   |                          |                                          |        |
   | -- request resource ---> |                                          |        |
   |                          |                                          |        |
   |                          | --- request credentials for provider --> |        |
   |                          |                                          |        |
   |                          | <-- return credentials, if valid user -- |        |
   |                          |                                                   |
   |                          | ----- relay user request to provider w/ creds --> |
   |                          |                                                   |
   |                          | <---- return request response to WB ------------- |
   |                          |
   | <-- provider response -- |

If the user is interacting with WaterButler via the OSF, the diagram looks like this::

  User                       OSF                               WaterButler                     Provider
   |                          |                                     |                             |
   | -- request resource ---> |                                     |                             |
   |                          |                                     |                             |
   |                          | --- relay user req. --------------> |                             |
   |                          |                                     |                             |
   |                          | <-- request creds for provider ---- |                             |
   |                          |                                     |                             |
   |                          | --- return creds, if valid user --> |                             |
   |                          |                                     |                             |
   |                          |                                     | --- relay req. w/ creds --> |
   |                          |                                     |                             |
   |                          |                                     | <-- return resp. ---------- |
   |                          |                                     |
   |                          | <-- relay provider resp. to OSF --- |
   |                          |
   | <-- provider response -- |


Only one auth provider so far, the OSF.

Two APIs, v0 and v1.  v0 is deprecated.


Terminology
-----------

**auth provider** - The service that provides the authentication needed to communicate with the storage provider.  WaterButler currently only supports the `Open Science Framework <https://osf.io/>`_  as an auth provider.

**storage provider** - The external service being connected to. ex. Google Drive, GitHub, Dropbox.

**provider** - When we refer to a *provider* without specifying which type, we are talking about a *storage provider*.

**resource** - The parent resource the provider is connected to.  This will depend on the auth provider.  For the OSF, the resource is the GUID of the project that the provider is connected to.  For example, the OSF project for the `Reproducibility Project: Psychology <https://osf.io/ezcuj/>`_ is found at https://osf.io/ezcuj/.  The *resource* in this case is `ezcuj`.  When a request is made to WaterButler for something under the `ezcuj` resource, a query will be sent to the OSF to make sure the authenticated user has permission to access the provider linked to that project.
