API
===

v0 API
------

.. warning::

   The v0 WaterButler API is deprecated and should no longer be used.  It is only documented to provide a reference for legacy consumers.

TODO: v0 api docs

v1 API
------

The version 1 WaterButler API tries to conform to RESTful principals. A v1 url takes the form:

::

    http://files.osf.io/v1/resources/<node_id>/providers/<provider_id>/<id_or_path>

    e.g. http://files.osf.io/v1/resources/jy4bd/providers/osfstorage/523402a0234

Here ``jy4bd`` is the id of an OSF project, ``osfstorage`` is the provider, and ``523402a0234`` is the identifier of a particular file.

Conventions
-----------

**Trailing slashes are significant.**  When ``<id_or_path>`` refers to a folder, it must *always* have a trailing slash.  If it's a file, it must *never* have a trailing slash.  Some providers allow files and folders to have the same name within a directory.  The slash indicates user intention.  This is true even for providers that use IDs.  If the request URL contains the ID ``1a23490d777c/`` but ``1a23490d777c`` refers to a file, WB will return a 404 Not Found.

**Create returns 201, Update returns 200, Move/Copy returns depends.**  A successful file or folder creation operations should always return a 201 Created status (or 202 Accepted).  A successful update/rename operation should always return a 200 Updated.  Move / copy should return 200 if overwriting a file at the destination path, otherwise it should return 200.

Actions
-------

The links property of the response provides endpoints for common file operations. The currently-supported actions are:

**Get Info (files, folders)**

::

    Method:   GET
    Params:   ?meta=
    Success:  200 OK + file representation
    Example:  GET /resources/mst3k/providers/osfstorage/?meta=

The contents of a folder or details of a particular file can be retrieved by performing a GET request against the entity's URL with the ``meta=`` query parameter appended.  The response will be a JSON-API formatted response.

**Download (files)**

::

    Method:   GET
    Params:   <none>
    Success:  200 OK + file body
    Example:  GET /resources/mst3k/providers/osfstorage/2348825492342

To download a file, issue a GET request against its URL. The response will have the Content-Disposition header set, which will will trigger a download in a browser.

**Download Zip Archive (folders)**

::

    Method:   GET
    Params:   <none>
    Success:  200 OK + folder body
    Example:  GET /resources/mst3k/providers/osfstorage/23488254123123/?zip=

To download a zip archive of a folder, issue a GET request against its URL. The response will have the Content-Disposition header set, which will will trigger a download in a browser.

**Create Subfolder (folders)**

::

    Method:       PUT
    Query Params: ?kind=folder&name={new_folder_name}
    Body:         <empty>
    Success:      201 Created + new folder representation
    Example:      PUT /resources/mst3k/providers/osfstorage/?kind=folder&name=foo-folder

You can create a subfolder of an existing folder by issuing a PUT request against the new_folder link. The `?kind=folder` portion of the query parameter is already included in the new_folder link. The name of the new subfolder should be provided in the name query parameter. The response will contain a WaterButler folder entity. If a folder with that name already exists in the parent directory, the server will return a 409 Conflict error response.

**Upload New File (folders)**

::

    Method:       PUT
    Query Params: ?kind=file&name={new_file_name}
    Body (Raw):   <file data (not form-encoded)>
    Success:      201 Created + new file representation
    Example:      PUT /resources/mst3k/providers/osfstorage/?kind=file&name=foo-file

To upload a file to a folder, issue a PUT request to the folder's upload link with the raw file data in the request body, and the kind and name query parameters set to 'file' and the desired name of the file. The response will contain a WaterButler file entity that describes the new file. If a file with the same name already exists in the folder, the server will return a 409 Conflict error response.  The file may be updated via the url in the create response's `/links/upload` attribute.

**Update Existing File (file)**

::

    Method:       PUT
    Query Params: ?kind=file
    Body (Raw):   <file data (not form-encoded)>
    Success:      200 OK + updated file representation
    Example:      PUT /resources/mst3k/providers/osfstorage/2348825492342?kind=file

To update an existing file, issue a PUT request to the file's upload link with the raw file data in the request body and the kind query parameter set to "file". The update action will create a new version of the file. The response will contain a WaterButler file entity that describes the updated file.

**Rename (files, folders)**

::

    Method:        POST
    Query Params:  <none>
    Body (JSON):   {
                    "action": "rename",
                    "rename": {new_file_name}
                   }
    Success:       200 OK + new entity representation

To rename a file or folder, issue a POST request to the move link with the action body parameter set to "rename" and the rename body parameter set to the desired name. The response will contain either a folder entity or file entity with the new name.

**Move & Copy (files, folders)**

::

    Method:        POST
    Query Params:  <none>
    Body (JSON):   {
                    // mandatory
                    "action":   "move"|"copy",
                    "path":     {path_attribute_of_target_folder},
                    // optional
                    "rename":   {new_name},
                    "conflict": "replace"|"keep"|"warn", // defaults to 'warn'
                    "resource": {node_id},               // defaults to current {node_id}
                    "provider": {provider}               // defaults to current {provider}
                   }
    Success:       200 OK or 201 Created + new entity representation

Move and copy actions both use the same request structure, a POST to the move url, but with different values for the action body parameters. The path parameter is also required and should be the OSF path attribute of the folder being written to. The rename and conflict parameters are optional. If you wish to change the name of the file or folder at its destination, set the rename parameter to the new name. The conflict param governs how name clashes are resolved. Possible values are ``replace``, ``keep``, and ``warn``. ``warn`` is the default and will cause WaterButler to throw a 409 Conflict error if the file that already exists in the target folder. ``replace`` will tell WaterButler to overwrite the existing file, if present. ``keep`` will attempt to keep both by adding a suffix to the new file's name until it no longer conflicts. The suffix will be ' (x)' where x is a increasing integer starting from 1. This behavior is intended to mimic that of the OS X Finder. The response will contain either a folder entity or file entity with the new name.

Files and folders can also be moved between nodes and providers. The resource parameter is the id of the node under which the file/folder should be moved. It must agree with the path parameter, that is the path must identify a valid folder under the node identified by resource. Likewise, the provider parameter may be used to move the file/folder to another storage provider, but both the resource and path parameters must belong to a node and folder already extant on that provider. Both resource and provider default to the current node and providers.

If a moved/copied file is overwriting an existing file, a 200 OK response will be returned. Otherwise, a 201 Created will be returned.

**Delete (file, folders)**

::

    Method:        DELETE
    Query Params:  ?confirm_delete=1 // required for root folder delete only
    Success:       204 No Content

To delete a file or folder send a DELETE request to the delete link. Nothing will be returned in the response body. As a precaution against inadvertantly deleting the root folder, the query parameter ``confirm_delete`` must be set to ``1`` for root folder deletes. In addition, a root folder delete does not actually delete the root folder. Instead it deletes all contents of the folder, but not the folder itself.

======================
Magic Query Parameters
======================


========================
Provider Handler Params
========================

These are the query parameters that apply to all providers. Along with the request method, this is
the where Waterbutler gets the information it needs to know what operation to perform, whether to
upload, download, move, rename .etc.

meta
----

Indicates that WB should return metadata about the files (not folders) and not download the contents.

**Type**: flag

**Expected on**: GET requests against file paths

**Interactions**:

* revisions / versions: meta takes precedence.  File metadata is returned, not a revision list.

* revision / version: These are honored and passed on the the metadata method.  version takes precendence over revision.

**Notes**: The meta query param is not required to fetch folder metadata; a bare GET folder request suffices. To download a folder a zip param should be included.


zip
---
This parameter tells Waterbutler to download a folder's content into a zip for download.

**Type**: flag

**Expected on**: GET requests against folder paths

**Interactions**:

* All other query parameters will be ignored.

**Notes**: A GET request folder with no query params will return metadata, but the same request on a file will download it.


kind
----
This param is used on uploads and indicates whether an item to upload is a file or a folder.

**Type**: string ('file' or 'folder') defaults to file.

**Expected on**: PUT requests

**Interactions**:

* validated by use of the trailing slash, Example ``/folder/item`` is a file, ``/folder/item/`` is a folder

name
----
This param indicates the name of a folder to be created or file to be uploaded

**Type**: string

**Expected on**: PUT requests for folders

**Purpose**:

**Interactions**:

* None

**Notes**: Only applies to new files/folders renaming names are transmitted through the request body, not query params.


revisions / versions
--------------------
This indicates the user wants a a list of all available metadata for file revisions.

**Type**: flag

**Expected on**: GET for file paths

**Interactions**:

* is overridden by the 'meta' query param, but shouldn't be used with other params.

**Notes**:

* Revision and version can be used interchangeably, comments within the code indicate version is preferred, but no reason is supplied.
* Note the pluralization.


revision / version
------------------
This is the id of the version or revision of the file or folder which Waterbuter is to return.

**Type**: int

**Expected on**: GET or HEAD requests for files or folders

**Interactions**:

* is used as a parameter of the metadata provider function.

**Notes**:

* Revision and version can be used interchangeably, comments within the code indicate version is preferred, but no reason is supplied.
* Note the lack of pluralization.


direct
------
Issuing a download request with a query parameter named direct indicates that WB should handle the download, even if a redirect would be possible (e.g. osfstorage and s3). In this case, WB will act as a middleman, downloading the data from the provider and passing through to the requestor.

**Type**: flag

**Expected on**: GET  file paths

**Interactions**:

* This is parameter of the download provider method.

**Notes**:

* Not used for all providers, currently only used for OwnCloud, Cloudfiles and S3


displayName
-----------
Gives the name of a file being downloaded

**Type**: string

**Expected on**: GET file paths

**Interactions**:
* Is used both in handler and in S3 provider.
* Overrides path.name when not null.

**Notes**:

* Currently only useful for S3
* May want to depreciate soon.


mode
----
Indicates if a file is being downloaded to be rendered. Outside OSF's MFR this isn't useful.

**Type**: string

**Expected on**: GET file paths

**Interactions**:

* Is only used for the osfstorage provider.

**Notes**:

* currently only used with MFR.


confirm_delete
--------------
Certain providers; Figshare, Dropbox, Box, Github, S3, Google Drive and osfstorage need to include the parameter confirm_delete as equal 1 in order to delete a root folder. This is done to prevent inadvertant root deletion.

**Type**: bool

**Expected on**: DELETE for a root folder



Auth Handler Params
===================
These query params are used to decide autorization and permissions for the user.

cookie
------
This gives Waterbutler the user's credentials.

**Type**: string

**Expected on**: All calls

**Notes**: May be depericated in furture.


view_only
---------
This param is used only in the OSF to give users a view only permission for the file resource.

**Type**: flag

**Expected on**: GET for files or folders

Notes: Only used internally for the Open Science Framework.


Github Provider Params
======================
Query params specific to Github

ref
---
This gives a reference so Waterbutler can retrieve a commit with proper information to get metadata so it can preform other operations.

**Type**: str

**Expected on**: Calls to Github provider

**Interactions**:

* overrides 'branch' param

branch
------
This gives Github a reference to the correct repo branch, so it can retrieve a commit with metadata useful for other operations.

**Type**: str

**Expected on**: Calls to Github provider

fileSha
-------
This gives Github a reference to a file sha is part of it's path id with the branch ref.

**Type**: str

**Expected on**: Calls to Github provider
