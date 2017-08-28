API
===

v0 API
------

.. warning::

   The v0 WaterButler API is deprecated and should no longer be used.  It is only documented to provide a reference for legacy consumers.

TODO: v0 api docs

v1 API
------

The version 1 WaterButler API tries to conform to RESTful principles. A v1 url takes the form:

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


Magic Query Parameters
----------------------

* ``direct``: issuing a download request with a query parameter named ``direct`` indicates that WB should handle the download, even if a redirect would be possible (e.g. osfstorage and s3).  In this case, WB will act as a middleman, downloading the data from the provider and passing through to the requestor.
