Adding A New Provider
=====================

The job of the provider is to translate our common RESTful API into actions against the external provider.  The WaterButler API v1 handler (waterbutler.server.api.v1.provider) accepts the incoming requests, builds the appropriate provider object, does some basic validation on the inputs, then passes the request data off to the provider action method.  A new provider will inherit from `waterbutler.core.provider.BaseProvider` and implement some or all of the following methods::

    validate_path()         abstract
    validate_v1_path()      abstract
    download()              abstract
    metadata()              abstract
    upload()                abstract
    delete()                abstract
    can_duplicate_names()   abstract
    create_folder()         error (405 Not Supported)
    intra_copy()            error (501 Not Implemented)
    intra_move()            default
    can_intra_copy()        default
    can_intra_move()        default
    exists()                default
    revalidate_path()       default
    zip()                   default
    path_from_metadata()    default
    revisions()             default
    shares_storage_root()   default
    move()                  default
    copy()                  default
    handle_naming()         default
    handle_name_conflict()  default


The methods labeled ``abstract`` must be implemented.  The methods labeled ``error`` do not need to be implemented, but will raise errors if a user accesses them.  The methods labeled ``default`` have default implementations that may suffice depending on the provider.
