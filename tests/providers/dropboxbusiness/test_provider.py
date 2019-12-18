from tests.providers.dropbox.test_provider import (TestValidatePath,
                                                   TestCRUD,
                                                   TestMetadata,
                                                   TestCreateFolder,
                                                   TestIntraMoveCopy,
                                                   TestOperations,)
from tests.providers.dropbox.fixtures import (auth,
                                              file_like,
                                              credentials,
                                              file_stream,
                                              file_content,
                                              error_fixtures,
                                              other_credentials,
                                              provider_fixtures,
                                              revision_fixtures,)
from tests.providers.dropboxbusiness.fixtures import (settings,
                                                      settings_root,
                                                      provider,
                                                      provider_root,
                                                      other_provider,)


class TestValidatePathBusiness(TestValidatePath):
    pass

class TestCRUDBusiness(TestCRUD):
    pass

class TestMetadataBusiness(TestMetadata):
    pass

class TestCreateFolderBusiness(TestCreateFolder):
    pass

class TestIntraMoveCopyBusiness(TestIntraMoveCopy):
    pass

class TestOperationsBusiness(TestOperations):
    pass
