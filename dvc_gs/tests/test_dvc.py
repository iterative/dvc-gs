import pytest
from dvc.testing.test_api import TestAPI  # noqa, pylint: disable=unused-import
from dvc.testing.test_remote import (  # noqa, pylint: disable=unused-import
    TestRemote,
)
from dvc.testing.test_workspace import (  # noqa, pylint: disable=unused-import
    TestAdd,
    TestImport,
)


@pytest.fixture
def cloud_name():
    return "gs"


@pytest.fixture
def remote(make_remote, cloud_name):
    yield make_remote(name="upstream", typ=cloud_name)


@pytest.fixture
def workspace(make_workspace, cloud_name):
    yield make_workspace(name="workspace", typ=cloud_name)


@pytest.fixture
def stage_md5():
    pytest.skip("not supported")


@pytest.fixture
def is_object_storage():
    pytest.skip("not supported")


@pytest.fixture
def dir_md5():
    pytest.skip("not supported")


@pytest.fixture
def hash_name():
    pytest.skip("not supported")


@pytest.fixture
def hash_value():
    pytest.skip("not supported")


@pytest.fixture
def dir_hash_value(dir_md5):
    pytest.skip("not supported")
