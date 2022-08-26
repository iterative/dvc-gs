import pytest
from dvc.testing.test_api import TestAPI  # noqa, pylint: disable=unused-import
from dvc.testing.test_remote import (  # noqa, pylint: disable=unused-import
    TestRemote,
)
from dvc.testing.test_workspace import TestImport as _TestImport


@pytest.fixture
def remote(make_remote):
    yield make_remote(name="upstream", typ="gs")


@pytest.fixture
def workspace(make_workspace):
    yield make_workspace(name="workspace", typ="gs")


class TestImport(_TestImport):
    @pytest.fixture
    def stage_md5(self):
        # stage md5 depends on the dir_md5, see below
        return None

    @pytest.fixture
    def is_object_storage(self):
        return True

    @pytest.fixture
    def dir_md5(self):
        # dir md5 for imported dirs changes on every run.
        # we temporarily set it to None until the test is revisited
        # https://github.com/iterative/dvc-gs/issues/7#issuecomment-1218497067
        return None
