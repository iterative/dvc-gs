import pytest

from dvc.testing.api_tests import (  # noqa: F401
    TestAPI,
)
from dvc.testing.remote_tests import (  # noqa: F401
    TestRemote,
    TestRemoteVersionAware,
)
from dvc.testing.workspace_tests import (  # noqa: F401  # noqa: F401
    TestGetUrl,
    TestImportURLVersionAware,
    TestLsUrl,
)
from dvc.testing.workspace_tests import TestImport as _TestImport


@pytest.fixture
def cloud(make_cloud):
    return make_cloud(typ="gs")


@pytest.fixture
def remote(make_remote):
    return make_remote(name="upstream", typ="gs")


@pytest.fixture
def remote_version_aware(make_remote_version_aware):
    return make_remote_version_aware(name="upstream", typ="gs")


@pytest.fixture
def remote_worktree(make_remote_worktree):
    return make_remote_worktree(name="upstream", typ="gs")


@pytest.fixture
def workspace(make_workspace):
    return make_workspace(name="workspace", typ="gs")


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
