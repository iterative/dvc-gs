import os

import pytest

from .cloud import GCP


@pytest.fixture(scope="session")
def docker_compose_file(pytestconfig):
    return os.path.join(os.path.dirname(__file__), "docker-compose.yml")


@pytest.fixture(scope="session")
def gs_creds(tmp_path_factory):
    gcreds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not gcreds_path:
        dpath = tmp_path_factory.mktemp("gs_creds")
        fpath = dpath / "creds.json"
        fpath.write_text(os.getenv("GS_CREDS"))
        gcreds_path = os.fspath(fpath)
    # FIXME: we shouldn't need to use this return value
    # if GOOGLE_APPLICATION_CREDENTIALS is set
    return gcreds_path


@pytest.fixture
def make_gs(gs_creds):
    def _make_gs():
        return GCP(GCP.get_url(), gs_creds)

    return _make_gs


@pytest.fixture
def gs(make_gs):
    return make_gs()
