import os

import pytest

from .cloud import GCP, FakeGCP


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
def make_gs(tmp_gcs_path, fake_gcs_server):
    def _make_gs():
        return FakeGCP(
            str(tmp_gcs_path).replace("gcs://", "gs://"),
            endpoint_url=fake_gcs_server,
        )

    return _make_gs


@pytest.fixture
def make_gs_version_aware(  # pylint: disable=unused-argument
    versioning, tmp_gcs_path, fake_gcs_server
):
    def _make_gs():
        return FakeGCP(
            str(tmp_gcs_path).replace("gcs://", "gs://"),
            endpoint_url=fake_gcs_server,
        )

    return _make_gs


@pytest.fixture
def gs(make_gs):  # pylint: disable=redefined-outer-name
    return make_gs()


@pytest.fixture
def real_gs():
    yield GCP(GCP.get_url(), "")
