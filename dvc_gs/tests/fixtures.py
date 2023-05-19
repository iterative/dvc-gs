import os

import pytest

from .cloud import GCP, FakeGCP


@pytest.fixture
# def make_gs(tmp_gcs_path, fake_gcs_server):
def make_gs(request):
    def _make_gs():
        bucket = os.environ.get("DVC_TEST_GS_BUCKET")
        if bucket and os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
            return GCP(GCP.get_url(), "")
        tmp_gcs_path = request.getfixturevalue("tmp_gcs_path")
        fake_gcs_server = request.getfixturevalue("fake_gcs_server")
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
