import os
import shlex
import subprocess
import sys
import time
from urllib.request import urlopen

import pytest
from google.cloud.storage import Client

from .cloud import GCP, FakeGCP

GCS_DEFAULT_PORT = 4443
CONTAINER_NAME = "dvc_gs_fake_gcs_server"


def stop_docker():
    container_id = subprocess.check_output(
        ["docker", "ps", "-q", "-f", f"name={CONTAINER_NAME}"],
        text=True,
    ).strip()
    if container_id:
        subprocess.call(["docker", "rm", "-f", "-v", container_id])


def start_docker():
    url_no_scheme = "localhost:4443"
    url = f"http://{url_no_scheme}"
    cmd = (
        "docker run -d -p 4443:4443 "
        f"--name {CONTAINER_NAME} "
        "fsouza/fake-gcs-server:latest -backend memory -scheme http "
        f"-public-host {url_no_scheme} -external-url {url}"
    )
    subprocess.check_output(shlex.split(cmd))
    return url


def wait_for_gcs(url, timeout):
    while timeout > 0:
        try:
            urlopen(f"{url}/storage/v1/b", timeout=3)
            yield
            return
        except Exception as e:
            timeout -= 1
            if timeout <= 0:
                raise TimeoutError("Failed to start fake GCS server") from e
            time.sleep(1)


@pytest.fixture(scope="session")
def fake_gcs_server():
    if os.environ.get("CI") and sys.platform == "darwin":
        pytest.skip("disabled for macOS on GitHub Actions")
    if sys.platform == "win32":
        pytest.skip("Docker-based fake GCS server is not supported on Windows")

    stop_docker()
    url = start_docker()
    try:
        wait_for_gcs(url + "/storage/v1/b", timeout=15)
        yield url
    finally:
        stop_docker()


@pytest.fixture
def gs_client(fake_gcs_server):
    return Client(
        use_auth_w_custom_endpoint=False,
        client_options={"api_endpoint": fake_gcs_server},
    )


@pytest.fixture
def gs_bucket(gs_client):
    bucket_name = "test-bucket"
    bucket = gs_client.bucket(bucket_name)
    bucket.create()
    try:
        yield bucket.name
    finally:
        for blob in bucket.list_blobs():
            blob.delete()
        bucket.delete()


@pytest.fixture
def gs_versioned_bucket(gs_client):
    bucket_name = "test-bucket-versioned"
    bucket = gs_client.bucket(bucket_name)
    bucket.versioning_enabled = True
    bucket.create()
    try:
        yield bucket.name
    finally:
        for blob in bucket.list_blobs():
            blob.delete()
        bucket.delete()


@pytest.fixture
def make_gs(request):
    bucket = os.environ.get("DVC_TEST_GS_BUCKET")
    if bucket and os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        return lambda: GCP(GCP.get_url(), "")

    fake_gcs_server = request.getfixturevalue("fake_gcs_server")
    bucket = request.getfixturevalue("gs_bucket")
    return lambda: FakeGCP("gs://" + bucket, endpoint_url=fake_gcs_server)


@pytest.fixture
def make_gs_version_aware(request):
    bucket = os.environ.get("DVC_TEST_GS_BUCKET_VERSIONED")
    if bucket and os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        return lambda: GCP(GCP.get_url(), "")

    fake_gcs_server = request.getfixturevalue("fake_gcs_server")
    bucket = request.getfixturevalue("gs_versioned_bucket")
    return lambda: FakeGCP("gs://" + bucket, endpoint_url=fake_gcs_server)


@pytest.fixture
def gs(make_gs):  # pylint: disable=redefined-outer-name
    return make_gs()


@pytest.fixture
def remote(make_remote):
    return make_remote(name="upstream", typ="gs")


@pytest.fixture
def real_gs():
    return GCP(GCP.get_url(), "")
