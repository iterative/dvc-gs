import os

import pytest

from .cloud import GCP


@pytest.fixture(scope="session")
def docker_compose_file(pytestconfig):
    return os.path.join(
        str(pytestconfig.rootdir), "dvc_gs", "tests", "docker-compose.yml"
    )


@pytest.fixture
def make_gs():
    def _make_gs():
        return GCP(GCP.get_url())

    return _make_gs


@pytest.fixture
def gs(make_gs):
    return make_gs()
