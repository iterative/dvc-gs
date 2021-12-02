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
    if not GCP.should_test():
        pytest.skip("no gs")
    yield GCP(GCP.get_url())


@pytest.fixture
def gs(make_gs):
    return make_gs()
