import os

import pytest


@pytest.fixture(scope="session")
def docker_compose_file(pytestconfig):
    return os.path.join(
        str(pytestconfig.rootdir), "dvc_gs", "tests", "docker-compose.yml"
    )


@pytest.fixture
def make_gs():
    def _make_gs():
        raise NotImplementedError

    return _make_gs


@pytest.fixture
def gs(make_gs):
    return make_gs()

