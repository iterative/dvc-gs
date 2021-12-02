from dvc.testing.cloud import Cloud
from dvc.testing.path_info import CloudURLInfo

class GS(Cloud, CloudURLInfo):
    @property
    def config(self):
        return {"url": self.url}

    def is_file(self):
        raise NotImplementedError

    def is_dir(self):
        raise NotImplementedError

    def exists(self):
        raise NotImplementedError

    def mkdir(self, mode=0o777, parents=False, exist_ok=False):
        raise NotImplementedError

    def write_bytes(self, contents):
        raise NotImplementedError

    def read_bytes(self):
        raise NotImplementedError

    @property
    def fs_path(self):
        raise NotImplementedError
