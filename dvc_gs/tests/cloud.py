import os
import uuid

from dvc.testing.cloud import Cloud
from dvc.testing.path_info import CloudURLInfo


class GCP(Cloud, CloudURLInfo):
    IS_OBJECT_STORAGE = True

    def __init__(self, url, credentialpath):
        super().__init__(url)
        self.credentialpath = credentialpath

    def __truediv__(self, key):
        ret = super().__truediv__(key)
        ret.credentialpath = self.credentialpath
        return ret

    @property
    def fs_path(self):
        return self.bucket + "/" + self.path.lstrip("/")

    @property
    def config(self):
        config = {"url": self.url}
        if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
            config["credentialpath"] = self.credentialpath
        return config

    @staticmethod
    def _get_storagepath():
        bucket = os.environ.get("DVC_TEST_GS_BUCKET")
        assert bucket
        return bucket + "/" + str(uuid.uuid4())

    @staticmethod
    def get_url():
        return "gs://" + GCP._get_storagepath()

    @property
    def _gc(self):
        from google.cloud.storage import Client

        return Client.from_service_account_json(self.credentialpath)

    @property
    def _bucket(self):
        return self._gc.bucket(self.bucket)

    @property
    def _blob(self):
        return self._bucket.blob(self.path)

    def is_file(self):
        if self.path.endswith("/"):
            return False

        return self._blob.exists()

    def is_dir(self):
        dir_path = self / ""
        return bool(list(self._bucket.list_blobs(prefix=dir_path.path)))

    def exists(self):
        return self.is_file() or self.is_dir()

    # pylint: disable-next=unused-argument
    def mkdir(self, mode=0o777, parents=False, exist_ok=False):
        assert mode == 0o777
        assert parents

    def write_bytes(self, contents):
        assert isinstance(contents, bytes)
        self._blob.upload_from_string(contents)

    def unlink(self, missing_ok: bool = False) -> None:
        if not self.exists():
            if not missing_ok:
                raise FileNotFoundError(str(self))
            return
        self._blob.delete()

    def rmdir(self, recursive: bool = True) -> None:
        if not self.is_dir():
            raise NotADirectoryError(str(self))

        blobs = list(self._bucket.list_blobs(prefix=(self / "").path))
        if not blobs:
            return

        if not recursive:
            raise OSError(f"Not recursive and directory not empty: {self}")

        for blob in blobs:
            blob.delete()

    def read_bytes(self):
        return self._blob.download_as_string()


class FakeGCP(GCP):
    """Fake GCP client that is supposed to be using a mock server's endpoint"""

    def __init__(self, url, endpoint_url: str):
        super().__init__(
            url,
            "",  # no need to provide credentials for a mock server
        )
        self.endpoint_url = endpoint_url

    def __truediv__(self, key):
        ret = super().__truediv__(key)
        ret.endpoint_url = self.endpoint_url
        return ret

    @property
    def config(self):
        return {
            "url": self.url,
            "endpointurl": self.endpoint_url,
            "allow_anonymous_login": True,
        }

    def get_url(self):  # pylint: disable=arguments-differ
        return self.url

    @property
    def _gc(self):
        from google.cloud.storage import Client

        return Client(
            use_auth_w_custom_endpoint=False,
            client_options={"api_endpoint": self.endpoint_url},
        )
