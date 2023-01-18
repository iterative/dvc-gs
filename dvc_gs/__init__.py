import threading

# pylint:disable=abstract-method
from dvc_objects.fs.base import ObjectFileSystem
from funcy import cached_property, wrap_prop

from .path import GSPath


class GSFileSystem(ObjectFileSystem):
    protocol = "gs"
    REQUIRES = {"gcsfs": "gcsfs"}
    PARAM_CHECKSUM = "etag"

    @cached_property
    def path(self) -> GSPath:
        def _getcwd():
            return self.fs.root_marker

        return GSPath(self.sep, getcwd=_getcwd)

    def _prepare_credentials(self, **config):
        login_info = {"consistency": None}
        login_info["version_aware"] = config.get("version_aware", False)
        login_info["project"] = config.get("projectname")
        login_info["token"] = config.get("credentialpath")
        login_info["endpoint_url"] = config.get("endpointurl")
        login_info["session_kwargs"] = {"trust_env": True}
        return login_info

    @wrap_prop(threading.Lock())
    @cached_property
    def fs(self):
        from gcsfs import GCSFileSystem

        return GCSFileSystem(**self.fs_args)

    @classmethod
    def _strip_protocol(cls, path: str) -> str:
        from fsspec.utils import infer_storage_options

        return infer_storage_options(path)["path"]

    def unstrip_protocol(self, path: str) -> str:
        return "gs://" + path.lstrip("/")

    @staticmethod
    def _get_kwargs_from_urls(urlpath: str):
        from gcsfs import GCSFileSystem

        parts = GCSFileSystem._split_path(  # pylint: disable=protected-access
            urlpath, version_aware=True
        )
        _, _, generation = parts
        if generation is not None:
            return {"version_aware": True}
        return {}
