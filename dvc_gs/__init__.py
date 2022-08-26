import threading

# pylint:disable=abstract-method
from dvc_objects.fs.base import ObjectFileSystem
from funcy import cached_property, wrap_prop


class GSFileSystem(ObjectFileSystem):
    protocol = "gs"
    REQUIRES = {"gcsfs": "gcsfs"}
    PARAM_CHECKSUM = "etag"

    def _prepare_credentials(self, **config):
        login_info = {"consistency": None}
        login_info["project"] = config.get("projectname")
        login_info["token"] = config.get("credentialpath")
        login_info["endpoint_url"] = config.get("endpointurl")
        login_info["session_kwargs"] = {"trust_env": True}
        return login_info

    @wrap_prop(threading.Lock())
    @cached_property
    def fs(self):
        # TODO: Use `gcsfs` when https://github.com/fsspec/gcsfs/pull/488
        #       is merged and its version bumped
        from .gcsfs import GCSFileSystem

        return GCSFileSystem(**self.fs_args)

    @classmethod
    def _strip_protocol(cls, path: str) -> str:
        from fsspec.utils import infer_storage_options

        return infer_storage_options(path)["path"]

    def unstrip_protocol(self, path: str) -> str:
        return "gs://" + path.lstrip("/")
