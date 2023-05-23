import threading
from typing import Iterator, List, Optional, Union

# pylint:disable=abstract-method
from dvc_objects.fs.base import AnyFSPath, ObjectFileSystem
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
        project = config.get("projectname")
        if project is not None:
            login_info["project"] = project
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

    def find(
        self,
        path: Union[AnyFSPath, List[AnyFSPath]],
        prefix: bool = False,
        batch_size: Optional[int] = None,  # pylint: disable=unused-argument
        **kwargs,
    ) -> Iterator[str]:
        def _add_dir_sep(path: str) -> str:
            # NOTE: gcsfs expects explicit trailing slash for dir find()
            if self.isdir(path) and not path.endswith(self.path.flavour.sep):
                return path + self.path.flavour.sep
            return path

        if not prefix:
            if isinstance(path, str):
                path = _add_dir_sep(path)
            else:
                path = [_add_dir_sep(p) for p in path]
        return super().find(
            path, prefix=prefix, batch_size=batch_size, **kwargs
        )
