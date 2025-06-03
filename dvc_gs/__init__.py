import threading
from collections.abc import Iterator
from typing import ClassVar, Optional, Union
from urllib.parse import urlsplit, urlunsplit

from funcy import wrap_prop

# pylint:disable=abstract-method
from dvc.utils.objects import cached_property
from dvc_objects.fs.base import AnyFSPath, ObjectFileSystem


class GSFileSystem(ObjectFileSystem):
    protocol = "gs"
    REQUIRES: ClassVar[dict[str, str]] = {"gcsfs": "gcsfs"}
    PARAM_CHECKSUM = "etag"

    @classmethod
    def split_version(cls, path: AnyFSPath) -> tuple[str, Optional[str]]:
        from gcsfs import GCSFileSystem

        parts = list(urlsplit(path))
        # NOTE: we use urlsplit/unsplit here to strip scheme before calling
        # GCSFileSystem._split_path, otherwise it will consider DVC
        # remote:// protocol to be a bucket named "remote:"
        scheme = parts[0]
        parts[0] = ""
        path = urlunsplit(parts)
        parts = GCSFileSystem._split_path(  # pylint: disable=protected-access
            path, version_aware=True
        )
        bucket, key, generation = parts
        scheme = f"{scheme}://" if scheme else ""
        return f"{scheme}{bucket}/{key}", generation

    @classmethod
    def join_version(cls, path: AnyFSPath, version_id: Optional[str]) -> str:
        path, path_version = cls.split_version(path)
        if path_version:
            raise ValueError("path already includes an object generation")
        return f"{path}#{version_id}" if version_id else path

    @classmethod
    def version_path(cls, path: AnyFSPath, version_id: Optional[str]) -> str:
        path, _ = cls.split_version(path)
        return cls.join_version(path, version_id)

    @classmethod
    def coalesce_version(
        cls, path: AnyFSPath, version_id: Optional[str]
    ) -> tuple[AnyFSPath, Optional[str]]:
        path, path_version_id = cls.split_version(path)
        versions = {ver for ver in (version_id, path_version_id) if ver}
        if len(versions) > 1:
            raise ValueError(f"Path version mismatch: '{path}', '{version_id}'")
        return path, (versions.pop() if versions else None)

    def _prepare_credentials(self, **config):
        login_info = {"consistency": None}
        login_info["version_aware"] = config.get("version_aware", False)
        project = config.get("projectname")
        if project is not None:
            login_info["project"] = project

        if config.get("allow_anonymous_login"):
            login_info["token"] = "anon"  # noqa: S105
        else:
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
        path: Union[AnyFSPath, list[AnyFSPath]],
        prefix: bool = False,
        batch_size: Optional[int] = None,  # pylint: disable=unused-argument
        **kwargs,
    ) -> Iterator[str]:
        def _add_dir_sep(path: str) -> str:
            # NOTE: gcsfs expects explicit trailing slash for dir find()
            if self.isdir(path) and not path.endswith(self.sep):
                return path + self.sep
            return path

        if not prefix:
            if isinstance(path, str):
                path = _add_dir_sep(path)
            else:
                path = [_add_dir_sep(p) for p in path]
        return super().find(path, prefix=prefix, batch_size=batch_size, **kwargs)
