from typing import Optional, Tuple
from urllib.parse import urlsplit, urlunsplit

from dvc_objects.fs.base import AnyFSPath
from dvc_objects.fs.path import Path


class GSPath(Path):
    def split_version(self, path: AnyFSPath) -> Tuple[str, Optional[str]]:
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

    def join_version(self, path: AnyFSPath, version_id: Optional[str]) -> str:
        path, path_version = self.split_version(path)
        if path_version:
            raise ValueError("path already includes an object generation")
        return f"{path}#{version_id}" if version_id else path

    def version_path(self, path: AnyFSPath, version_id: Optional[str]) -> str:
        path, _ = self.split_version(path)
        return self.join_version(path, version_id)

    def coalesce_version(
        self, path: AnyFSPath, version_id: Optional[str]
    ) -> Tuple[AnyFSPath, Optional[str]]:
        path, path_version_id = self.split_version(path)
        versions = {ver for ver in (version_id, path_version_id) if ver}
        if len(versions) > 1:
            raise ValueError(
                f"Path version mismatch: '{path}', '{version_id}'"
            )
        return path, (versions.pop() if versions else None)
