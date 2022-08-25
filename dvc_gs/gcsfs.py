# pylint: disable=abstract-method
# TODO: remove this module when https://github.com/fsspec/gcsfs/pull/488
#       is merged and version is bumped

from gcsfs import GCSFileSystem as GCSFileSystem_


class GCSFileSystem(GCSFileSystem_):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    async def _find(
        self, path, withdirs=False, detail=False, prefix="", **kwargs
    ):
        path = self._strip_protocol(path)
        bucket, key = self.split_path(path)

        if prefix:
            _path = "" if not key else key.rstrip("/") + "/"
            _prefix = f"{_path}{prefix}"
        else:
            _prefix = key

        objects, _ = await self._do_list_objects(
            bucket, delimiter="", prefix=_prefix
        )

        dirs = {}
        cache_entries = {}

        for obj in objects:
            parent = self._parent(obj["name"])
            previous = obj

            while parent:
                dir_key = self.split_path(parent)[1]
                if not dir_key:
                    break

                dirs[parent] = {
                    "Key": dir_key,
                    "Size": 0,
                    "name": parent,
                    "StorageClass": "DIRECTORY",
                    "type": "directory",
                    "size": 0,
                }

                if len(parent) < len(path):
                    # don't go above the requested level
                    break

                cache_entries.setdefault(parent, []).append(previous)

                previous = dirs[parent]
                parent = self._parent(parent)

        if not prefix:
            self.dircache.update(cache_entries)

        if withdirs:
            objects = sorted(
                objects + list(dirs.values()), key=lambda x: x["name"]
            )

        if detail:
            return {o["name"]: o for o in objects}

        return [o["name"] for o in objects]
