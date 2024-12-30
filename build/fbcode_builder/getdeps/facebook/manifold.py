# (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

# pyre-unsafe


import datetime
import os

from .. import cache as cache_module
from ..runcmd import run_cmd


class ManifoldArtifactCache(cache_module.ArtifactCache):
    BUCKET = "opensource_getdeps_build_cache"
    API_KEY = "opensource_getdeps_build_cache-key"
    CACHE_BUSTER = "1"
    TTL = int(datetime.timedelta(days=14).total_seconds())

    def _key(self, name) -> str:
        return "%s/flat/%s-%s" % (self.BUCKET, self.CACHE_BUSTER, name)

    def download_to_file(self, name, dest_file_name) -> bool:
        key = self._key(name)
        try:
            run_cmd(
                [
                    "manifold",
                    # uses HTTP through manifold.facebook.net VIP
                    "--vip",
                    "--apikey",
                    self.API_KEY,
                    "get",
                    key,
                    dest_file_name,
                ]
            )
            return True
        except Exception:
            # Remove a potential stray partial file
            try:
                os.unlink(dest_file_name)
            except OSError:
                pass

            return False

    def upload_from_file(self, name, source_file_name) -> None:
        key = self._key(name)
        run_cmd(
            [
                "manifold",
                # uses HTTP through manifold.facebook.net VIP
                "--vip",
                "--apikey",
                self.API_KEY,
                "put",
                "--ttl",
                str(self.TTL),
                source_file_name,
                key,
            ]
        )


# pyre-fixme[9]: create_cache has type `() -> None`; used as
#  `Type[ManifoldArtifactCache]`.
cache_module.create_cache = ManifoldArtifactCache
