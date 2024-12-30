# (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

# pyre-unsafe


import os
import subprocess
import sys

from ..envfuncs import Env
from ..errors import TransientFailure
from ..fetcher import ArchiveFetcher, download_url_to_file_with_progress
from ..runcmd import run_cmd


def no_proxy_env():
    """Return a copy of the environment with the proxy variables removed"""
    env = Env()
    env.unset("http_proxy")
    env.unset("https_proxy")
    return env


def lfs_path(build_options):
    """returns the path to the lfs.py utility"""
    return os.path.join(build_options.fbsource_dir, "fbcode/tools/lfs/lfs.py")


class NoProxy(object):
    """A context manager to help run a block of code with the proxy env vars unset"""

    def __enter__(self) -> None:
        pass
        # self.http_proxy = os.environ.pop("http_proxy", None)
        # self.https_proxy = os.environ.pop("https_proxy", None)

    def __exit__(self, *args) -> None:
        pass
        # if self.http_proxy:
        #    os.environ["http_proxy"] = self.http_proxy
        # if self.https_proxy:
        #    os.environ["https_proxy"] = self.https_proxy


def lfs_download(lfs_py, filename, download_dir) -> None:
    """download filename from LFS and place it into download_dir.
    filename is just the basename of the file; it is resolve from
    the lfs pointers file that lfs.py maintains."""
    url = (
        subprocess.check_output([sys.executable, lfs_py, "url", "-q", filename])
        .strip()
        .decode("ascii")
    )
    with NoProxy():
        download_url_to_file_with_progress(url, os.path.join(download_dir, filename))


def lfs_upload(lfs_py, filename) -> None:
    """upload filename to LFS"""
    try:
        run_cmd([sys.executable, lfs_py, "upload", filename], env=no_proxy_env())
    except subprocess.CalledProcessError as exc:
        raise TransientFailure(exc)


class LFSCachingArchiveFetcher(ArchiveFetcher):
    """Wraps the regular ArchiveFetcher up in LFS, checking in LFS
    for the desired file before falling back to fetching from the
    URL for real.  If it fetches from the URL, then it will also
    upload that archive to LFS."""

    _skip_lfs_download = False

    def skip_lfs_download(self) -> None:
        self._skip_lfs_download = True

    def _download(self) -> None:
        download_dir = self._download_dir()

        if self.build_options.fbsource_dir or self.build_options.lfs_path:
            if self.build_options.lfs_path:
                lfs_py = os.path.join(self.build_options.lfs_path, "lfs/lfs.py")
            else:
                lfs_py = lfs_path(self.build_options)

            lfs_filename = os.path.basename(self.file_name)
            if not self._skip_lfs_download:
                try:
                    lfs_download(lfs_py, lfs_filename, download_dir)
                    self._verify_hash()
                    return
                except Exception:
                    print(
                        "Failed to obtain %s via lfs, fall back to downloading directly"
                        % self.url
                    )
                    if os.path.exists(self.file_name):
                        os.unlink(self.file_name)

        super(LFSCachingArchiveFetcher, self)._download()

        if not self._skip_lfs_download and self.build_options.fbsource_dir:
            # pyre-fixme[61]: `lfs_py` is undefined, or not always defined.
            lfs_upload(lfs_py, self.file_name)
