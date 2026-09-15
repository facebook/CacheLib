# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.


import argparse
import os
import shutil
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from ..cli import VendorCmd
from ..fetcher import ChangeStatus, LocalDirFetcher, PreinstalledNopFetcher
from ..manifest import ManifestContext, ManifestParser


def make_manifest(name: str, extra: str = "") -> ManifestParser:
    return ManifestParser(name, f"[manifest]\nname = {name}\n{extra}")


def make_ctx() -> ManifestContext:
    return ManifestContext(
        {
            "os": "linux",
            "distro": None,
            "distro_vers": None,
            "fb": "off",
            "fbsource": "off",
            "test": "off",
        }
    )


class FakeSourceFetcher:
    """Stands in for a Git/Archive fetcher: owns a source tree on disk."""

    def __init__(self, src_dir: str, hash_value: str) -> None:
        self.src_dir = src_dir
        self.hash_value = hash_value
        self.updated = False

    def update(self) -> ChangeStatus:
        self.updated = True
        return ChangeStatus()

    def hash(self) -> str:
        return self.hash_value

    def get_src_dir(self) -> str:
        return self.src_dir


class VendorCmdTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp)
        self.output_dir = os.path.join(self.tmp, "vendor")

    def make_src_tree(self, name: str) -> str:
        src = os.path.join(self.tmp, "src", name)
        os.makedirs(os.path.join(src, ".git"))
        os.makedirs(os.path.join(src, "sub"))
        with open(os.path.join(src, ".git", "HEAD"), "w") as f:
            f.write("ref: refs/heads/main\n")
        with open(os.path.join(src, "sub", "code.cpp"), "w") as f:
            f.write("int x;\n")
        # a symlink into a directory that won't exist on an offline builder
        os.symlink(os.path.join(src, "sub", "code.cpp"), os.path.join(src, "link.cpp"))
        return src

    def run_vendor(self, manifests, fetchers) -> None:
        loader = MagicMock()
        loader.manifests_in_dependency_order.return_value = manifests
        loader.create_fetcher.side_effect = lambda m: fetchers[m.name]
        args = argparse.Namespace(output_dir=self.output_dir)
        VendorCmd().run_project_cmd(args, loader, manifests[-1])

    def test_vendors_source_deps_and_skips_system_and_top_level(self) -> None:
        dep_src = FakeSourceFetcher(self.make_src_tree("depa"), "a" * 40)
        top_src = FakeSourceFetcher(self.make_src_tree("top"), "t" * 40)
        manifests = [
            make_manifest("depa"),
            make_manifest("sysdep"),
            make_manifest("top"),
        ]
        fetchers = {
            "depa": dep_src,
            "sysdep": PreinstalledNopFetcher(),
            "top": top_src,
        }

        self.run_vendor(manifests, fetchers)

        self.assertTrue(dep_src.updated, "source dep must be fetched before copying")
        self.assertFalse(top_src.updated, "the project itself is not vendored")
        self.assertEqual(
            sorted(os.listdir(self.output_dir)), ["depa", "getdeps-vendor.txt"]
        )

        vendored = os.path.join(self.output_dir, "depa")
        self.assertTrue(os.path.isfile(os.path.join(vendored, "sub", "code.cpp")))
        self.assertFalse(os.path.exists(os.path.join(vendored, ".git")))
        # symlinks are materialised so the tree is self-contained
        self.assertTrue(os.path.isfile(os.path.join(vendored, "link.cpp")))
        self.assertFalse(os.path.islink(os.path.join(vendored, "link.cpp")))

        with open(os.path.join(self.output_dir, "getdeps-vendor.txt")) as f:
            self.assertEqual(f.read(), "depa %s\n" % ("a" * 40))

    def test_replaces_stale_vendored_tree(self) -> None:
        stale = os.path.join(self.output_dir, "depa", "stale.txt")
        os.makedirs(os.path.dirname(stale))
        with open(stale, "w") as f:
            f.write("old\n")
        dep_src = FakeSourceFetcher(self.make_src_tree("depa"), "b" * 40)
        manifests = [make_manifest("depa"), make_manifest("top")]
        fetchers = {"depa": dep_src, "top": FakeSourceFetcher(self.tmp, "t" * 40)}

        self.run_vendor(manifests, fetchers)

        self.assertFalse(os.path.exists(stale))
        self.assertTrue(
            os.path.isfile(os.path.join(self.output_dir, "depa", "sub", "code.cpp"))
        )


class VendorDirFetcherTest(unittest.TestCase):
    """--vendor-dir routes third-party deps through LocalDirFetcher, or fails."""

    DOWNLOAD_MANIFEST = """
[download]
url = https://example.com/dep-1.0.tar.gz
sha256 = 0000000000000000000000000000000000000000000000000000000000000000
"""

    def setUp(self) -> None:
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp)
        self.build_opts = MagicMock()
        self.build_opts.use_shipit = False
        self.build_opts.fbsource_dir = None
        self.build_opts.allow_system_packages = False
        self.build_opts.vendor_dir = os.path.join(self.tmp, "vendor")
        patcher = patch(
            "getdeps.manifest.ShipitTransformerFetcher.available", return_value=False
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_vendored_project_uses_local_dir_fetcher(self) -> None:
        vendored = os.path.join(self.build_opts.vendor_dir, "dep")
        os.makedirs(vendored)
        manifest = make_manifest("dep", self.DOWNLOAD_MANIFEST)

        fetcher = manifest._create_fetcher(self.build_opts, make_ctx())

        self.assertIsInstance(fetcher, LocalDirFetcher)
        self.assertEqual(fetcher.get_src_dir(), os.path.realpath(vendored))

    def test_missing_vendored_project_fails_instead_of_downloading(self) -> None:
        os.makedirs(self.build_opts.vendor_dir)
        manifest = make_manifest("dep", self.DOWNLOAD_MANIFEST)

        with self.assertRaisesRegex(
            Exception, "project dep is not present in .*vendor"
        ):
            manifest._create_fetcher(self.build_opts, make_ctx())

    def test_no_vendor_dir_keeps_normal_fetcher(self) -> None:
        self.build_opts.vendor_dir = None
        manifest = make_manifest("dep", self.DOWNLOAD_MANIFEST)

        fetcher = manifest._create_fetcher(self.build_opts, make_ctx())

        self.assertNotIsInstance(fetcher, LocalDirFetcher)
        self.assertEqual(fetcher.hash(), "0" * 64)
