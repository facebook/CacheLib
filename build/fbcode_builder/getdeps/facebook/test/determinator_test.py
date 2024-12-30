# (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

# pyre-unsafe


import os
import unittest

from ...buildopts import BuildOptions
from ...load import patch_loader
from ...platform import HostType
from ..sandcastle import DeterminatorData


def files_to_projects(files):
    opts = BuildOptions(
        fbcode_builder_dir=os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "../../../"
        ),
        scratch_dir="/tmp",
        host_type=HostType(),
    )

    # patch the manifest loader so that it can find everything
    # in the context of buck's sandboxed environment
    patch_loader(__name__)

    data = DeterminatorData(opts)
    projects = data.affected_projects(files)
    return projects


class DeterminatorTest(unittest.TestCase):
    def assertMapping(self, files, projects) -> None:
        self.assertEqual(files_to_projects(files), projects)

    def test_not_open_source(self) -> None:
        self.assertMapping(
            [
                "fbcode/supersecret/ExtraSpecialMagic.cpp",
                "fbcode/supersecret/TopSecretCode.cpp",
            ],
            [],
        )

    def test_no_facebook(self) -> None:
        self.assertMapping(["fbcode/folly/facebook/secret.cpp"], [])

    def test_folly(self) -> None:
        self.assertMapping(
            ["fbcode/folly/foo.cpp"],
            [
                "cachelib",
                "eden",
                "edencommon",
                "fb303",
                "fboss",
                "fbthrift",
                "fizz",
                "folly",
                "glean",
                "hsthrift",
                "katran",
                "mononoke",
                "mononoke_integration",
                "mvfst",
                "openr",
                "proxygen",
                "rust-shed",
                "sapling",
                "wangle",
                "watchman",
                "ws_airstore",
            ],
        )

    def test_getdeps_in_all(self) -> None:
        self.assertMapping(
            ["fbcode/opensource/fbcode_builder/getdeps.py"],
            [
                "cachelib",
                "eden",
                "edencommon",
                "fb303",
                "fboss",
                "fbthrift",
                "fizz",
                "folly",
                "glean",
                "hsthrift",
                "katran",
                "mononoke",
                "mononoke_integration",
                "mvfst",
                "openr",
                "proxygen",
                "rust-shed",
                "sapling",
                "wangle",
                "watchman",
                "ws_airstore",
            ],
        )

    def test_dont_over_exclude(self) -> None:
        self.assertMapping(
            [
                "fbcode/eden/fs/rocksdb/RocksHandles.h",
                "fbcode/eden/fs/service/EdenServer.cpp",
                "fbcode/eden/fs/store/RocksDbLocalStore.cpp",
                "fbcode/eden/fs/store/RocksDbLocalStore.h",
            ],
            [
                "eden",
                "mononoke_integration",
                "sapling",
            ],
        )

    def test_exclude_targets_only(self) -> None:
        self.assertMapping(["fbcode/eden/fs/TARGETS"], [])

    def test_fizz(self) -> None:
        self.assertMapping(
            ["fbcode/fizz/protocol/Factory.h"],
            [
                "cachelib",
                "eden",
                "edencommon",
                "fb303",
                "fboss",
                "fbthrift",
                "fizz",
                "hsthrift",
                "katran",
                "mononoke",
                "mononoke_integration",
                "mvfst",
                "openr",
                "proxygen",
                "rust-shed",
                "sapling",
                "wangle",
                "watchman",
                "ws_airstore",
            ],
        )

    def test_wangle(self) -> None:
        self.assertMapping(
            ["fbcode/wangle/codec/StringCodec.h"],
            [
                "cachelib",
                "eden",
                "edencommon",
                "fb303",
                "fboss",
                "fbthrift",
                "hsthrift",
                "mononoke",
                "mononoke_integration",
                "openr",
                "proxygen",
                "rust-shed",
                "sapling",
                "wangle",
                "watchman",
                "ws_airstore",
            ],
        )

    def test_thrift(self) -> None:
        self.assertMapping(
            ["fbcode/thrift/lib/cpp2/Thrift.h"],
            [
                "cachelib",
                "eden",
                "edencommon",
                "fb303",
                "fboss",
                "fbthrift",
                "hsthrift",
                "mononoke",
                "mononoke_integration",
                "openr",
                "rust-shed",
                "sapling",
                "watchman",
            ],
        )

    def test_watchman(self) -> None:
        self.assertMapping(["fbcode/watchman/foo"], ["watchman"])

    def test_no_targets(self) -> None:
        self.assertMapping(["fbcode/folly/TARGETS"], [])
