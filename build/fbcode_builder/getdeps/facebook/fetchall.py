# (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

# pyre-unsafe


import os
import shutil

from ..buildopts import setup_build_options
from ..load import ManifestLoader
from ..subcmd import cmd, SubCmd
from .sandcastle import PLATFORMS


@cmd("fetch-all", "Fetch all possible sources to update lfs-pointers")
class FetchAllCmd(SubCmd):
    # pyre-fixme[15]: `run` overrides method defined in `SubCmd` inconsistently.
    def run(self, args) -> None:
        opts = setup_build_options(args)

        if args.clean:
            for name in ["downloads", "extracted"]:
                d = os.path.join(opts.scratch_dir, name)
                print("Cleaning %s..." % d)
                if os.path.exists(d):
                    shutil.rmtree(d)

        for p in PLATFORMS:
            ctx_gen = opts.get_context_generator(p)
            loader = ManifestLoader(opts, ctx_gen)
            manifests = loader.load_all_manifests()
            for m in manifests.values():
                if m.is_first_party_project():
                    # Not fbsource projects
                    continue

                try:
                    fetcher = loader.create_fetcher(m)
                except KeyError:
                    pass

                # pyre-fixme[61]: `fetcher` is undefined, or not always defined.
                if args.skip_lfs_download and hasattr(fetcher, "skip_lfs_download"):
                    fetcher.skip_lfs_download()
                fetcher.update()

    def setup_parser(self, parser) -> None:
        parser.add_argument(
            "--clean",
            action="store_true",
            default=False,
            help="Clean the downloads dir before fetching",
        )
        parser.add_argument(
            "--skip-lfs-download",
            action="store_true",
            default=False,
            help=(
                "Download from the URL, rather than LFS.  This is useful "
                "in cases where the upstream project has uploaded a new "
                "version of the archive with a different hash"
            ),
        )
