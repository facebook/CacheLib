# (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

# pyre-unsafe


import glob
import os


__all__ = [
    # pyre-fixme[16]: Module `facebook` has no attribute `os`.
    os.path.splitext(os.path.basename(x))[0]
    # pyre-fixme[16]: Module `facebook` has no attribute `glob`.
    # pyre-fixme[16]: Module `facebook` has no attribute `os`.
    for x in sorted(glob.glob(os.path.dirname(__file__) + "/*.py"))
    if not x.endswith("__init__.py")
]

# pyre-fixme[21]: Could not find name `glob` in
#  `opensource.fbcode_builder.getdeps.facebook`.
# pyre-fixme[21]: Could not find name `os` in
#  `opensource.fbcode_builder.getdeps.facebook`.
from . import *  # isort:skip # noqa: F401, F403
