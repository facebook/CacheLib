# (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

# pyre-unsafe

import os.path


def build_default_vcvarsall(fbsource_dir):
    return [
        os.path.join(
            fbsource_dir,
            "third-party/toolchains/visual_studio/14.29.30133/VC/Auxiliary/Build/vcvarsall.bat",
        ).replace("\\", "/")
    ]


extra_vc_cmake_defines = {
    "Boost_COMPILER": "vc142",
}
