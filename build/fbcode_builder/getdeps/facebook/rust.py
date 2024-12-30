# (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

# pyre-unsafe


import os


def vendored_crates(fbsource_dir: str, cargo_config_content: str) -> str:
    """download vendored crates-io from LFS for the cargo builder"""
    snippet = """
[source.crates-io]
replace-with = "vendored-sources"

[source.vendored-sources]
directory = "{}"
""".format(
        os.path.join(fbsource_dir, "third-party/rust/vendor").replace("\\", "\\\\")
    )
    if snippet not in cargo_config_content:
        cargo_config_content += snippet
    return cargo_config_content
