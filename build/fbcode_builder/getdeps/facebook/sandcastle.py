# (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

# pyre-unsafe


import json
import os
import re
import subprocess
import sys
from copy import copy

from ..buildopts import setup_build_options
from ..load import ManifestLoader
from ..platform import HostType
from ..subcmd import cmd, SubCmd


PLATFORMS = [
    HostType("linux", "centos_stream", "8"),
    HostType("darwin", None, None),
    HostType("windows", None, None),
]

DEFAULT_TENANT = "oss_fbcode_builder"
FBCODE_CASTLE_TENANT = "fbcode_castle"


class Owner(object):
    """Owner and other metadata info for sandcastle jobs"""

    def __init__(
        self,
        manifest,
        oncall=None,
        tasks: bool = False,
        tenant=None,
        extra_cmake_defines=None,
        extra_build_args=None,
        extra_test_opts=None,
        diff_time: bool = True,
        skip_sandcastle=None,
        facebook_internal=None,
        os_variants=None,
        continuous_system_packages=None,
        diff_time_system_packages=True,
        diff_time_alerts=None,
        job_suffix=None,
        build_target=None,
        build_target_manifest=None,
        shared_libs: bool = False,
    ) -> None:
        self.manifest = manifest
        self.oncall = oncall
        self.tasks = tasks
        self.tenant = tenant
        self.extra_cmake_defines = extra_cmake_defines
        self.extra_build_args = extra_build_args
        self.extra_test_opts = extra_test_opts
        self.diff_time = diff_time
        self.skip_sandcastle = skip_sandcastle
        if facebook_internal is None:
            # one job and let defaulting happen
            facebook_internal = {"oss": None}
        if os_variants is None:
            os_variants = {}
        self.facebook_internal = facebook_internal
        self.os_variants = os_variants
        self.continuous_system_packages = continuous_system_packages
        self.diff_time_system_packages = diff_time_system_packages
        self.diff_time_alerts = diff_time_alerts or {}
        self.job_suffix = job_suffix
        self.build_target = build_target
        self.build_target_manifest = build_target_manifest
        self.shared_libs = shared_libs

    def jobspecs(self, args, platform):
        specs = []

        job_variants = self.os_variants.get(platform.ostype, self.facebook_internal)

        for prefix, facebook_internal in job_variants.items():
            if args.facebook_internal is None:
                args = copy(args)
                args.facebook_internal = facebook_internal
            build_opts = setup_build_options(args, platform)

            if args.job_name_prefix:
                prefix = args.job_name_prefix + "-" + prefix

            spec = jobspec_for_platform(self, prefix, build_opts, args, self.manifest)
            if spec:
                specs.append(spec)

        return specs


# A list of ownership information.
# Ideally this would live in the manifest files themselves, but since
# those are sync'ed out to public repos and this information doesn't need
# to be public, it's encoded here instead.
OWNERS = [
    Owner(
        "eden",
        oncall="scm_client_infra",
        tenant="eden",
        tasks=True,
        facebook_internal={"oss": False},
        continuous_system_packages=True,
    ),
    Owner(
        "edencommon",
        oncall="scm_client_infra",
        tenant="eden",
        tasks=True,
        continuous_system_packages=True,
    ),
    Owner(
        "fboss",
        oncall="fboss_oss",
        diff_time_alerts={
            "type": "email",
            "triggers": ["fail", "infra_fail", "user_error"],
            "emails": ["paulcruz74@meta.com"],
        },
        extra_build_args="--num-jobs 32",
        extra_cmake_defines={
            "SKIP_ALL_INSTALL": "ON",
            "BUILD_SAI_FAKE": "ON",
            "BUILD_SAI_FAKE_LINK_TEST": "ON",
            "CMAKE_CXX_STANDARD": "20",
            "CMAKE_BUILD_TYPE": "MinSizeRel",
        },
        extra_test_opts="--no-testpilot --retry 0",
        job_suffix="fboss_agent_targets",
        build_target="fboss_fake_agent_targets",
        build_target_manifest="fboss",
    ),
    Owner(
        "fboss",
        oncall="fboss_oss",
        diff_time_alerts={
            "type": "email",
            "triggers": ["fail", "infra_fail", "user_error"],
            "emails": ["paulcruz74@meta.com"],
        },
        extra_build_args="--num-jobs 48",
        extra_cmake_defines={
            "SKIP_ALL_INSTALL": "ON",
            "BUILD_SAI_FAKE": "ON",
            "CMAKE_CXX_STANDARD": "20",
            "CMAKE_BUILD_TYPE": "MinSizeRel",
        },
        extra_test_opts="--no-testpilot --retry 0",
        job_suffix="fboss_other_services",
        build_target="fboss_other_services",
        build_target_manifest="fboss",
    ),
    Owner(
        "fboss",
        oncall="fboss_oss",
        diff_time_alerts={
            "type": "email",
            "triggers": ["fail", "infra_fail", "user_error"],
            "emails": ["paulcruz74@meta.com"],
        },
        extra_cmake_defines={
            "SKIP_ALL_INSTALL": "ON",
            "BUILD_SAI_FAKE": "ON",
            "CMAKE_CXX_STANDARD": "20",
            "CMAKE_BUILD_TYPE": "MinSizeRel",
        },
        extra_test_opts="--no-testpilot --retry 0",
        job_suffix="fboss_platform_services",
        build_target="fboss_platform_services",
        build_target_manifest="fboss",
    ),
    Owner("fb303", oncall="thrift"),
    Owner("fbthrift", oncall="thrift"),
    Owner("fizz", oncall="secure_pipes", tasks=True),
    Owner("folly", oncall="folly_oss"),
    Owner(
        "mononoke",
        oncall="scm_server_infra",
        tasks=True,
        facebook_internal={
            "oss":
            # facebook_internal
            False
        },
        continuous_system_packages=True,
    ),
    Owner("mvfst", oncall="traffic_protocols", tasks=True),
    Owner("proxygen", oncall="traffic_protocols", tasks=True),
    Owner("wangle", oncall="folly_oss"),
    Owner(
        "watchman",
        oncall="scm_client_infra",
        tenant="eden",
        tasks=True,
        continuous_system_packages=True,
    ),
    Owner("openr", oncall="routing_protocol"),
    Owner(
        "f4d",
        oncall="f4d_sync",
        extra_cmake_defines={
            "CMAKE_CXX_FLAGS": "-mavx2 -mfma -mavx -mf16c -march=native"
        },
        skip_sandcastle=True,
    ),
    Owner(
        "glean",
        oncall="code_indexing",
        os_variants={"windows": {}, "darwin": {}},
        extra_cmake_defines={"BOOST_LINK_STATIC": "OFF"},
        shared_libs=True,
    ),
    Owner(
        "hsthrift",
        oncall="code_indexing",
        os_variants={"windows": {}, "darwin": {}},
        extra_cmake_defines={"BOOST_LINK_STATIC": "OFF"},
        shared_libs=True,
    ),
    Owner("rust-shed", oncall="autocargo"),
    Owner("ws_airstore", oncall="ws_airstore", tasks=True),
    Owner("katran", oncall="traffic_shiv"),
    Owner("delos_core", oncall="delos"),
]

DEFAULT_SHIPIT_EXCLUSIONS = [".*/TARGETS", ".*/facebook/.*", ".+\\.bzl"]


def get_owners_for_project(project):
    owners = []
    for o in OWNERS:
        if o.manifest == project:
            owners.append(o)
    return owners if len(owners) != 0 else [Owner(project)]


def jobspec_for_platform(owner, prefix, build_opts, args, project):
    if build_opts.is_linux():
        capabilities = {
            "vcs": "fbcode-fbsource",
            "type": "lego",
        }
    elif build_opts.is_darwin():
        capabilities = {
            "vcs": "fbcode-fbsource",
            "type": "lego-mac-arm",
            "xcode": "stable",
            "marker": "eden",
        }
    elif build_opts.is_windows():
        capabilities = {
            "vcs": "full-fbsource",
            "type": "lego-windows",
            "marker": "eden",
        }
    else:
        raise Exception("don't know how to build for this system")

    ctx_gen = build_opts.get_context_generator()
    loader = ManifestLoader(build_opts, ctx_gen)
    manifest = loader.load_manifest(project)
    manifest_ctx = loader.ctx_gen.get_context(manifest.name)

    # Some projects don't do anything "useful" as a leaf project, only
    # as a dep for a leaf project.  Check for those here; we don't want
    # to waste the effort scheduling them on sandcastle.
    # We do this by looking at the builder type in the manifest file
    # rather than creating a builder and checking its type because we
    # don't know enough to create the full builder instance here.
    if manifest.get("build", "builder", ctx=manifest_ctx) == "nop":
        return None

    # `skip_sandcastle` won't generate any entries for either Sandcastle
    # or determinator runs.
    if owner.skip_sandcastle:
        return None

    # Some projects don't run at diff time. The default is to do it, but when
    # diff_time is set to False, then we don't run.
    if args.schedule_type == "diff" and not owner.diff_time:
        return None

    projects = loader.manifests_in_dependency_order()

    if build_opts.is_windows():
        # Windows jobs run with cwd=fbsource
        getdeps = "fbcode/opensource/fbcode_builder/getdeps.py"
    else:
        # Posix jobs run with cwd=fbsource/fbcode
        getdeps = "opensource/fbcode_builder/getdeps.py"

    steps = []

    proxy = "http://fwdproxy:8080"
    env = {
        "http_proxy": proxy,
        "https_proxy": proxy,
        "no_proxy": ".fbcdn.net,.facebook.com,.thefacebook.com,.tfbnw.net,"
        + ".fb.com,.fburl.com,.facebook.net,.sb.fbsbx.com,localhost",
    }

    if build_opts.is_linux():
        # The facebook user doesn't exist on lego, twsvcscm is the
        # closest analogue
        user = "twsvcscm"
    else:
        user = "facebook"

    if build_opts.is_windows():
        # Cargo uses libgit2 to internally handle git fetching, and for some
        # reason it doesn't respect environment variables on Windows. Let's
        # instead instruct it to shell out to the git cli.
        env["CARGO_NET_GIT_FETCH_WITH_CLI"] = "true"
        # Apparently Cargo with tls sometimes fails on Windows
        # (https://github.com/rust-lang/cargo/issues/7096). The recommendation
        # is to skip the revocation check.
        env["CARGO_HTTP_CHECK_REVOKE"] = "false"
        steps.append(
            {
                "name": "Set git longpaths and line endings",
                "user": user,
                # these are the same git config settings as used in github getdeps CI
                # && is not supported on default powershell, hence the cmd invocation
                "shell": 'cmd /c "git config --system core.longpaths true && git config --system core.autocrlf false && git config --system core.symlinks true"',
            }
        )

    alias_suffix = "getdeps"
    system_arguments = ""
    schedule_type = args.schedule_type or "diff"

    if build_opts.is_linux() and build_opts.host_type.get_package_manager():

        def should_use_system_packages() -> bool:
            if args.allow_system_packages:
                return True

            if args.disallow_system_packages:
                return False

            # diff_time_system_packages defaults to True. packages must
            # explicitly opt out of diff time system packages.
            if schedule_type == "diff":
                return owner.diff_time_system_packages
            # continuous_system_packages defaults to None i.e. false. packages
            # must explicitly opt in to using system packages on continuous
            # builds.
            else:
                return owner.continuous_system_packages

        if should_use_system_packages():
            steps.append(
                {
                    "name": "Install system dependencies",
                    "user": "root",
                    "shell": "unset http_proxy; unset https_proxy; fbpython %s %s %s"
                    % (getdeps, "install-system-deps --recursive", manifest.name),
                }
            )

            steps.append(
                {
                    "name": "Chown scratch directory",
                    "user": "root",
                    "shell": 'chown -R %s:%s "$(%s show-scratch-dir)"'
                    % (user, user, getdeps),
                }
            )

            system_arguments = "--allow-system-packages "
            # if someone forces a cont job with system packages, lets use different job name
            if schedule_type != "diff" and args.allow_system_packages:
                alias_suffix += "-system"
        else:
            # if someone runs a diff job with no system packages, lets use different job name
            if schedule_type == "diff":
                alias_suffix += "-nosystem"

    extra_build_arguments = system_arguments + "--schedule-type " + schedule_type
    extra_test_arguments = system_arguments + "--schedule-type " + schedule_type

    if owner.extra_cmake_defines:
        extra_build_arguments += " --extra-cmake-defines='%s'" % json.dumps(
            owner.extra_cmake_defines
        )
        extra_test_arguments += " --extra-cmake-defines='%s'" % json.dumps(
            owner.extra_cmake_defines
        )

    if owner.shared_libs:
        extra_build_arguments += " --shared-libs"
        extra_test_arguments += " --shared-libs"

    if owner.extra_build_args:
        extra_build_arguments += " %s" % json.dumps(owner.extra_build_args).strip('"')

    if owner.extra_test_opts:
        extra_test_arguments = " %s" % json.dumps(owner.extra_test_opts).strip('"')

    # Always start clean: while sandcastle should start afresh each
    # time, when it runs the build again on the base rev it won't be
    # clean.  Forcing a clean state helps to squash out state invalidation
    # bugs in getdeps which crop up from time to time.
    clean_step_name = "Clean getdeps state"
    steps.append(
        {
            "name": clean_step_name,
            "user": user,
            "shell": "fbpython %s clean" % getdeps,
        }
    )

    if build_opts.facebook_internal:
        facebook_internal_arg = "--facebook-internal"
    else:
        facebook_internal_arg = "--no-facebook-internal"

    # First pass for fetching so we can measure fetch time
    # separately from build time
    fetch_step_names = []
    for m in projects:
        fetch_name = "Fetch %s" % m.name
        fetch_step_names.append(fetch_name)
        steps.append(
            {
                "name": fetch_name,
                "user": user,
                "shell": "fbpython %s fetch %s%s" % (getdeps, system_arguments, m.name),
                "env": env,
                # Downloading from LFS is sometimes flakey.
                # Downloading from the public internet is much more
                # likely to be flakey.
                "retries": 3,
                # Fetches are independent, only depend on the clean step
                # this allows them to run in parallel
                "dependencies": [clean_step_name],
            }
        )

    # Terrible hack to fix PATH on lego-mac machines. This makes
    # sure the system clang binary wins over the out of date
    # homebrew clang binary.
    # Also sets newer nix-based cmake to be used over the default older version
    mac_path_override = [
        # This must be a fixed path to avoid invalidating getdeps's
        # build caches every time.
        #
        # On macOS, TMPDIR has a randomly-generated name, but our Mac Minis in
        # the fleet are imaged such that they all have a consistent TMPDIR.
        'OVERRIDE_PATH_DIR="$TMPDIR/clang-override"',
        'mkdir -p "$OVERRIDE_PATH_DIR"',
        'ln -sf /usr/bin/clang "$OVERRIDE_PATH_DIR"',
        'ln -sf /usr/local/bin/cmake "$OVERRIDE_PATH_DIR"',
        'ln -sf /usr/local/bin/ctest "$OVERRIDE_PATH_DIR"',
        'export PATH="$OVERRIDE_PATH_DIR:$PATH"',
    ]

    run_tests = manifest.get("sandcastle", "run_tests", defval="on") != "off"
    next_step_deps = fetch_step_names
    # Now the builds
    for m in projects:
        # Only build tests for the leaf project.
        # getdeps will normally do this by default when invoked with just a specific
        # project, but since we explicitly invoke it for each dependency we need to
        # pass in --no-tests to tell it that they aren't really the leaf projects
        # we care about.
        test = ""
        if m.name != manifest.name or not run_tests:
            test = "--no-tests"

        # If build target is specified, it will be configured for the project and its
        # dependencies. Few dependencies (for eg, CLI1) do not have allow cmake-target
        # and hance will fail compilation due to "unknown target" failure.
        # build_target_manifest ensures that the cmake-target is set only when the
        # project manifest matches with build_target_manifest.
        extra_build_args = extra_build_arguments
        if owner.build_target and owner.build_target_manifest == m.name:
            extra_build_args += " --cmake-target=%s" % (owner.build_target)

        build_cmd = "fbpython %s build %s %s %s %s" % (
            getdeps,
            extra_build_args,
            facebook_internal_arg,
            test,
            m.name,
        )

        if build_opts.is_darwin():
            build_cmd = " && ".join([*mac_path_override, build_cmd])

        build_step_name = "Build %s" % m.name
        steps.append(
            {
                "name": build_step_name,
                "user": user,
                "shell": build_cmd,
                "dependencies": next_step_deps,
            }
        )
        next_step_deps = [build_step_name]

    oncall = owner.oncall or getattr(args, "oncall", None)
    if oncall is None:
        return None
    tenant = owner.tenant or DEFAULT_TENANT
    test_oncall = None

    if run_tests:
        # and finally, test the leaf project
        test_oncall = oncall
        test_owner = ("--test-owner " + test_oncall) if test_oncall else ""

        test_cmd = "fbpython %s test %s %s %s %s" % (
            getdeps,
            extra_test_arguments,
            test_owner,
            facebook_internal_arg,
            manifest.name,
        )

        if build_opts.is_darwin():
            test_cmd = " && ".join([*mac_path_override, test_cmd])

        test_step_name = "Test %s" % manifest.name
        steps.append(
            {
                "name": test_step_name,
                "user": user,
                "shell": test_cmd,
                "dependencies": next_step_deps,
            }
        )
        next_step_deps = [test_step_name]

    capabilities["tenant"] = FBCODE_CASTLE_TENANT if build_opts.is_darwin() else tenant

    job_alias_suffix = "-%s" % (owner.job_suffix) if owner.job_suffix else ""

    spec = {
        "command": "SandcastleUniversalCommand",
        "alias": "%s-%s-%s-%s%s"
        % (
            prefix,
            manifest.name,
            build_opts.host_type.ostype,
            alias_suffix,
            job_alias_suffix,
        ),
        "args": {
            "steps": steps,
            "env": env,
            "oncall": oncall,
            "skip_if_error_exists_on_base_revision": True,
        },
        "capabilities": capabilities,
        "tags": [manifest.name, "getdeps", "oss"],
        "oncall": oncall,
        "priority": args.priority,
        "scheduleType": schedule_type,
    }

    # File tasks for the owning oncall if the continuous build is broken and that
    # is enabled for a project.  It's not on by default because some projects
    # (eg: folly, wangle) are co-owned and don't necessarily have a POC.
    spec["args"]["report"] = []
    if (schedule_type == "continuous") and (test_oncall is not None) and owner.tasks:
        spec["args"]["report"] = [
            {
                "type": "task",
                "oncall": test_oncall,
                "triggers": ["fail", "infra_fail"],
                "tags": [manifest.name, "getdeps", "oss"],
                # high priority == 2 (UBN is 1, Wish is 5)
                "priority": 2,
            }
        ]

    # Add diff-time alerts
    if schedule_type == "diff" and owner.diff_time_alerts:
        spec["args"]["report"].append(owner.diff_time_alerts)

    if build_opts.is_windows():
        spec["args"]["shell_type"] = "SandcastleRemotePowershellWindows"
    tag = getattr(args, "tags", None)
    if tag:
        spec["tags"].append(tag)
    return spec


@cmd("sandcastle", "build a given project on sandcastle")
class SandcastleCmd(SubCmd):
    # pyre-fixme[15]: `run` overrides method defined in `SubCmd` inconsistently.
    def run(self, args) -> None:
        specs = []
        for p in PLATFORMS:
            if args.os_types and p.ostype not in args.os_types:
                continue
            if p.distro and args.distros and p.distro not in args.distros:
                continue
            for project in args.project:
                # Find ownership information for the project.
                owners = get_owners_for_project(project)
                if len(owners) == 0:
                    print(
                        "project %s has no ownernship definition; skipping" % project,
                        file=sys.stderr,
                    )
                for owner in owners:
                    extra_specs = owner.jobspecs(args, p)
                    if not owner.oncall:
                        print(
                            "project %s has no oncall; skipping" % project,
                            file=sys.stderr,
                        )
                    if extra_specs:
                        specs += extra_specs
                    else:
                        print(
                            "project %s has nop builder; skipping" % project,
                            file=sys.stderr,
                        )
        if args.dry_run:
            print(json.dumps(specs, indent=4))
        else:
            subprocess.call(["jf", "sandcastle", "--spec", json.dumps(specs)])

    def setup_parser(self, parser) -> None:
        parser.add_argument(
            "project",
            nargs="+",
            help=(
                "name of the project or path to a manifest "
                "file describing the project"
            ),
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            default=False,
            help="Don't schedule, just print the job specs we would schedule",
        )
        parser.add_argument(
            "--oncall",
            default=os.environ.get("USER", None),
            help="name of the oncall to set on the jobs",
        )
        parser.add_argument(
            "--priority", type=int, help="set the job scheduling priority", default=3
        )
        parser.add_argument(
            "--job-name-prefix",
            type=str,
            help="add a prefix to all job names, useful if job disabled by SignalQuality",
            default=None,
        )
        parser.add_argument(
            "--schedule-type",
            help="Indicates how the build was activated",
            default="diff",
        )
        parser.add_argument(
            "--os-type",
            help="Filter to just this OS type to run",
            choices=["linux", "darwin", "windows"],
            action="append",
            dest="os_types",
            default=[],
        )
        parser.add_argument(
            "--distro",
            help="Filter to just this distro to run",
            choices=["centos_stream"],
            action="append",
            dest="distros",
            default=[],
        )
        parser.add_argument(
            "--disallow-system-packages",
            help="Disallow satisfying third party deps from installed system packages. System packages make builds faster, so they are default at diff time",
            action="store_true",
            default=False,
        )


def modified_files_in_commit(build_opts, stack: bool = False):
    cmd = ["hg", "status", "-n", "--rev"]
    if stack:
        cmd.append("bottom^")
    else:
        cmd.append(".^")
    output = subprocess.check_output(cmd, cwd=build_opts.fbsource_dir)
    return output.strip().decode("utf-8").split("\n")


class DeterminatorData(object):
    """Builds up some mappings that are helpful to evaluate
    reverse dependency information so that we can map from
    a file name to a set of impacted projects"""

    def __init__(self, opts) -> None:
        ctx_gen = opts.get_context_generator()
        self.loader = ManifestLoader(opts, ctx_gen)
        self.fb_manifests_by_name = self.load_fb_manifests(opts)
        self.mapping = self.build_filename_to_project_mapping(
            opts, self.fb_manifests_by_name
        )
        self.rdeps = self.build_rdeps(opts)
        self._debug = False

    def affected_projects(self, modified_files):
        """Given a list of file names, returns a list of projects
        that are impacted by changes to those files"""
        why = []

        directly_affected = set()
        for file_name in modified_files:
            if self.matches_default_exclusion(file_name):
                why.append((file_name, "excluded from all"))
                continue

            for pattern, projects in self.mapping.items():
                if not re.match(pattern, file_name):
                    why.append((file_name, "not match", pattern))
                    continue

                for p in projects:
                    if p in directly_affected:
                        # Don't bother checking for exclusions if this project is
                        # already affected due to other files.
                        continue
                    if self.is_excluded(p, file_name):
                        why.append((file_name, "excluded from ", p))
                        continue
                    directly_affected.add(p)

        affected_projects = set()
        for p in directly_affected:
            affected_projects.add(p)
            rdeps = self.rdeps.get(p, [])
            for rdep in rdeps:
                affected_projects.add(rdep)

        if self._debug:
            print("why: %r" % why, file=sys.stderr)
        return sorted(affected_projects)

    def matches_default_exclusion(self, file_name) -> bool:
        for exc in DEFAULT_SHIPIT_EXCLUSIONS:
            if re.match(exc, file_name):
                return True
        return False

    def is_excluded(self, project_name, file_name) -> bool:
        """Returns true if file_name is excluded by the mapping data"""
        m = self.fb_manifests_by_name[project_name]
        ctx = self.loader.ctx_gen.get_context(m.name)
        for exc in m.get_section_as_args("shipit.strip", ctx):
            if re.match(exc, file_name):
                return True
        return False

    def load_fb_manifests(self, opts):
        """Load just the FB specific manifest files"""
        fb_manifests_by_name = {}
        for m in self.loader.load_all_manifests().values():
            if m.shipit_project:
                fb_manifests_by_name[m.name] = m

        return fb_manifests_by_name

    def build_rdeps(self, opts):
        """Traverse the dependency graph and build the reverse mapping"""
        rdeps = {}

        def add_mapping(k, v):
            dep_list = rdeps.get(k)
            if not dep_list:
                dep_list = set()
                rdeps[k] = dep_list
            dep_list.add(v)

        for m in self.fb_manifests_by_name.values():
            for p in self.loader.manifests_in_dependency_order(m):
                # only care about FB projects, and since the list
                # of projects includes `m`, exclude it here.
                if p != m and p.name in self.fb_manifests_by_name:
                    add_mapping(p.name, m.name)

        for k in rdeps:
            rdeps[k] = sorted(rdeps[k])

        return rdeps

    def build_filename_to_project_mapping(self, opts, manifests_by_name):
        """Extract the shipit.pathmap data from the manifests and build
        a mapping from path -> list of projects"""
        mapping = {}

        def add_mapping(pattern, project):
            project_list = mapping.get(pattern)
            if not project_list:
                project_list = set()
                mapping[pattern] = project_list

            project_list.add(project)

        for m in manifests_by_name.values():
            if m.shipit_fbcode_builder:
                add_mapping("fbcode/opensource/fbcode_builder", m.name)

            ctx = self.loader.ctx_gen.get_context(m.name)
            for src, _dest in m.get_section_as_ordered_pairs("shipit.pathmap", ctx):
                add_mapping("%s/.*" % src, m.name)

        for k in mapping:
            mapping[k] = sorted(mapping[k])

        return mapping


@cmd("determinator", "emit sandcastle job specs for the current commit")
class DeterminatorCmd(SubCmd):
    def run(self, args) -> int:
        opts = setup_build_options(args)
        if args.changed_files_list:
            with open(args.changed_files_list, "r") as f:
                modified_files = [line.strip() for line in f]
        elif args.changed_files is None:
            modified_files = modified_files_in_commit(opts, stack=args.stack)
        else:
            modified_files = args.changed_files
        data = DeterminatorData(opts)
        if args.schedule_type == "testwarden":
            # The list of changed files isn't useful wrt. stress runs, so
            # we always schedule all projects for testwarden runs
            projects = data.fb_manifests_by_name.keys()
        else:
            projects = data.affected_projects(modified_files)
        specs = []
        args.priority = 3
        for p in PLATFORMS:
            for project in projects:
                owners = get_owners_for_project(project)
                for owner in owners:
                    extra_specs = owner.jobspecs(args, p)
                    if extra_specs:
                        specs += extra_specs

        print(json.dumps(specs, indent=4))
        return 0

    def setup_parser(self, parser) -> None:
        parser.add_argument(
            "--stack",
            action="store_true",
            default=False,
            help="Consider the commit stack rather than the current commit",
        )
        parser.add_argument(
            "--schedule-type", help="Indicates how the build was activated"
        )
        parser.add_argument(
            "--changed-files",
            nargs="+",
            help=(
                "Specify the list of changed files to use, instead of "
                "inspecting the local commits"
            ),
        )
        parser.add_argument(
            "--changed-files-list",
            help=(
                "Name of a file that lists the changed files to use, "
                "instead of inspecting the local commits"
            ),
        )
        parser.add_argument(
            "--disallow-system-packages",
            help="Disallow satisfying third party deps from installed system packages.",
            action="store_true",
            default=False,
        )
        parser.add_argument(
            "--job-name-prefix",
            type=str,
            help="add a prefix to all job names, useful if job disabled by SignalQuality",
            default=None,
        )
