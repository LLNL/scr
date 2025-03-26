#!/usr/bin/env python3
#
#  Generate a build matrix for use with github workflows
#

from copy import deepcopy
import json
import os
import re
import pathlib

docker_run_checks = pathlib.Path("src/test/docker/docker-run-checks.sh")

default_args = ""

DOCKER_REPO = "libscr/scr"


def on_master_or_tag(matrix):
    return matrix.branch == "master" or matrix.tag


DEFAULT_MULTIARCH_PLATFORMS = {
    "linux/arm64": {
        "when": lambda _: True,
        "suffix": " - arm64",
        "timeout_minutes": 90,
        "runner": "ubuntu-24.04-arm",
    },
    "linux/amd64": {"when": lambda _: True, "suffix": " - amd64", "runner": "ubuntu-latest"},
}


class BuildMatrix:
    def __init__(self):
        self.matrix = []
        self.branch = None
        self.tag = None

        #  Set self.branch or self.tag based on GITHUB_REF
        if "GITHUB_REF" in os.environ:
            self.ref = os.environ["GITHUB_REF"]
            match = re.search("^refs/heads/(.*)", self.ref)
            if match:
                self.branch = match.group(1)
            match = re.search("^refs/tags/(.*)", self.ref)
            if match:
                self.tag = match.group(1)

    def create_docker_tag(self, image, env, command, platform):
        """Create docker tag string if this is master branch or a tag"""
        if self.branch == "master" or self.tag:
            tag = f"{DOCKER_REPO}:{image}"
            if self.tag:
                tag += f"-{self.tag}"
            if platform is not None:
                tag += "-" + platform.split("/")[1]
            env["DOCKER_TAG"] = tag
            command += f" --tag={tag}"
            return True, command

        return False, command

    def add_build(
        self,
        name=None,
        image=None,
        args=default_args,
        jobs=6,
        env=None,
        docker_tag=False,
        platform=None,
        command_args="",
        timeout_minutes=60,
        runner="ubuntu-latest",
    ):
        """Add a build to the matrix.include array"""

        # Extra environment to add to this command:
        # NOTE: ensure we copy the dict rather than modify, re-used dicts can cause
        #       overwriting
        env = dict(env) if env is not None else {}

        needs_buildx = False
        if platform:
            command_args += f"--platform={platform}"
            needs_buildx = True

        # The command to run:
        command = f"{docker_run_checks} -j{jobs} --image={image} {command_args}"

        if docker_tag:
            #  Only export docker_tag if this is main branch or a tag:
            docker_tag, command = self.create_docker_tag(image, env, command, platform)

        create_release = False
        if self.tag and "DISTCHECK" in env:
            create_release = True

        command += f" -- {args}"

        self.matrix.append(
            {
                "name": name,
                "env": env,
                "command": command,
                "image": image,
                "runner": runner,
                "tag": self.tag,
                "branch": self.branch,
                "docker_tag": docker_tag,
                "needs_buildx": needs_buildx,
                "create_release": create_release,
                "timeout_minutes": timeout_minutes,
            }
        )

    def add_multiarch_build(
        self,
        name: str,
        platforms=DEFAULT_MULTIARCH_PLATFORMS,
        default_suffix="",
        image=None,
        docker_tag=True,
        **kwargs,
    ):
        for p, args in platforms.items():
            if args["when"](self):
                suffix = args.get("suffix", default_suffix)
                self.add_build(
                    name + suffix,
                    platform=p,
                    docker_tag=docker_tag,
                    image=image if image is not None else name,
                    command_args=args.get("command_args", ""),
                    timeout_minutes=args.get("timeout_minutes", 30),
                    runner=args["runner"],
                    **kwargs,
                )

    def __str__(self):
        """Return compact JSON representation of matrix"""
        return json.dumps(
            {"include": self.matrix}, skipkeys=True, separators=(",", ":")
        )


matrix = BuildMatrix()


for name in ("bookworm", "alpine", "fedora40"):
    matrix.add_multiarch_build(name=name)

matrix.add_build(
    name="el9 - arm64",
    image="el9",
    platform="linux/arm64",
    runner="ubuntu-24.04-arm",
    docker_tag=True
)


print(matrix)
