# pyre-unsafe
import getpass
import json
import socket
import sys
import time
from contextlib import contextmanager
from http.client import HTTPSConnection
from typing import Any, Callable, Dict, Generator, Optional, TypeVar
from urllib.parse import urlencode

# https://www.internalfb.com/intern/wiki/XcontrollerGuide/XInternGraphController/#Creating_an_App_ID_and_Token
APP_ID = 638842582810757
TOKEN = "AeNl7KJNqzNeh2vKd1U"

# https://www.internalfb.com/intern/wiki/Test_Infra/Custom_Integrations/Result_Recorder/


def send_request(data: Dict[str, Any]) -> Dict[str, str]:
    host = "interngraph.scgraph.facebook.com"
    endpoint = "/testinfra/record_results"

    args = {"app": APP_ID, "token": TOKEN, "input": data}

    conn = HTTPSConnection(host)
    conn.request("POST", endpoint, body=urlencode(args))

    response = conn.getresponse()
    return json.load(response)


def record_start(commit_hash: str) -> str:
    data = {
        "runData": {
            "startedTime": str(int(time.time())),
            "stillRunning": "true",
            "hostname": socket.gethostname(),
            "username": getpass.getuser(),
            "repository": "ssh://hg.vip.facebook.com//data/scm/fbsource",
            "gitRevision": commit_hash,
            "tags": [],
        }
    }

    started_run = send_request(data)
    return str(started_run["runId"])


def record_stop(run_id: str) -> None:
    data = {
        "runData": {
            "stillRunning": "false",
        },
        "runId": run_id,
    }
    send_request(data)


T = TypeVar("T")


def retry(times: int, fn: Callable[[], T]) -> T:
    tries = 0
    while True:
        try:
            return fn()
        except Exception:
            tries += 1
            if tries < times:
                continue
            else:
                raise


@contextmanager
def start_run(commit_hash: str) -> Generator[Optional[str], None, None]:
    try:
        run_id = retry(2, lambda: record_start(commit_hash))
    except Exception as exc:
        # Probably IOError if the request fails, JSONDecodeError if the response isn't
        # JSON, or KeyError if the response doesn't contain "runId".
        print("error recording test session start: {}", exc, file=sys.stderr)
        print("continuing run anyway", file=sys.stderr)
        yield None
        return

    try:
        yield run_id
    finally:
        try:
            retry(2, lambda: record_stop(run_id))
        except Exception as exc:
            print("error recording test session stop: {}", exc, file=sys.stderr)
