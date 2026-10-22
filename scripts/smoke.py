#!/usr/bin/env python3
"""Rehearse real nodes, mining, a host outage, forks, replay, and convergence.

Build first: go build -o /tmp/blockchain-go .
Run: python3 scripts/smoke.py --binary /tmp/blockchain-go
All state and logs live in a temporary directory; no tunnel is required.
"""
import argparse
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import time
import urllib.request


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True)
    args = parser.parse_args()
    binary = str(Path(args.binary).resolve())
    process_env = dict(os.environ)
    process_env.pop("BLOCKCHAIN_PEER_TOKEN", None)
    with tempfile.TemporaryDirectory(prefix="blockchain-smoke-") as directory:
        root = Path(directory)
        processes, logs = {}, []
        reservations = []
        for _ in range(3):
            sock = socket.socket()
            sock.bind(("127.0.0.1", 0))
            reservations.append(sock)
        ports = [sock.getsockname()[1] for sock in reservations]
        for sock in reservations:
            sock.close()
        names = ["alice", "bob", "carol"]
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

        def status(index):
            request = urllib.request.Request(
                f"http://127.0.0.1:{ports[index]}/status",
            )
            with opener.open(request, timeout=1) as response:
                return json.load(response)

        def wait(condition, description):
            deadline = time.monotonic() + 25
            while time.monotonic() < deadline:
                for index, process in processes.items():
                    if process.poll() is not None:
                        raise RuntimeError(f"{names[index]} exited: {process.returncode}")
                try:
                    result = condition()
                    if result:
                        return result
                except (OSError, ValueError):
                    pass
                time.sleep(0.05)
            raise RuntimeError(f"Timed out: {description}")

        def start(index, mining):
            logfile = open(root / f"{names[index]}.log", "ab")
            logs.append(logfile)
            command = [binary, "--listen", f"127.0.0.1:{ports[index]}",
                       "--data-dir", str(root / names[index]), "--miner", names[index],
                       "--workers", "1", "--initial-bits", "18",
                       f"--mine={'true' if mining else 'false'}"]
            if index:
                command += ["--peer", f"ws://127.0.0.1:{ports[0]}/p2p"]
            processes[index] = subprocess.Popen(command, env=process_env, stdout=logfile, stderr=logfile)
            wait(lambda: status(index), f"{names[index]} startup")

        def stop(index):
            process = processes.pop(index)
            process.terminate()
            try:
                code = process.wait(timeout=8)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
                raise RuntimeError(f"{names[index]} did not shut down gracefully")
            if code != 0:
                raise RuntimeError(f"{names[index]} shutdown exit status {code}")

        try:
            start(0, True)
            start(1, True)
            wait(lambda: status(0)["peers"] == 1 and status(1)["height"] >= 3,
                 "two miners exchanging blocks")
            start(2, True)
            wait(lambda: status(0)["peers"] == 2 and status(2)["height"] >= 3,
                 "late joiner synchronized")
            stop(0)
            wait(lambda: status(1)["peers"] == 0 and status(2)["peers"] == 0,
                 "host outage partitions friends")
            before = [None, status(1)["height"], status(2)["height"]]
            wait(lambda: all(status(i)["height"] >= before[i] + 2 for i in (1, 2)),
                 "both isolated friends continue mining")
            stop(1)
            stop(2)
            for i in range(3):
                start(i, False)
            wait(lambda: status(0)["peers"] == 2, "peers reconnect after restart")
            # Resolve a possible equal-work tie by extending a synchronized branch.
            largest_work = max(int(status(i)["work"]) for i in range(3))
            stop(0)
            start(0, True)
            wait(lambda: int(status(0)["work"]) > largest_work,
                 "new block resolves partition fork")
            stop(0)
            start(0, False)

            def converged():
                snapshots = [status(i) for i in range(3)]
                return snapshots if len({s["tip"] for s in snapshots}) == 1 else None

            snapshots = wait(converged, "all three persisted chains converge")
            for snapshot in snapshots:
                assert sum(snapshot["wins"].values()) == snapshot["height"]
            assert all(s["wins"] == snapshots[0]["wins"] for s in snapshots)
            print("PASS: three processes mined, synchronized, survived host outage,")
            print("      mined isolated forks, replayed storage, and converged after restart.")
            print(f"Final height: {snapshots[0]['height']}; wins: {snapshots[0]['wins']}")
        except Exception:
            for name in names:
                path = root / f"{name}.log"
                if path.exists():
                    print(f"Last log lines for {name}:")
                    print("\n".join(path.read_text(errors="replace").splitlines()[-8:]))
            raise
        finally:
            for process in processes.values():
                if process.poll() is None:
                    process.terminate()
            for process in processes.values():
                try:
                    process.wait(timeout=8)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
            for logfile in logs:
                logfile.close()


if __name__ == "__main__":
    main()
