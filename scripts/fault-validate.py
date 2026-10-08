#!/usr/bin/env python3
"""Run isolated storage fault tests against a content-addressed source copy."""
import argparse
import base64
import hashlib
import json
import os
import re
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import tarfile
import time
import zipfile


def inventory(root):
    files = {}
    for path in sorted(root.rglob("*")):
        relative = path.relative_to(root)
        if any(part.startswith(".") or part in ("build", "__pycache__", "node_modules") for part in relative.parts) or str(relative).startswith("tests/faults/evidence/"):
            continue
        if path.is_file() and path.suffix in (".go", ".mod", ".sum", ".json", ".proto", ".sql", ".md", ".py", ".s", ".syso", ".h", ".c"):
            if path.is_symlink():
                raise RuntimeError(f"source symlink refused: {relative}")
            files[str(relative)] = hashlib.sha256(path.read_bytes()).hexdigest()
    return files


def digest(files):
    encoded = json.dumps(files, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def go_content_sum(files):
    # Go module dirhash.Hash1: SHA256 of sorted per-file SHA256/name lines.
    lines = "".join(f"{sha}  {name}\n" for name, sha in sorted(files.items()))
    return "h1:" + base64.b64encode(hashlib.sha256(lines.encode()).digest()).decode()


def verify_used_modules(work, modules):
    verified = []
    cache = work / "module-cache"
    for item in modules:
        if "zip_sha256" not in item:
            continue
        escaped = "".join("!" + c.lower() if c.isupper() else c for c in item["Path"])
        version = item["Version"]
        zipped = cache / "cache" / "download" / escaped / "@v" / (version + ".zip")
        prefix = item["Path"] + "@" + version + "/"
        with zipfile.ZipFile(zipped) as archive:
            zip_files = {n: hashlib.sha256(archive.read(n)).hexdigest() for n in archive.namelist() if not n.endswith("/")}
        root = cache / (escaped + "@" + version)
        if not root.is_dir():
            raise RuntimeError(f"used dependency was not extracted: {item['Path']}")
        dir_files = {}
        for path in root.rglob("*"):
            if path.is_symlink():
                raise RuntimeError("module cache symlink refused")
            if path.is_file():
                dir_files[prefix + str(path.relative_to(root))] = hashlib.sha256(path.read_bytes()).hexdigest()
        if go_content_sum(zip_files) != item["Sum"] or go_content_sum(dir_files) != item["Sum"]:
            raise RuntimeError(f"used dependency source checksum mismatch: {item['Path']}")
        verified.append({"path": item["Path"], "version": version, "sum": item["Sum"], "zip_and_extracted_source_verified": True})
    return verified


def json_objects(raw):
    decoder = json.JSONDecoder()
    while raw.strip():
        value, end = decoder.raw_decode(raw.lstrip())
        yield value
        raw = raw.lstrip()[end:]


def fixed_modules(snapshot, work, output, env, cache):
    """Seed a private cache from checksum-bound archives, never a SDK checkout."""
    lookup = dict(env, GOPROXY="off", GOMODCACHE=str(cache))
    spec = json.loads(subprocess.check_output(["go", "mod", "edit", "-json"], cwd=snapshot, env=lookup, text=True, timeout=10))
    if spec.get("Replace"):
        raise RuntimeError("fault validation requires immutable modules; go.mod replacements refused")
    required = {m["Path"]: m["Version"] for m in spec["Require"]}
    sdk_path = "github.com/XGC-Team/xgc2-xrpc/go"
    if sdk_path not in required:
        raise RuntimeError("fixed XRPC Go module version required")
    listing = output / "dependency-list.jsonl"
    if execute(["go", "list", "-deps", "-test", "-json", "./cmd/...", "./tests/faults"], snapshot, lookup, listing, 60):
        raise RuntimeError("cached pinned dependencies unavailable; see dependency-list.jsonl")
    used = {p["Module"]["Path"]: p["Module"]["Version"]
            for p in json_objects(listing.read_text())
            if p.get("Module", {}).get("Version")}
    required.update(used)
    private = work / "module-cache"
    modules, artifacts = [], {}
    sums = set((snapshot / "go.sum").read_text().splitlines())
    for path, version in sorted(required.items()):
        result = subprocess.run(["go", "mod", "download", "-json", f"{path}@{version}"], cwd=snapshot,
                                env=lookup, capture_output=True, text=True, timeout=30)
        item = json.loads(result.stdout)
        if path in used and (result.returncode or not item.get("Zip")):
            raise RuntimeError(f"cached archive required for {path}@{version}")
        identity = {k: item[k] for k in ("Path", "Version", "Sum", "GoModSum") if k in item}
        for key, suffix in (("Sum", ""), ("GoModSum", "/go.mod")):
            if key in item and f"{path} {version}{suffix} {item[key]}" not in sums:
                raise RuntimeError(f"dependency checksum absent from fixed go.sum: {path}{suffix}")
        for key in ("Info", "GoMod", "Zip"):
            if key not in item or (key == "Zip" and path not in used):
                continue
            source = Path(item[key])
            if not source.is_file() or source.is_symlink():
                raise RuntimeError(f"regular cached dependency artifact required: {path} {key}")
            relative = source.relative_to(cache)
            target = private / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, target)
            sha = hashlib.sha256(target.read_bytes()).hexdigest()
            if hashlib.sha256(source.read_bytes()).hexdigest() != sha:
                raise RuntimeError(f"dependency changed during copy: {path}")
            artifacts[str(relative)] = sha
            identity[f"{key.lower()}_sha256"] = sha
            if path == sdk_path and key == "Zip":
                with zipfile.ZipFile(target) as archive:
                    prefix = f"{path}@{version}/"
                    sdk_files = {name.removeprefix(prefix): hashlib.sha256(archive.read(name)).hexdigest()
                                 for name in archive.namelist() if not name.endswith("/")}
            if key == "Zip":
                with zipfile.ZipFile(target) as archive:
                    files = {n: hashlib.sha256(archive.read(n)).hexdigest() for n in archive.namelist() if not n.endswith("/")}
                if go_content_sum(files) != item["Sum"]:
                    raise RuntimeError(f"cached dependency archive checksum mismatch: {path}")
            if key == "GoMod" and "GoModSum" in item and go_content_sum({"go.mod": sha}) != item["GoModSum"]:
                raise RuntimeError(f"cached module metadata checksum mismatch: {path}")
        modules.append(identity)
    env["GOMODCACHE"] = str(private)
    env["GOPROXY"] = "off"
    return modules, artifacts, sdk_files


def execute(command, cwd, env, log, timeout):
    # Only these private children receive termination on timeout. No pid searches.
    with log.open("wb") as stream:
        child = subprocess.Popen(command, cwd=cwd, env=env, stdout=stream,
                                 stderr=subprocess.STDOUT, start_new_session=True)
        try:
            return child.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            stream.write(b"\nprivate validation process group timed out\n")
            return 124
        finally:
            # Also reap a daemon left by a failed Go assertion/test timeout.
            # This group was created by this runner, never discovered by name.
            try:
                os.killpg(child.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            child.wait()


def remove_private_source(work):
    # Go extracts immutable modules with readonly directories. This tree was
    # allocated by this invocation; make only its directories removable.
    for root, _, _ in os.walk(work):
        os.chmod(root, 0o700)
    shutil.rmtree(work)


def cleanup_containers(output):
    ledger = output / "owned-containers.jsonl"
    results = []
    if not ledger.exists():
        return results
    names = set()
    for line in ledger.read_text().splitlines():
        entry = json.loads(line)
        name = entry["name"]
        if not re.fullmatch(r"sol20-fault-\d+-\d+", name):
            raise RuntimeError("invalid container ownership entry")
        names.add(name)
    for name in sorted(names):
        try:
            result = subprocess.run(["docker", "rm", "-f", name], capture_output=True, text=True, timeout=8)
            absent = "No such container" in result.stderr
            results.append({"name": name, "released": result.returncode == 0 or absent,
                            "diagnostic": (result.stdout + result.stderr).strip()})
        except subprocess.TimeoutExpired:
            results.append({"name": name, "released": False, "diagnostic": "owned container removal timed out"})
    return results


def main():
    def interrupted(signum, _frame):
        raise InterruptedError(f"validation interrupted by signal {signum}")
    signal.signal(signal.SIGTERM, interrupted)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, help="new evidence directory; defaults to a private /tmp directory")
    parser.add_argument("--run", default="^TestFault", help="Go test name expression")
    parser.add_argument("--race", action="store_true", help="instrument the independent test binary")
    parser.add_argument("--container-image", help="existing local image for private readonly/tmpfs mounts; never pulled")
    parser.add_argument("--module-cache", type=Path, help="cached immutable module archives; no network downloads")
    parser.add_argument("--expected-implementation-sha256", help="refuse a different storage implementation content hash")
    args = parser.parse_args()
    storage = Path(__file__).resolve().parent.parent
    cache = args.module_cache
    if cache is None:
        cache = Path(subprocess.check_output(["go", "env", "GOMODCACHE"], text=True, timeout=10).strip())
    if not cache.is_dir():
        parser.error("module cache does not exist; prepare fixed dependencies before validation")
    cache = cache.resolve()
    if args.output:
        output = args.output.absolute()
        if output.is_relative_to(storage) or output.is_relative_to(cache):
            parser.error("evidence must be outside source and module cache")
        output.mkdir(mode=0o700, parents=False, exist_ok=False)
    else:
        output = Path(tempfile.mkdtemp(prefix="storage-fault-evidence-"))
    print(f"evidence={output}", flush=True)
    report = {"format": "storage-fault-evidence-v1", "started_unix": time.time(),
              "command": sys.argv, "race": args.race, "run": args.run,
              "scope": "private temporary databases and child processes; no existing database inputs"}
    work = Path(tempfile.mkdtemp(prefix="storage-fault-source-"))
    report["private_source_directory"] = str(work)
    try:
        snapshot = work / "storage"
        before = {"storage": inventory(storage)}
        for name, root, destination in (("storage", storage, snapshot),):
            for relative in before[name]:
                target = destination / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(root / relative, target)
            if inventory(destination) != before[name] or inventory(root) != before[name]:
                raise RuntimeError(f"{name} changed while copying; rerun against a stable batch")
        report["source_files"] = before
        report["storage_source_sha256"] = digest(before["storage"])
        implementation = {k: v for k, v in before["storage"].items()
                          if not k.startswith(("tests/faults/", "scripts/fault-validate"))
                          and k != "contracts/fault-evidence.md"}
        report["storage_implementation_sha256"] = digest(implementation)
        if args.expected_implementation_sha256 and args.expected_implementation_sha256 != report["storage_implementation_sha256"]:
            raise RuntimeError("storage implementation differs from requested fixed hash")
        report["suite_sha256"] = digest({k: v for k, v in before["storage"].items()
                                         if k.startswith(("tests/faults/", "scripts/fault-validate"))})
        report["go_version"] = subprocess.check_output(["go", "version"], text=True, timeout=10).strip()
        report["kernel"] = os.uname().release
        env = os.environ.copy()
        env["GOMAXPROCS"] = "2"
        env["GOFLAGS"] = "-mod=readonly"
        env["GOWORK"] = "off"
        env["TMPDIR"] = str(work)
        env["FAULT_STORAGE_BIN"] = str(work / "xgc2-storage")
        env["FAULT_STORAGE_ADMIN_BIN"] = str(work / "storage-admin")
        env["FAULT_EVIDENCE_DIR"] = str(output)
        env["FAULT_CONTAINER_LEDGER"] = str(output / "owned-containers.jsonl")
        modules, artifacts, sdk_files = fixed_modules(snapshot, work, output, env, cache)
        report["dependency_modules"] = modules
        report["dependency_artifacts"] = artifacts
        report["source_files"]["xrpc_go"] = sdk_files
        report["xrpc_go_source_sha256"] = digest(sdk_files)
        sdk_cache_prefix = "cache/download/github.com/!x!g!c-!team/xgc2-xrpc/go/"
        archived_dependencies = {k: v for k, v in artifacts.items() if k.startswith(sdk_cache_prefix)}
        report["archived_dependency_artifacts"] = archived_dependencies
        report["archive_dependency_requirement"] = "other exact go.mod/go.sum dependencies must be cached; no network downloads during validation"
        archive = output / "source.tar.gz"
        with tarfile.open(archive, "w:gz") as tar:
            for relative in before["storage"]:
                tar.add(snapshot / relative, arcname=f"storage/{relative}", recursive=False)
            for relative in archived_dependencies:
                tar.add(work / "module-cache" / relative, arcname=f"module-cache/{relative}", recursive=False)
        report["source_archive_sha256"] = hashlib.sha256(archive.read_bytes()).hexdigest()
        if args.container_image:
            image = subprocess.check_output(["docker", "image", "inspect", "--format", "{{.Id}}", args.container_image], text=True, timeout=8).strip()
            env["FAULT_CONTAINER_IMAGE"] = image
            report["container_image_id"] = image
        build = ["go", "build", "-p=2", "-o", env["FAULT_STORAGE_BIN"], "./cmd/xgc2-storage"]
        report["build_command"] = build
        build_env = dict(env, CGO_ENABLED="0")
        report["build_exit"] = execute(build, snapshot, build_env, output / "build.log", 180)
        if report["build_exit"]:
            return report["build_exit"]
        report["binary_sha256"] = hashlib.sha256(Path(env["FAULT_STORAGE_BIN"]).read_bytes()).hexdigest()
        admin_build = ["go", "build", "-p=2", "-o", env["FAULT_STORAGE_ADMIN_BIN"], "./cmd/storage-admin"]
        report["admin_build_command"] = admin_build
        report["admin_build_exit"] = execute(admin_build, snapshot, build_env, output / "admin-build.log", 180)
        if report["admin_build_exit"]:
            return report["admin_build_exit"]
        report["admin_binary_sha256"] = hashlib.sha256(Path(env["FAULT_STORAGE_ADMIN_BIN"]).read_bytes()).hexdigest()
        if args.container_image:
            env["FAULT_CONTAINER_TEST_BIN"] = str(work / "fault-container-test")
            probe = ["go", "test", "-c", "-p=2", "-o", env["FAULT_CONTAINER_TEST_BIN"], "./tests/faults"]
            report["container_probe_build_exit"] = execute(probe, snapshot, build_env, output / "container-build.log", 180)
            if report["container_probe_build_exit"]:
                return report["container_probe_build_exit"]
        command = ["go", "test", "-p=2", "-count=1", "-json", "-timeout=120s"]
        if args.race:
            command.append("-race")
        command += ["./tests/faults", "-run", args.run]
        report["test_command"] = command
        report["test_exit"] = execute(command, snapshot, env, output / "tests.jsonl", 210)
        report["used_module_verification"] = verify_used_modules(work, modules)
        report["container_cleanup"] = cleanup_containers(output)
        if any(not r["released"] for r in report["container_cleanup"]):
            report["cleanup_failed"] = True
            return 2
        events = []
        for line in (output / "tests.jsonl").read_text().splitlines():
            try:
                event = json.loads(line)
                if event.get("Test") and event.get("Action") in ("pass", "fail", "skip"):
                    events.append({k: event[k] for k in ("Test", "Action", "Elapsed") if k in event})
            except json.JSONDecodeError:
                pass
        report["tests"] = events
        print(json.dumps({"implementation_sha256": report["storage_implementation_sha256"],
                          "sdk_sha256": report["xrpc_go_source_sha256"], "test_exit": report["test_exit"],
                          "tests": events}, ensure_ascii=False), flush=True)
        return report["test_exit"]
    except Exception as error:
        report["error"] = str(error)
        print(str(error), file=sys.stderr)
        return 2
    finally:
        # A Docker daemon owns its containers, outside the Go process group.
        # Recover only the exact names this private run recorded, even when Go
        # times out or the runner receives an interrupt.
        try:
            report["container_cleanup"] = cleanup_containers(output)
        except Exception as error:
            report["container_cleanup_error"] = str(error)
        try:
            remove_private_source(work)
            report["source_copy_removed"] = True
        except Exception as error:
            report["source_copy_removed"] = False
            report["source_cleanup_error"] = str(error)
        report["finished_unix"] = time.time()
        (output / "manifest.json").write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n")
        if not report.get("source_copy_removed") or report.get("container_cleanup_error") or any(not r["released"] for r in report.get("container_cleanup", [])):
            return 2


if __name__ == "__main__":
    sys.exit(main())
