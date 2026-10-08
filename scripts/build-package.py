#!/usr/bin/env python3
"""Build an isolated static storage Debian package, with exact source receipts."""
import argparse
import hashlib
import json
import os
import re
from pathlib import Path
import shutil
import subprocess
import tempfile


def source_files(root):
    found = {}
    for path in sorted(root.rglob("*")):
        relative = path.relative_to(root)
        if any(part in (".git", "build", "__pycache__", "evidence", ".ci") for part in relative.parts):
            continue
        if path.is_file():
            if path.is_symlink():
                raise RuntimeError(f"source symlink refused: {relative}")
            if path.suffix in (".go", ".mod", ".sum", ".proto", ".sql", ".json", ".md", ".yml", ".py"):
                found[str(relative)] = hashlib.sha256(path.read_bytes()).hexdigest()
    return found


def digest(files):
    return hashlib.sha256(json.dumps(files, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True, help="absent output artifact .deb")
    parser.add_argument("--architecture", choices=("amd64", "arm64"), required=True)
    parser.add_argument("--distribution", choices=("focal", "jammy", "noble"), required=True)
    parser.add_argument("--preview", action="store_true", help="allow a dirty source snapshot; never a release build")
    args = parser.parse_args()
    root = Path(__file__).resolve().parent.parent
    output = args.output.absolute()
    if root == output or root in output.parents:
        parser.error("artifact output must be outside the source tree")
    if output.exists() or output.with_suffix(".build.json").exists():
        parser.error("artifact/receipt output already exists")
    root_before = source_files(root)
    source_sha = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
    source_dirty = bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=root, text=True))
    if source_dirty and not args.preview:
        parser.error("release package requires committed source; --preview permits an isolated dirty snapshot")
    sdk_module = json.loads(subprocess.check_output(
        ["go", "list", "-mod=readonly", "-m", "-json", "github.com/XGC-Team/xgc2-xrpc/go"],
        cwd=root, text=True))
    if sdk_module.get("Replace") or not sdk_module.get("Version") or not sdk_module.get("Sum"):
        parser.error("shared XRPC must be an immutable remote module with go.sum, without replace")
    with tempfile.TemporaryDirectory(prefix=".storage-package-", dir=output.parent) as temporary:
        work = Path(temporary)
        storage_copy = work / "storage"
        for relative in root_before:
            target = storage_copy / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(root / relative, target)
        if source_files(storage_copy) != root_before or source_files(root) != root_before:
            raise RuntimeError("source changed during snapshot; rerun stable batch")
        package = work / "package"
        binary_dir = package / "usr/bin"
        binary_dir.mkdir(parents=True)
        env = dict(os.environ, CGO_ENABLED="0", GOOS="linux", GOARCH=args.architecture, GOWORK="off")
        for source, name in (("xgc2-storage", "xgc2-storage"), ("storage-admin", "xgc2-storage-admin")):
            subprocess.run(["go", "build", "-mod=readonly", "-trimpath", "-o", str(binary_dir / name), "./cmd/" + source], cwd=storage_copy, env=env, check=True)
        for name in ("contracts", "schemas"):
            shutil.copytree(storage_copy / name, package / "usr/share/xgc2-storage" / name)
        metadata = (storage_copy / ".xgc2/product.yml").read_text()
        version = re.search(r"^    " + args.distribution + r": (\S+)$", metadata, re.MULTILINE).group(1)
        control = package / "DEBIAN"
        control.mkdir()
        (control / "control").write_text(
            f"Package: xgc2-storage\nVersion: {version}\nArchitecture: {args.architecture}\n"
            "Maintainer: XGC Team\nDepends: ca-certificates\nSection: utils\nPriority: optional\n"
            "Description: Independent bounded XRPC SQLite owner\n"
        )
        receipt = {
            "format": "xgc2-storage-build-v1", "version": version,
            "architecture": args.architecture, "distribution": args.distribution,
            "source_sha": source_sha, "source_sha256": digest(root_before),
            "xrpc_module": {key: sdk_module[key] for key in ("Path", "Version", "Sum", "GoModSum")},
            "source_dirty": source_dirty, "preview": args.preview,
            "go_version": subprocess.check_output(["go", "version"], text=True).strip(),
            "sqlite_module": "modernc.org/sqlite v1.46.2", "sqlite_engine": "3.51.3",
            "binaries": {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(binary_dir.iterdir())},
            "storage_source_files": root_before,
        }
        (package / "usr/share/xgc2-storage/build.json").write_text(json.dumps(receipt, indent=2) + "\n")
        if source_files(root) != root_before:
            raise RuntimeError("source drift before packaging; rerun stable batch")
        artifact = work / "storage.deb"
        subprocess.run(["dpkg-deb", "--root-owner-group", "--build", str(package), str(artifact)], check=True)
        receipt["deb_sha256"] = hashlib.sha256(artifact.read_bytes()).hexdigest()
        # Publish exclusively: another build cannot be silently overwritten.
        with output.with_suffix(".build.json").open("x") as receipt_file:
            receipt_file.write(json.dumps(receipt, indent=2) + "\n")
            receipt_file.flush()
            os.fsync(receipt_file.fileno())
        os.link(artifact, output)
        print(json.dumps({"artifact": str(output), "sha256": receipt["deb_sha256"], "source_sha256": receipt["source_sha256"], "xrpc_module": receipt["xrpc_module"]}))


if __name__ == "__main__":
    main()
