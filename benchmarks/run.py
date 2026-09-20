#!/usr/bin/env python3
"""Run release benchmarks and compare only equivalent environments and harnesses."""
import argparse
import hashlib
import json
import math
import os
import platform
import statistics
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def command(*args):
    return subprocess.check_output(args, cwd=ROOT, text=True, timeout=30).strip()


def digest(data):
    return hashlib.sha256(data).hexdigest()


def source_digest():
    paths = {Path(name) for name in command("git", "ls-files").splitlines()}
    paths.update(path.relative_to(ROOT) for path in (ROOT / "src").rglob("*.rs"))
    inputs = sorted(path for path in paths if (path.suffix == ".rs" or str(path) in {"Cargo.toml", "Cargo.lock", "rust-toolchain.toml"}) and (ROOT / path).is_file())
    data = b"".join(str(path).encode() + b"\0" + (ROOT / path).read_bytes() + b"\0" for path in inputs)
    return digest(data)


def environment():
    cpuinfo = Path("/proc/cpuinfo")
    cpu = platform.processor()
    if cpuinfo.exists():
        cpu = next((line.split(":", 1)[1].strip() for line in cpuinfo.read_text().splitlines() if line.startswith("model name")), cpu)
    governor = Path("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor")
    return {
        "os": platform.system(),
        "architecture": platform.machine(),
        "cpu": cpu,
        "governor": governor.read_text().strip() if governor.exists() else "unavailable",
        "rustc": command("rustc", "-Vv"),
        "cargo": command("cargo", "--version"),
        "lockfileSha256": digest((ROOT / "Cargo.lock").read_bytes()),
        "profile": "release",
        "features": "default",
        "compilerOverrides": {
            key: value for key, value in sorted(os.environ.items())
            if key in {"RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", "CARGO_BUILD_TARGET", "RUSTC_WRAPPER", "RUSTC_WORKSPACE_WRAPPER"}
            or key.startswith("CARGO_PROFILE_RELEASE_")
        },
    }


def compare(baseline, candidate):
    for key in ("schemaVersion", "environment", "harnessSha256"):
        if baseline[key] != candidate[key]:
            raise ValueError(f"incompatible {key}; benchmark comparison refused")
    previous = {item["name"]: item for item in baseline["scenarios"]}
    current = {item["name"]: item for item in candidate["scenarios"]}
    if len(previous) != len(baseline["scenarios"]) or len(current) != len(candidate["scenarios"]):
        raise ValueError("duplicate scenario names; benchmark comparison refused")
    if previous.keys() != current.keys():
        raise ValueError("scenario sets differ; benchmark comparison refused")
    results = []
    for name, after in current.items():
        before = previous[name]
        for key in ("unit", "direction", "iterationsPerSample", "warmupBatches"):
            if before[key] != after[key]:
                raise ValueError(f"incompatible {name}.{key}; benchmark comparison refused")
        if any(not math.isfinite(item["median"]) or item["median"] <= 0 for item in (before, after)):
            raise ValueError(f"invalid {name} median; benchmark comparison refused")
        delta = after["median"] - before["median"]
        relative = delta / before["median"]
        limit = before["regressionLimit"]
        results.append({
            "name": name,
            "baselineMedian": before["median"],
            "candidateMedian": after["median"],
            "changePercent": relative * 100,
            "regressed": relative > limit["relative"] and delta > limit["absoluteNs"],
        })
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--compare", type=Path)
    parser.add_argument("--convention-source-revision")
    parser.add_argument("--cpu", type=int, help="pin only the timed executable to one allowed CPU (Linux)")
    args = parser.parse_args()
    if args.cpu is not None and (not hasattr(os, "sched_getaffinity") or args.cpu not in os.sched_getaffinity(0)):
        raise ValueError("requested CPU is unavailable")
    env = environment()
    env["benchmarkCpu"] = args.cpu
    harness = digest((ROOT / "src/benchmarks.rs").read_bytes() + b"\0" + Path(__file__).read_bytes())
    baseline = json.loads(args.compare.read_text()) if args.compare else None
    if baseline:
        for key, value in (("schemaVersion", 1), ("environment", env), ("harnessSha256", harness)):
            if baseline[key] != value:
                raise ValueError(f"incompatible {key}; benchmark comparison refused before execution")
    measured_source = source_digest()
    build_root = Path(os.environ.get("CARGO_TARGET_DIR", ROOT / "target")).resolve()
    build_environment = dict(os.environ, CARGO_TARGET_DIR=str(build_root / "benchmarks" / "build" / measured_source))
    build_command = ["cargo", "test", "--release", "--locked", "--offline", "--lib", "--no-run", "--message-format=json"]
    build = subprocess.run(build_command, cwd=ROOT, env=build_environment, text=True, stdout=subprocess.PIPE, stderr=sys.stderr, check=True, timeout=300)
    executables = []
    for line in build.stdout.splitlines():
        artifact = json.loads(line)
        if artifact.get("reason") == "compiler-artifact" and artifact.get("executable") and artifact.get("profile", {}).get("test"):
            if Path(artifact["manifest_path"]).resolve() == ROOT / "Cargo.toml":
                executables.append(Path(artifact["executable"]))
    if len(executables) != 1:
        raise ValueError("expected exactly one repository library test executable")
    executable = executables[0]
    test_arguments = ["benchmarks::benchmark_hot_paths", "--ignored", "--exact", "--nocapture", "--test-threads=1"]
    def pin_cpu():
        os.sched_setaffinity(0, {args.cpu})

    load_before = os.getloadavg() if hasattr(os, "getloadavg") else None
    run = subprocess.run([str(executable), *test_arguments], cwd=ROOT, env=build_environment, text=True, stdout=subprocess.PIPE, stderr=sys.stderr, check=True, timeout=120, preexec_fn=pin_cpu if args.cpu is not None else None)
    load_after = os.getloadavg() if hasattr(os, "getloadavg") else None
    if source_digest() != measured_source:
        raise ValueError("source changed during measurement; receipt refused")
    scenarios = []
    for line in run.stdout.splitlines():
        if "BENCHMARK " not in line:
            continue
        scenario = json.loads(line.split("BENCHMARK ", 1)[1])
        samples = sorted(scenario["samples"])
        scenario["median"] = statistics.median(samples)
        scenario["p95"] = samples[math.ceil(0.95 * len(samples)) - 1]
        scenario["regressionLimit"] = {"relative": 0.20, "absoluteNs": 5.0 if scenario["name"] == "live_command_1024b_no_replay" else 25.0}
        scenarios.append(scenario)
    if len(scenarios) != 6:
        raise ValueError(f"expected six benchmark scenarios, received {len(scenarios)}")
    receipt = {
        "schemaVersion": 1,
        "sourceRevision": command("git", "rev-parse", "HEAD"),
        "sourceSha256": measured_source,
        "conventionSourceRevision": args.convention_source_revision,
        "harnessSha256": harness,
        "environment": env,
        "buildCommand": build_command,
        "testArguments": test_arguments,
        "executableSha256": digest(executable.read_bytes()),
        "scenarios": scenarios,
        "loadBefore": load_before,
        "loadAfter": load_after,
    }
    comparisons = compare(baseline, receipt) if baseline else []
    if baseline:
        receipt["comparison"] = comparisons
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(receipt, indent=2) + "\n")
    for scenario in scenarios:
        print(f'{scenario["name"]}: median={scenario["median"]:.1f} ns/op p95={scenario["p95"]:.1f} ns/op')
    for result in comparisons:
        verdict = "REGRESSION" if result["regressed"] else "within limits"
        print(f'{result["name"]}: {result["changePercent"]:+.1f}% ({verdict})')
    return 1 if any(item["regressed"] for item in comparisons) else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (ValueError, OSError, KeyError, TypeError, subprocess.SubprocessError) as error:
        print(f"benchmark error: {error}", file=sys.stderr)
        sys.exit(2)
