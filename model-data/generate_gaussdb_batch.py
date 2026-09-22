"""Generate 200 EXPLAIN-valid DeepSeek SQL statements for every gsbench size."""

import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path


SIZES = (1, 2, 5, 10, 15, 20)
ROOT = Path(__file__).resolve().parent


def query_count(path: Path) -> int:
    return sum(1 for line in path.read_text(encoding="utf-8").splitlines()
               if line.startswith("-- Query "))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--instance", default=str(ROOT / "runtime" / "instance.json"))
    parser.add_argument("--output-root", default=str(ROOT / "output"))
    parser.add_argument("--target-count", type=int, default=200)
    parser.add_argument("--max-specs", type=int, default=30)
    parser.add_argument("--templates-per-spec", type=int, default=4)
    parser.add_argument("--instances-per-template", type=int, default=5)
    parser.add_argument("--llm-workers", type=int, default=4,
                        help="Concurrent DeepSeek requests per database")
    parser.add_argument("--sizes", nargs="+", type=int, choices=SIZES,
                        default=list(SIZES), help="Only generate the selected benchmark sizes")
    parser.add_argument("--resume", action="store_true", help="skip a size whose generated.sql already has target-count entries")
    args = parser.parse_args()

    instance = json.loads(Path(args.instance).read_text(encoding="utf-8"))
    output_root = Path(args.output_root)
    output_root.mkdir(parents=True, exist_ok=True)
    api_file = ROOT / "api.env"
    if not api_file.exists():
        raise RuntimeError(f"API key file not found: {api_file}")
    first = next((line.strip() for line in api_file.read_text(encoding="utf-8").splitlines()
                  if line.strip() and not line.lstrip().startswith("#")), "")
    if "=" in first:
        key_name, key = first.split("=", 1)
        os.environ[key_name.strip()] = key.strip().strip("'\"")
    else:
        os.environ["OPENAI_API_KEY"] = first
    if not os.environ.get("OPENAI_API_KEY") and not os.environ.get("DEEPSEEK_API_KEY"):
        raise RuntimeError("api.env does not contain a usable API key")

    configs = output_root / ".configs"
    configs.mkdir(exist_ok=True)
    for size in args.sizes:
        database = instance["databases"][str(size)]
        size_output = output_root / f"gsbench-{size}gb"
        sql_file = size_output / "generated.sql"
        if args.resume and sql_file.exists() and query_count(sql_file) == args.target_count:
            print(f"resume: {size}GB already has {args.target_count} SQL statements")
            continue
        size_output.mkdir(parents=True, exist_ok=True)
        config_path = configs / f"gaussdb-{size}gb.json"
        config = {
            "database": {"host": instance.get("socket_dir", "127.0.0.1"), "port": instance["port"],
                         "user": instance["user"], "dbname": database,
                         "schema": instance.get("schema", "gsbench")},
            "model": {"api_key_env": "OPENAI_API_KEY",
                      "base_url": "https://api.deepseek.com", "name": "deepseek-chat"},
            "api_env_file": str(api_file),
            "spec_file": str(ROOT / "specs_memory.json"),
            "prompt_dir": str(ROOT / "prompts-memory"),
        }
        config_path.write_text(json.dumps(config, indent=2), encoding="utf-8")
        command = [sys.executable, str(ROOT / "sqlGEM.py"),
                   "--config", str(config_path), "--schema", instance.get("schema", "gsbench"),
                   "--target-count", str(args.target_count), "--max-specs", str(args.max_specs),
                   "--templates-per-spec", str(args.templates_per_spec),
                   "--instances-per-template", str(args.instances_per_template),
                   "--llm-workers", str(args.llm_workers),
                   "--strict-target", "--output-dir", str(size_output)]
        print(f"generating {args.target_count} SQL statements for {size}GB ({database})")
        result = subprocess.run(command, cwd=ROOT, env=os.environ.copy())
        if result.returncode != 0:
            raise RuntimeError(f"SQL generation failed for {size}GB with exit code {result.returncode}")
        count = query_count(sql_file)
        if count != args.target_count:
            raise RuntimeError(f"{size}GB produced {count} SQL statements, expected {args.target_count}")
        manifest = {"size_gb": size, "database": database, "schema": instance.get("schema", "gsbench"),
                    "port": instance["port"], "server_version": instance.get("version"),
                    "target_count": args.target_count, "valid_sql_count": count,
                    "model": "deepseek-chat", "base_url": "https://api.deepseek.com"}
        (size_output / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print("all requested sizes completed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
