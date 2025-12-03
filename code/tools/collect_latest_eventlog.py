#!/usr/bin/env python3
"""
根据 spark-events 目录下最新的 event log，调用 parse_spark_eventlog.py 生成结果 JSON。

用法：
    python code/tools/collect_latest_eventlog.py <strategy> [--event-dir LOG_DIR] [--results-dir RESULTS_DIR] [--tag TAG]

参数说明：
    strategy    : 策略标识，用于结果文件前缀，例如 "range" / "custom" / "hash"
    --event-dir : Spark event log 目录，默认 "logs/spark-events"
    --results-dir : 结果输出目录，默认 "code/results"
    --tag       : 可选的额外标签，会拼到文件名中间（不写的话只用参数名推导）

结果文件命名格式大致为：
    <results-dir>/{strategy}_{input_type}_{partitions}_{records}_{timestamp}.json
    或如果提供了 tag：
    <results-dir>/{strategy}_{tag}_{timestamp}.json

注意：
  - 本脚本只负责“找到最新日志并调用 parse_spark_eventlog.py”，不会修改 Spark 配置。
  - 依赖 parse_spark_eventlog.py 位于 code/tools/parse_spark_eventlog.py。
"""

import argparse
import os
import subprocess
import sys
from datetime import datetime

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_CODE_DIR = os.path.abspath(os.path.join(_SCRIPT_DIR, ".."))
if _CODE_DIR not in sys.path:
    sys.path.insert(0, _CODE_DIR)

from jobs.common import SKEW_RATIO


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect latest Spark event log and export metrics JSON.")
    parser.add_argument(
        "strategy",
        type=str,
        help='strategy name, e.g. "range", "custom", "hash"',
    )
    parser.add_argument(
        "--event-dir",
        type=str,
        default="logs/spark-events",
        help="directory where Spark event logs are stored (default: logs/spark-events)",
    )
    parser.add_argument(
        "--tag",
        type=str,
        default="",
        help="optional custom tag to embed in filename, e.g. uniform_p128_10m",
    )
    return parser.parse_args()


def derive_skew_tag() -> str:
    try:
        ratio = float(SKEW_RATIO)
    except Exception as exc:
        raise RuntimeError("Impossible to derive skew tag from SKEW_RATIO") from exc
    if ratio <= 0:
        raise RuntimeError("SKEW_RATIO must be positive to determine results directory")
    return str(int(round(ratio * 100)))


def find_latest_event_log(event_dir: str) -> str:
    if not os.path.isdir(event_dir):
        raise FileNotFoundError(f"Event log directory not found: {event_dir}")

    # 找到修改时间最新的文件
    candidates = [
        os.path.join(event_dir, f)
        for f in os.listdir(event_dir)
        if os.path.isfile(os.path.join(event_dir, f))
    ]
    if not candidates:
        raise FileNotFoundError(f"No event log files found in {event_dir}")

    latest = max(candidates, key=os.path.getmtime)
    return latest


def build_output_path(strategy: str, results_dir: str, tag: str) -> str:
    os.makedirs(results_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_tag = tag.strip().replace(" ", "_")
    if safe_tag:
        filename = f"{strategy}_{safe_tag}_{timestamp}.json"
    else:
        filename = f"{strategy}_{timestamp}.json"
    return os.path.join(results_dir, filename)


def main() -> None:
    args = parse_args()

    event_dir = args.event_dir

    skew_tag = derive_skew_tag()
    results_dir = os.path.join("code", f"results_{skew_tag}")
    strategy = args.strategy
    tag = args.tag

    try:
        latest_log = find_latest_event_log(event_dir)
    except FileNotFoundError as e:
        print(f"[collect_latest_eventlog] ERROR: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"[collect_latest_eventlog] Using latest event log: {latest_log}")

    out_path = build_output_path(strategy, results_dir, tag)
    print(f"[collect_latest_eventlog] Exporting summary to: {out_path}")

    # 调用 parse_spark_eventlog.py
    parser_script = os.path.join("code", "tools", "parse_spark_eventlog.py")
    if not os.path.isfile(parser_script):
        print(f"[collect_latest_eventlog] ERROR: Parser script not found: {parser_script}", file=sys.stderr)
        sys.exit(1)

    cmd = [
        sys.executable,
        parser_script,
        latest_log,
        out_path,
    ]
    print(f"[collect_latest_eventlog] Running: {' '.join(cmd)}")
    try:
        subprocess.check_call(cmd)
    except subprocess.CalledProcessError as e:
        print(f"[collect_latest_eventlog] ERROR: parse_spark_eventlog.py failed with code {e.returncode}", file=sys.stderr)
        sys.exit(e.returncode)

    print(f"[collect_latest_eventlog] Done. Summary written to {out_path}")


if __name__ == "__main__":
    main()