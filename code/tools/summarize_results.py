#!/usr/bin/env python3
"""
遍历 code/results/*.json，汇总 Range / Hash / Custom 实验的关键指标到一个 CSV。

- 从文件名中解析：
  - strategy: range / hash / custom
  - data_type: uniform / skewed / synthetic / unknown
  - partitions: pXX -> XX
  - records_tag: 5m / 2m / 200k 等（直接保存字符串）
  - hot_keys / bucket_factor: 对 custom 有意义（hash/range 留空）

- 从 JSON 内容中解析：
  - job_count, stage_count, task_count
  - job_total_ms, job_max_ms
  - stage_total_ms, stage_max_ms
  - total_shuffle_read_bytes, total_shuffle_write_bytes
  - task_duration_stats_ms.* (min, p50, p90, p99, max)

输出：
  code/results/summary_metrics.csv
"""

import csv
import json
import os
import re
import sys
from glob import glob
from typing import Dict, Any, List, Optional

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_CODE_DIR = os.path.abspath(os.path.join(_SCRIPT_DIR, ".."))
if _CODE_DIR not in sys.path:
    sys.path.insert(0, _CODE_DIR)

from jobs.common import SKEW_RATIO


def derive_skew_tag() -> str:
    try:
        ratio = float(SKEW_RATIO)
    except Exception as exc:
        raise RuntimeError("Cannot derive skew tag from SKEW_RATIO") from exc
    if ratio <= 0:
        raise RuntimeError("SKEW_RATIO must be positive to derive results directory")
    return str(int(round(ratio * 100)))


def parse_filename(path: str) -> Dict[str, Optional[str]]:
    """
    从结果文件名中解析出一些关键信息。

    预期文件名模式示例：
      - range_skewed_p64_5m_20251201_154625.json
      - hash_uniform_p16_2m_20251201_154835.json
      - custom_skewed_hot0_b4_p64_5m_20251201_155700.json
      - custom_uniform_hotnone_b4_p16_2m_20251201_155639.json
    """
    name = os.path.basename(path)
    if not name.endswith(".json"):
        return {}

    base = name[:-5]  # 去掉 .json
    parts = base.split("_")

    info: Dict[str, Optional[str]] = {
        "strategy": None,
        "data_type": None,
        "partitions": None,
        "records_tag": None,
        "hot_keys": None,
        "bucket_factor": None,
    }

    if not parts:
        return info

    # strategy: range / hash / custom
    info["strategy"] = parts[0]

    if info["strategy"] in ("range", "hash"):
        # strategy_dataType_pXX_recordsTag_时间戳
        if len(parts) >= 4:
            info["data_type"] = parts[1]
            if re.match(r"^p\d+$", parts[2]):
                info["partitions"] = parts[2][1:]
            info["records_tag"] = parts[3]
    elif info["strategy"] == "custom":
        # custom_dataType_hotXXX_bX_pXX_recordsTag_时间戳
        if len(parts) >= 6:
            info["data_type"] = parts[1]
            info["hot_keys"] = parts[2]      # hot0 / hotnone / ...
            info["bucket_factor"] = parts[3] # b4 / ...
            if re.match(r"^p\d+$", parts[4]):
                info["partitions"] = parts[4][1:]
            info["records_tag"] = parts[5]
    else:
        # 其它策略的话，可以在需要时扩展
        pass

    return info


def extract_metrics_from_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    job_count = data.get("job_count")
    stage_count = data.get("stage_count")
    task_count = data.get("task_count")

    # Job durations
    job_durations = data.get("job_durations_ms") or []
    job_total_ms = sum(j.get("duration_ms", 0) for j in job_durations)
    job_max_ms = max((j.get("duration_ms", 0) for j in job_durations), default=0)

    # Stage durations
    stage_durations = data.get("stage_durations_ms") or []
    stage_total_ms = sum(s.get("duration_ms", 0) for s in stage_durations)
    stage_max_ms = max((s.get("duration_ms", 0) for s in stage_durations), default=0)

    total_shuffle_read_bytes = data.get("total_shuffle_read_bytes")
    total_shuffle_write_bytes = data.get("total_shuffle_write_bytes")

    task_stats = data.get("task_duration_stats_ms") or {}
    task_min_ms = task_stats.get("min_ms")
    task_p50_ms = task_stats.get("p50_ms")
    task_p90_ms = task_stats.get("p90_ms")
    task_p99_ms = task_stats.get("p99_ms")
    task_max_ms = task_stats.get("max_ms")

    return {
        "job_count": job_count,
        "stage_count": stage_count,
        "task_count": task_count,
        "job_total_ms": job_total_ms,
        "job_max_ms": job_max_ms,
        "stage_total_ms": stage_total_ms,
        "stage_max_ms": stage_max_ms,
        "total_shuffle_read_bytes": total_shuffle_read_bytes,
        "total_shuffle_write_bytes": total_shuffle_write_bytes,
        "task_min_ms": task_min_ms,
        "task_p50_ms": task_p50_ms,
        "task_p90_ms": task_p90_ms,
        "task_p99_ms": task_p99_ms,
        "task_max_ms": task_max_ms,
    }


def main() -> None:
    skew_tag = derive_skew_tag()
    results_dir = os.path.join("code", f"results_{skew_tag}")

    if not os.path.isdir(results_dir):
        print(f"[summarize_results] Results directory not found: {results_dir}")
        return

    paths = sorted(glob(os.path.join(results_dir, "*.json")))
    if not paths:
        print(f"[summarize_results] No JSON result files found in {results_dir}")
        return

    rows: List[Dict[str, Any]] = []

    for path in paths:
        filename_info = parse_filename(path)
        if not filename_info.get("strategy"):
            print(f"[summarize_results] Skip unrecognized file: {path}")
            continue

        metrics = extract_metrics_from_json(path)

        row: Dict[str, Any] = {
            "file": os.path.basename(path),
        }
        row.update(filename_info)
        row.update(metrics)

        rows.append(row)

    if not rows:
        print("[summarize_results] No valid result rows to write.")
        return

    os.makedirs(results_dir, exist_ok=True)

    fieldnames = [
        "file",
        "strategy",
        "data_type",
        "partitions",
        "records_tag",
        "hot_keys",
        "bucket_factor",
        "job_count",
        "stage_count",
        "task_count",
        "job_total_ms",
        "job_max_ms",
        "stage_total_ms",
        "stage_max_ms",
        "total_shuffle_read_bytes",
        "total_shuffle_write_bytes",
        "task_min_ms",
        "task_p50_ms",
        "task_p90_ms",
        "task_p99_ms",
        "task_max_ms",
    ]

    output_csv = os.path.join(results_dir, "summary_metrics.csv")
    with open(output_csv, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    print(f"[summarize_results] Wrote {len(rows)} rows to {output_csv}")


if __name__ == "__main__":
    main()