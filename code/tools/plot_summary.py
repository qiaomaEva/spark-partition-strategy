#!/usr/bin/env python3
"""
基于 code/results/summary_metrics.csv 画出多个对比图。

支持多场景配置，例如：
  - skewed + 5m + p64
  - uniform + 2m + p16
  - 以后可以加: skewed + 10m + p32 等

每个场景可以画多个指标：
  - task_p90_ms, task_p99_ms
  - job_total_ms 等

使用前需要先运行:
    python3 code/tools/summarize_results.py
"""

import os
from typing import List, Dict

import matplotlib.pyplot as plt
import pandas as pd

RESULTS_DIR = os.path.join("code", "results")
SUMMARY_CSV = os.path.join(RESULTS_DIR, "summary_metrics.csv")


def _ensure_summary_exists():
    if not os.path.isfile(SUMMARY_CSV):
        raise FileNotFoundError(
            f"{SUMMARY_CSV} not found. Please run summarize_results.py first."
        )


def _select_latest_by_strategy(
    df: pd.DataFrame,
    data_type: str,
    records_tag: str,
    partitions: str,
    bucket_factor: str | None = None,
) -> pd.DataFrame:
    mask = (
        (df["data_type"] == data_type)
        & (df["records_tag"] == records_tag)
        & (df["partitions"].astype(str) == str(partitions))
    )
    if bucket_factor is not None:
        # 只保留给定 bucket_factor（例如 b4/b8/b32）的一组 Custom 结果
        mask = mask & (df["bucket_factor"].astype(str) == str(bucket_factor))

    subset = df[mask]
    if subset.empty:
        desc = f"{data_type} / {records_tag} / p{partitions}"
        if bucket_factor is not None:
            desc += f" / b{bucket_factor}"
        print(f"[plot_summary] No rows for {desc}")
        return pd.DataFrame()

    subset = subset.sort_values("file")
    latest_by_strategy = subset.groupby("strategy").tail(1)

    strategy_order = ["hash", "range", "custom"]
    latest_by_strategy["strategy"] = pd.Categorical(
        latest_by_strategy["strategy"], categories=strategy_order, ordered=True
    )
    latest_by_strategy = latest_by_strategy.sort_values("strategy")
    return latest_by_strategy


def _bar_plot(
    df: pd.DataFrame,
    value_col: str,
    title: str,
    out_name: str,
    ylabel: str,
):
    if df.empty:
        return

    strategies = df["strategy"].tolist()
    values = df[value_col].tolist()

    plt.figure(figsize=(5.5, 4))
    plt.bar(strategies, values, color=["#4e79a7", "#f28e2b", "#59a14f"])
    plt.ylabel(ylabel)
    plt.title(title, fontsize=11, wrap=True)
    plt.grid(axis="y", linestyle="--", alpha=0.4)

    os.makedirs(RESULTS_DIR, exist_ok=True)
    out_path = os.path.join(RESULTS_DIR, out_name)
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()
    print(f"[plot_summary] Saved plot to {out_path}")


def main():
    _ensure_summary_exists()
    df = pd.read_csv(SUMMARY_CSV)

    # ===== 在这里配置你要画的“场景” =====
    # 以后增加新场景，只需要在这个列表里加一行
    scenarios: List[Dict[str, str]] = [
        # 均匀场景：uniform, p16, 2m
        {"name": "uniform_2m_p16", "data_type": "uniform", "records_tag": "2m", "partitions": "16", "bucket_factor": None},

        # 温和倾斜场景：skewed, p64, 5m，比较 Hash / Range / Custom-b8
        {"name": "skewed_5m_p64_b8", "data_type": "skewed", "records_tag": "5m", "partitions": "64", "bucket_factor": "8"},

        # 极端倾斜场景：skewed, p64, 5m，比较 Hash / Range / Custom-b32
        {"name": "skewed_5m_p64_b32", "data_type": "skewed", "records_tag": "5m", "partitions": "64", "bucket_factor": "32"},

        # Custom(b4/b8/b32) 横向对比单独在下面处理
    ]

    # ===== 在这里配置要画的指标 =====
    # key: CSV 里的列名
    # label: y 轴和标题里使用的描述
    metrics_to_plot = [
        {
            "col": "task_p90_ms",
            "label": "Task p90 duration (ms)",
        },
        {
            "col": "task_p99_ms",
            "label": "Task p99 duration (ms)",
        },
        {
            "col": "job_total_ms",
            "label": "Sum of job durations (ms)",
        },
    ]

    # 第一类：Hash vs Range vs Custom（固定 b，例如 skewed + p64 + 5m + b8）
    for scenario in scenarios:
        name = scenario["name"]
        data_type = scenario["data_type"]
        records_tag = scenario["records_tag"]
        partitions = scenario["partitions"]
        bucket_factor = scenario.get("bucket_factor")

        subset = _select_latest_by_strategy(df, data_type, records_tag, partitions, bucket_factor)
        if subset.empty:
            continue

        for m in metrics_to_plot:
            col = m["col"]
            label = m["label"]

            if data_type == "uniform" and col in ("task_p90_ms",):
                continue

            bf_desc = f", b{bucket_factor}" if bucket_factor is not None else ""
            title = f"{col} ({data_type}, {records_tag}, p{partitions}{bf_desc}): hash vs range vs custom"
            title = title.replace("task_p90_ms", "Task p90").replace(
                "task_p99_ms", "Task p99"
            ).replace("job_total_ms", "Job total time")

            out_name = f"{col}_{name}.png"

            _bar_plot(
                subset,
                value_col=col,
                title=title,
                out_name=out_name,
                ylabel=label,
            )

    # 第二类：同一 skewed 场景下，对比不同 bucket_factor 的 Custom（b4/b8/b32）
    # x 轴为 bucket_factor，y 为各项指标
    skew_custom_buckets = ["4", "8", "32"]
    skew_name = "skewed_5m_p64_custom_buckets"
    data_type = "skewed"
    records_tag = "5m"
    partitions = "64"

    mask_custom = (
        (df["data_type"] == data_type)
        & (df["records_tag"] == records_tag)
        & (df["partitions"].astype(str) == str(partitions))
        & (df["strategy"] == "custom")
        & (df["bucket_factor"].astype(str).isin(skew_custom_buckets))
    )
    subset_custom = df[mask_custom]
    if subset_custom.empty:
        print("[plot_summary] No custom rows for skewed / 5m / p64 / b in {4,8,32}")
        return

    # 对每个 bucket_factor 只保留最新一条记录
    subset_custom = subset_custom.sort_values("file")
    latest_by_b = subset_custom.groupby("bucket_factor").tail(1)
    latest_by_b = latest_by_b.sort_values("bucket_factor")

    for m in metrics_to_plot:
        col = m["col"]
        label = m["label"]

        xs = [f"b{b}" for b in latest_by_b["bucket_factor"].astype(str).tolist()]
        ys = latest_by_b[col].tolist()

        plt.figure(figsize=(5.5, 4))
        plt.bar(xs, ys, color="#59a14f")
        plt.ylabel(label)
        title = f"{col} (skewed, 5m, p64): custom with different buckets"
        title = title.replace("task_p90_ms", "Task p90").replace(
            "task_p99_ms", "Task p99"
        ).replace("job_total_ms", "Job total time")
        plt.title(title, fontsize=11, wrap=True)
        plt.grid(axis="y", linestyle="--", alpha=0.4)

        os.makedirs(RESULTS_DIR, exist_ok=True)
        out_path = os.path.join(RESULTS_DIR, f"{col}_{skew_name}.png")
        plt.tight_layout()
        plt.savefig(out_path, dpi=150)
        plt.close()
        print(f"[plot_summary] Saved plot to {out_path}")


if __name__ == "__main__":
    main()