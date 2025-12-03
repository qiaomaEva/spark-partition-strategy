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
import sys
from typing import List, Dict, Optional

import matplotlib.pyplot as plt
import pandas as pd

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
        raise RuntimeError("SKEW_RATIO must be positive to derive figure paths")
    return str(int(round(ratio * 100)))


def _resolve_paths() -> Dict[str, str]:
    skew_tag = derive_skew_tag()
    results_dir = os.path.join("code", f"results_{skew_tag}")
    figures_dir = os.path.join("docs", f"figures_{skew_tag}")
    summary_csv = os.path.join(results_dir, "summary_metrics.csv")
    return {"results_dir": results_dir, "figures_dir": figures_dir, "summary_csv": summary_csv}


def _ensure_summary_exists(summary_csv: str):
    if not os.path.isfile(summary_csv):
        raise FileNotFoundError(
            f"{summary_csv} not found. Please run summarize_results.py first."
        )


def _select_latest_by_strategy(
    df: pd.DataFrame,
    data_type: str,
    records_tag: str,
    partitions: str,
    bucket_factor: Optional[str] = None,
) -> pd.DataFrame:
    # 先只按场景过滤（data_type / records_tag / partitions）
    mask = (
        (df["data_type"] == data_type)
        & (df["records_tag"] == records_tag)
        & (df["partitions"].astype(str) == str(partitions))
    )
    subset = df[mask]

    # 如果指定了 bucket_factor，只收紧 custom 的那一部分，
    # hash / range 不需要有 bucket_factor 这一列
    if bucket_factor is not None:
        bf_str = str(bucket_factor)
        if not bf_str.startswith("b"):
            bf_str = f"b{bf_str}"

        is_custom = subset["strategy"] == "custom"
        subset = subset[~is_custom | (subset["bucket_factor"].astype(str) == bf_str)]
    if subset.empty:
        desc = f"{data_type} / {records_tag} / p{partitions}"
        if bucket_factor is not None:
            desc += f" / b{bucket_factor}"
        print(f"[plot_summary] No rows for {desc}")
        return pd.DataFrame()

    subset = subset.sort_values("file")
    # groupby.tail may return a view; make an explicit copy to avoid
    # SettingWithCopyWarning when we assign into the DataFrame below.
    latest_by_strategy = subset.groupby("strategy").tail(1).copy()

    strategy_order = ["hash", "range", "custom"]
    # use .loc to assign to the column explicitly on the DataFrame copy
    latest_by_strategy.loc[:, "strategy"] = pd.Categorical(
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
    figures_dir: str,
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

    os.makedirs(figures_dir, exist_ok=True)
    out_path = os.path.join(figures_dir, out_name)
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()
    print(f"[plot_summary] Saved plot to {out_path}")


def main():
    paths = _resolve_paths()
    figures_dir = paths["figures_dir"]
    summary_csv = paths["summary_csv"]

    _ensure_summary_exists(summary_csv)
    df = pd.read_csv(summary_csv)

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
                figures_dir=figures_dir,
            )

    # 第二类：同一 skewed 场景下，对比不同 bucket_factor 的 Custom（b4/b8/b32）
    # x 轴为 bucket_factor，y 为各项指标
    # 这里 bucket_factor 在 CSV 中统一存储为 "b4" / "b8" / "b32" 这样的格式
    skew_custom_buckets = ["b4", "b8", "b32"]
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

        # bucket_factor 本身已经是 "b4" / "b8" / "b32"，直接作为 x 轴标签
        xs = [str(b) for b in latest_by_b["bucket_factor"].astype(str).tolist()]
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

        os.makedirs(figures_dir, exist_ok=True)
        out_path = os.path.join(figures_dir, f"{col}_{skew_name}.png")
        plt.tight_layout()
        plt.savefig(out_path, dpi=150)
        plt.close()
        print(f"[plot_summary] Saved plot to {out_path}")


if __name__ == "__main__":
    main()