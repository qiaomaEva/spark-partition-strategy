#!/usr/bin/env python3
"""
基于 code/results/summary_metrics.csv 画出几个对比图：

- skewed + 5m + p64:
    - task_p90_ms: hash / range / custom
    - task_p99_ms: hash / range / custom
    - job_total_ms: hash / range / custom

- uniform + 2m + p16:
    - task_p99_ms: hash / range / custom
    - job_total_ms: hash / range / custom

需要先运行:
    python3 code/tools/summarize_results.py
"""

import os

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
    df: pd.DataFrame, data_type: str, records_tag: str, partitions: str
) -> pd.DataFrame:
    mask = (
        (df["data_type"] == data_type)
        & (df["records_tag"] == records_tag)
        & (df["partitions"].astype(str) == str(partitions))
    )
    subset = df[mask]
    if subset.empty:
        print(f"[plot_summary] No rows for {data_type} / {records_tag} / p{partitions}")
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

    # 稍微放宽画布宽度，并减小标题字号，避免截断
    plt.figure(figsize=(5.5, 4))  # 原来是 (5, 4)
    plt.bar(strategies, values, color=["#4e79a7", "#f28e2b", "#59a14f"])
    plt.ylabel(ylabel)

    # 允许自动换行 + 调整字号
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

    # =========================
    # 1) Skewed, 5m, p64
    # =========================
    skewed_p64 = _select_latest_by_strategy(
        df, data_type="skewed", records_tag="5m", partitions="64"
    )

    # 1.a Task p90
    _bar_plot(
        skewed_p64,
        value_col="task_p90_ms",
        title="Task p90, skewed-5m-p64: hash vs range vs custom",
        out_name="task_p90_skewed_5m_p64.png",
        ylabel="Task p90 duration (ms)",
    )

    # 1.b Task p99
    _bar_plot(
        skewed_p64,
        value_col="task_p99_ms",
        title="Task p99, skewed-5m-p64: hash vs range vs custom",
        out_name="task_p99_skewed_5m_p64.png",
        ylabel="Task p99 duration (ms)",
    )

    # 1.c Job total duration
    _bar_plot(
        skewed_p64,
        value_col="job_total_ms",
        title="Job total time, skewed-5m-p64: hash vs range vs custom",
        out_name="job_total_skewed_5m_p64.png",
        ylabel="Sum of job durations (ms)",
    )

    # =========================
    # 2) Uniform, 2m, p16
    # =========================
    uniform_p16 = _select_latest_by_strategy(
        df, data_type="uniform", records_tag="2m", partitions="16"
    )

    # 2.a Task p99 (看差异是否足够小)
    _bar_plot(
        uniform_p16,
        value_col="task_p99_ms",
        title="Task p99, uniform-2m-p16: hash vs range vs custom",
        out_name="task_p99_uniform_2m_p16.png",
        ylabel="Task p99 duration (ms)",
    )

    # 2.b Job total duration
    _bar_plot(
        uniform_p16,
        value_col="job_total_ms",
        title="Job total time, uniform-2m-p16: hash vs range vs custom",
        out_name="job_total_uniform_2m_p16.png",
        ylabel="Sum of job durations (ms)",
    )


if __name__ == "__main__":
    main()