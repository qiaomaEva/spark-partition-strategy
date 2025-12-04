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

def _resolve_paths() -> Dict[str, str]:
    # 结果目录统一为 code/results，图片目录统一为 docs/figures。
    # 倾斜度和数据规模通过 CSV 中的 records_tag 字段（例如 skewed_5m_ratio0.95）区分。
    results_dir = os.path.join("code", "results")
    figures_dir = os.path.join("docs", "figures")
    summary_csv = os.path.join(results_dir, "summary_metrics.csv")
    return {"results_dir": results_dir, "figures_dir": figures_dir, "summary_csv": summary_csv}


def _ensure_summary_exists(summary_csv: str):
    if not os.path.isfile(summary_csv):
        raise FileNotFoundError(
            f"{summary_csv} not found. Please run summarize_results.py first."
        )


def _select_latest_by_strategy(
    df: pd.DataFrame,
    scene_tag: str,
    partitions: str,
) -> pd.DataFrame:
    """在给定 scene_tag / partitions 下，为每个 strategy 选出最新的一条记录。

    scene_tag 形如 "skewed_5m_ratio0.95"，已经编码了数据类型和倾斜度信息。
    这里不再对 bucket_factor 做过滤，仅按 strategy 维度各取最新一条记录。
    """

    mask = (
        (df["scene_tag"] == scene_tag)
        & (df["partitions"].astype(str) == str(partitions))
    )
    subset = df[mask]

    if subset.empty:
        desc = f"{scene_tag} / p{partitions}"
        print(f"[plot_summary] No rows for {desc}")
        return pd.DataFrame()

    subset = subset.sort_values("file")
    latest_by_strategy = subset.groupby("strategy").tail(1).copy()

    strategy_order = ["hash", "range", "custom"]
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

    # ===== 自动从 CSV 推导“场景” =====
    # 不再手动写死场景列表，而是按 (records_tag, partitions) 组合自动生成：
    #   - 对于每个 (records_tag, partitions)，画一组 hash/range/custom 对比图；
    #   - 对于 custom，仍然可以在下面单独做 bucket_factor 的横向对比。

    # 只保留我们关心的三种策略
    df = df[df["strategy"].isin(["hash", "range", "custom"])].copy()

    # ===== 兼容当前 CSV 列格式，构造统一的场景标签和分区数 =====
    # 当前 summary_metrics.csv 中：
    #   - data_type: skewed / uniform
    #   - records_tag: 5m / 2m / ...
    #   - ratio: ratio0.95 / ratioNone / ... （如果存在该列）
    #
    # 本脚本内部统一使用 scene_tag 形如：skewed_5m_ratio0.95
    # 如果没有 ratio 列，则退化为 data_type_records_tag，例如 uniform_2m。

    # 某些老版本 CSV 可能没有 ratio 列，这里做一个兼容处理
    has_ratio = "ratio" in df.columns

    def _build_scene_tag(row: pd.Series) -> str:
        data_type = str(row.get("data_type", "")).strip() or "unknown"
        records = str(row.get("records_tag", "")).strip() or "unknown"
        if has_ratio:
            ratio = str(row.get("ratio", "")).strip()
            if ratio:
                return f"{data_type}_{records}_{ratio}"
        return f"{data_type}_{records}"

    df["scene_tag"] = df.apply(_build_scene_tag, axis=1)

    # 确保 partitions 列有值：如果为空，则从 file 名里的 `_pXX_` 中提取，
    # 同时统一成整数形式的字符串，避免 32 和 32.0 被当成两个不同场景。
    def _extract_partitions(row: pd.Series) -> Optional[str]:
        val = row.get("partitions")
        if pd.notna(val) and str(val).strip() != "":
            s = str(val).strip()
            try:
                # 兼容 32 / 32.0 之类的写法
                return str(int(float(s)))
            except (ValueError, TypeError):
                return s
        fname = str(row.get("file", ""))
        # 例如 hash_skewed_5m_ratio0.95_p32_2025....json
        import re

        m = re.search(r"_p(\d+)_", fname)
        if m:
            return m.group(1)
        return None

    df["partitions"] = df.apply(_extract_partitions, axis=1)

    # 所有出现过的 (scene_tag, partitions) 组合
    scenarios_keys = (
        df[["scene_tag", "partitions"]]
        .dropna()
        .drop_duplicates()
        .to_dict(orient="records")
    )

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

    # 第一类：Hash vs Range vs Custom（对每个 (scene_tag, partitions) 自动生成）
    for key in scenarios_keys:
        scene_tag = str(key["scene_tag"])
        partitions = str(key["partitions"])

        # 默认文件名只包含 scene_tag + pXX，后面根据选中的 custom bucket 再附加后缀，
        # 例如 _b32，表示这张图里 custom 用的是 b32。
        name = f"{scene_tag}_p{partitions}"

        subset = _select_latest_by_strategy(df, scene_tag, partitions)
        if subset.empty:
            continue

        for m in metrics_to_plot:
            col = m["col"]
            label = m["label"]

            # 如果有 custom 记录，则找出本次选择的 custom 行对应的 bucket_factor，
            # 并在标题和文件名中标明，例如 custom=b32。
            custom_label = ""
            custom_rows = subset[subset["strategy"] == "custom"]
            if not custom_rows.empty and "bucket_factor" in custom_rows.columns:
                bf_val = str(custom_rows.iloc[-1]["bucket_factor"])  # 只会有一条
                if bf_val:
                    custom_label = f", custom={bf_val}"
            title = f"{col} ({scene_tag}, p{partitions}{custom_label}): hash vs range vs custom"
            title = title.replace("task_p90_ms", "Task p90").replace(
                "task_p99_ms", "Task p99"
            ).replace("job_total_ms", "Job total time")

            bucket_suffix = ""
            if custom_label:
                # custom_label 形如 ", custom=b32"，这里只取 b32
                bucket_suffix = "_" + custom_label.split("=")[-1]

            out_name = f"{col}_{name}{bucket_suffix}.png"

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

    # 针对每个 (records_tag, partitions) 组合，若存在至少两个不同的 bucket_factor，
    # 则自动画一张 "custom 不同 bucket_factor" 的对比图。
    for key in scenarios_keys:
        scene_tag = str(key["scene_tag"])
        partitions = str(key["partitions"])

        mask_custom = (
            (df["scene_tag"] == scene_tag)
            & (df["partitions"].astype(str) == str(partitions))
            & (df["strategy"] == "custom")
            & (df["bucket_factor"].notna())
        )
        subset_custom = df[mask_custom]
        if subset_custom.empty:
            continue

        # bucket_factor 本身已经是 "b4" / "b8" / "b32"，只保留我们感兴趣的几个，且至少有两个值
        subset_custom = subset_custom[subset_custom["bucket_factor"].astype(str).isin(skew_custom_buckets)]
        if subset_custom["bucket_factor"].nunique() < 2:
            continue
        subset_custom = subset_custom.sort_values("file")
        latest_by_b = subset_custom.groupby("bucket_factor").tail(1)
        latest_by_b = latest_by_b.sort_values("bucket_factor")

        # 文件名中带上 bucket_factor 信息，例如 ..._b4-b8-b32
        bucket_tags = "-".join(str(b) for b in latest_by_b["bucket_factor"].astype(str).tolist())
        skew_name = f"{scene_tag}_p{partitions}_custom_buckets_{bucket_tags}"

        for m in metrics_to_plot:
            col = m["col"]
            label = m["label"]

            xs = [str(b) for b in latest_by_b["bucket_factor"].astype(str).tolist()]
            ys = latest_by_b[col].tolist()

            plt.figure(figsize=(5.5, 4))
            plt.bar(xs, ys, color="#59a14f")
            plt.ylabel(label)
            title = f"{col} ({scene_tag}, p{partitions}, buckets={bucket_tags}): custom with different buckets"
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