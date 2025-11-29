import json
import os
import sys
from typing import Dict, Any, List


def parse_event_log(path: str) -> Dict[str, Any]:
    """
    解析一个 Spark event log，提取：
      - job_count, stage_count, task_count
      - job_durations
      - stage_durations
      - total_shuffle_read / write
      - task_durations (用于 tail task 分析)
    """

    jobs: Dict[str, Dict[str, Any]] = {}
    stages: Dict[str, Dict[str, Any]] = {}
    tasks: List[Dict[str, Any]] = []

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            event = json.loads(line)
            event_type = event.get("Event")

            # Job start / end
            if event_type == "SparkListenerJobStart":
                job_id = str(event["Job ID"])
                jobs[job_id] = jobs.get(job_id, {})
                jobs[job_id]["submissionTime"] = event.get("Submission Time")
            elif event_type == "SparkListenerJobEnd":
                job_id = str(event["Job ID"])
                jobs[job_id] = jobs.get(job_id, {})
                jobs[job_id]["completionTime"] = event.get("Completion Time")

            # Stage start / end
            elif event_type == "SparkListenerStageSubmitted":
                stage_info = event.get("Stage Info", {})
                stage_id = str(stage_info.get("Stage ID"))
                stages[stage_id] = stages.get(stage_id, {})
                stages[stage_id]["submissionTime"] = stage_info.get("Submission Time")
            elif event_type == "SparkListenerStageCompleted":
                stage_info = event.get("Stage Info", {})
                stage_id = str(stage_info.get("Stage ID"))
                stages[stage_id] = stages.get(stage_id, {})
                stages[stage_id]["completionTime"] = stage_info.get("Completion Time")
                # Shuffle 读写量在 Stage 级别的累积里
                task_metrics = stage_info.get("Task Metrics", {})
                shuffle_read = task_metrics.get("Shuffle Read Metrics", {}).get("Total Bytes Read", 0)
                shuffle_write = task_metrics.get("Shuffle Write Metrics", {}).get("Shuffle Bytes Written", 0)
                stages[stage_id]["shuffle_read_bytes"] = shuffle_read
                stages[stage_id]["shuffle_write_bytes"] = shuffle_write

            # Task end (用于统计 task durations / tail tasks)
            elif event_type == "SparkListenerTaskEnd":
                task_info = event.get("Task Info", {})
                task_metrics = event.get("Task Metrics", {})

                launch_time = task_info.get("Launch Time")
                finish_time = task_info.get("Finish Time")
                duration = None
                if launch_time is not None and finish_time is not None:
                    duration = finish_time - launch_time

                tasks.append(
                    {
                        "taskId": task_info.get("Task ID"),
                        "stageId": task_info.get("Stage ID"),
                        "launchTime": launch_time,
                        "finishTime": finish_time,
                        "duration": duration,
                        "host": task_info.get("Host"),
                        "attempt": task_info.get("Attempt"),
                        "speculative": task_info.get("Speculative"),
                    }
                )

    # 汇总 Job/Stage 时间
    job_durations = []
    for job_id, info in jobs.items():
        st = info.get("submissionTime")
        ct = info.get("completionTime")
        if st is not None and ct is not None:
            job_durations.append({"jobId": job_id, "duration_ms": ct - st})

    stage_durations = []
    total_shuffle_read = 0
    total_shuffle_write = 0
    for stage_id, info in stages.items():
        st = info.get("submissionTime")
        ct = info.get("completionTime")
        if st is not None and ct is not None:
            stage_durations.append({"stageId": stage_id, "duration_ms": ct - st})

        total_shuffle_read += info.get("shuffle_read_bytes", 0)
        total_shuffle_write += info.get("shuffle_write_bytes", 0)

    # Task durations（用于 tail task 分析）
    task_durations = [t["duration"] for t in tasks if t["duration"] is not None]
    task_durations.sort()
    tail_info = {}
    if task_durations:
        tail_info = {
            "min_ms": task_durations[0],
            "p50_ms": task_durations[len(task_durations) // 2],
            "p90_ms": task_durations[int(len(task_durations) * 0.9)],
            "p99_ms": task_durations[int(len(task_durations) * 0.99) - 1],
            "max_ms": task_durations[-1],
            "count": len(task_durations),
        }

    return {
        "job_count": len(jobs),
        "stage_count": len(stages),
        "task_count": len(tasks),
        "job_durations_ms": job_durations,
        "stage_durations_ms": stage_durations,
        "total_shuffle_read_bytes": total_shuffle_read,
        "total_shuffle_write_bytes": total_shuffle_write,
        "task_duration_stats_ms": tail_info,
    }


def main():
    if len(sys.argv) < 3:
        print(
            "Usage: python code/tools/parse_spark_eventlog.py <event_log_path> <output_json_path>",
            file=sys.stderr,
        )
        sys.exit(1)

    event_log_path = sys.argv[1]
    output_path = sys.argv[2]

    summary = parse_event_log(event_log_path)

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print(f"Summary written to {output_path}")


if __name__ == "__main__":
    main()