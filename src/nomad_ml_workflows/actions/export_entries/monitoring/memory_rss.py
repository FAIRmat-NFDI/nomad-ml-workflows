import json
import threading
import time
from datetime import datetime, timezone
from functools import wraps
from pathlib import Path

import psutil
from nomad.actions.manager import get_action_result, get_action_status, start_action

from nomad_ml_workflows.actions.export_entries.models import ExportEntriesUserInput

RSS_PRINT_INTERVAL = 10.0
RSS_LOG_INTERVAL = 0.1
RSS_BASELINE_SECONDS = 15.0
RSS_PAUSE_SECONDS = 15.0
WORKFLOW_COMPLETION_WAIT_SECONDS = 0.1


def _get_worker_rss_mb(worker_name: str) -> float | None:
    processes: dict[int, psutil.Process] = {}

    for process in psutil.process_iter(['name', 'cmdline']):
        try:
            name = process.info['name'] or ''
            command = ' '.join(process.info['cmdline'] or [])
            if worker_name not in name and worker_name not in command:
                continue

            processes[process.pid] = process
            for child in process.children(recursive=True):
                processes[child.pid] = child
        except (psutil.AccessDenied, psutil.NoSuchProcess, psutil.ZombieProcess):
            continue

    if not processes:
        return None

    rss_bytes = 0
    for process in processes.values():
        try:
            rss_bytes += process.memory_info().rss
        except (psutil.AccessDenied, psutil.NoSuchProcess, psutil.ZombieProcess):
            continue

    return rss_bytes / 1_000_000


def monitor_rss(
    worker_name: str = 'action-cpu-worker',
    print_interval: float = 10,
    log_interval: float = 1,
):
    """
    Decorate a synchronous function and monitor a named worker process while it runs.

    RSS samples, in MB, and their UTC timestamps are available through
    ``decorated_function.rss_values`` and
    ``decorated_function.rss_timestamps_utc`` after each call. Process names and
    command lines are searched for ``worker_name``. RSS from matching processes and
    all their descendants is summed without double-counting PIDs.
    """
    if not worker_name:
        raise ValueError('worker_name must not be empty.')
    if print_interval <= 0:
        raise ValueError('print_interval must be greater than zero.')
    if log_interval <= 0:
        raise ValueError('log_interval must be greater than zero.')

    rss_values: list[float] = []
    rss_timestamps_utc: list[str] = []

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            rss_values.clear()
            rss_timestamps_utc.clear()
            stop = threading.Event()
            started_at = time.monotonic()

            def sample_rss():
                next_log = started_at
                next_print = started_at

                while not stop.is_set():
                    now = time.monotonic()
                    should_log = now >= next_log
                    should_print = now >= next_print

                    if should_log or should_print:
                        rss_mb = _get_worker_rss_mb(worker_name)
                        sampled_at_utc = datetime.now(timezone.utc).isoformat()

                        if should_log:
                            if rss_mb is not None:
                                rss_values.append(rss_mb)
                                rss_timestamps_utc.append(sampled_at_utc)
                            next_log = now + log_interval

                        if should_print:
                            if rss_mb is None:
                                print(
                                    f'[{sampled_at_utc}] [{worker_name}] '
                                    'worker process not found',
                                    flush=True,
                                )
                            else:
                                print(
                                    f'[{sampled_at_utc}] [{worker_name}] '
                                    f'RSS: {rss_mb:.2f} MB',
                                    flush=True,
                                )
                            next_print = now + print_interval

                    wait_time = max(
                        0,
                        min(next_log, next_print) - time.monotonic(),
                    )
                    stop.wait(wait_time)

            monitor = threading.Thread(
                target=sample_rss,
                name=f'{worker_name}-rss-monitor',
                daemon=True,
            )
            monitor.start()

            try:
                return func(*args, **kwargs)
            finally:
                stop.set()
                monitor.join()

        wrapper.rss_values = rss_values
        wrapper.rss_timestamps_utc = rss_timestamps_utc
        return wrapper

    return decorator


def save_rss_results(
    rss_values_mb: list[float],
    rss_timestamps_utc: list[str],
    workflow_intervals: list[dict],
    log_interval: float,
    baseline_seconds: float = 0,
) -> tuple[Path, Path]:
    """Save timestamped RSS samples and plot measured workflow windows."""
    if not rss_values_mb:
        raise ValueError('No RSS values were collected.')
    if len(rss_values_mb) != len(rss_timestamps_utc):
        raise ValueError('RSS values and timestamps must have the same length.')
    if log_interval <= 0:
        raise ValueError('log_interval must be greater than zero.')

    sample_timestamps = [
        datetime.fromisoformat(timestamp) for timestamp in rss_timestamps_utc
    ]
    plot_intervals = []
    for interval in workflow_intervals:
        start_timestamp = datetime.fromisoformat(interval['start_timestamp_utc'])
        stop_timestamp = datetime.fromisoformat(interval['stop_timestamp_utc'])
        if stop_timestamp < start_timestamp:
            raise ValueError('Workflow stop timestamp must not precede its start.')
        plot_intervals.append((interval, start_timestamp, stop_timestamp))

    output_directory = Path(__file__).with_name(
        f'logging_rss_monitoring_results_{rss_timestamps_utc[0]}'
    )
    output_directory.mkdir(parents=True, exist_ok=True)
    json_path = output_directory / 'cpuworker_rss.json'
    plot_path = output_directory / 'cpuworker_rss.png'

    result = {
        'rss_unit': 'MB',
        'log_interval_seconds': log_interval,
        'baseline_seconds': baseline_seconds,
        'pause_after_workflow_seconds': RSS_PAUSE_SECONDS,
        'rss_values_mb': rss_values_mb,
        'rss_timestamps_utc': rss_timestamps_utc,
        'time_taken_seconds': [
            interval['reported_duration_seconds'] for interval in workflow_intervals
        ],
        'page_sizes': [interval['page_size'] for interval in workflow_intervals],
        'workflow_intervals': workflow_intervals,
    }
    with json_path.open('w', encoding='utf-8') as output_file:
        json.dump(result, output_file, indent=2)

    from matplotlib import dates as mdates
    from matplotlib import pyplot as plt

    figure, axis = plt.subplots(figsize=(12, 6))
    axis.plot(sample_timestamps, rss_values_mb, color='black', linewidth=1.5)

    for index, (interval, start_timestamp, stop_timestamp) in enumerate(plot_intervals):
        color = f'C{index % 10}'
        axis.axvspan(
            start_timestamp,
            stop_timestamp,
            color=color,
            alpha=0.15,
            label=(
                f'Workflow {interval["workflow"]}: page size {interval["page_size"]}'
            ),
        )
        axis.axvline(start_timestamp, color=color, linestyle=':', alpha=0.8)
        axis.axvline(stop_timestamp, color=color, linestyle='--', alpha=0.8)

    axis.set_title('CPU worker RSS by export workflow')
    axis.set_xlabel('Absolute timestamp (UTC)')
    axis.set_ylabel('RSS (MB)')
    axis.xaxis.set_major_formatter(
        mdates.DateFormatter('%Y-%m-%d\n%H:%M:%S', tz=timezone.utc)
    )
    axis.grid(alpha=0.25)
    if plot_intervals:
        axis.legend()

    figure.autofmt_xdate()
    figure.tight_layout()
    figure.savefig(plot_path, dpi=160)
    plt.close(figure)

    print(f'Saved RSS values to: {json_path}', flush=True)
    print(f'Saved RSS plot to: {plot_path}', flush=True)

    return json_path, plot_path


def execute_workflow(data: ExportEntriesUserInput) -> tuple[float, str, str]:
    started_at_utc = datetime.now(timezone.utc)
    action_instance_id = start_action('nomad_ml_workflows.actions:export_entries', data)
    print(f'Started workflow for page size: {data.search_settings.page_size}')

    action_result = wait_and_get_result(
        action_instance_id, data.user_id, wait=WORKFLOW_COMPLETION_WAIT_SECONDS
    )
    print(
        f'Page size: {data.search_settings.page_size}, Time taken: {action_result["workflow_duration"]}'
    )
    stopped_at_utc = datetime.now(timezone.utc)

    return (
        action_result['workflow_duration'],
        started_at_utc.isoformat(),
        stopped_at_utc.isoformat(),
    )


def wait_and_get_result(action_instance_id: str, user_id: str, wait: float = 10):
    # Check if the action has completed at 'wait' second intervals
    while True:
        if get_action_status(action_instance_id, user_id).name != 'RUNNING':
            return get_action_result(action_instance_id, user_id)
        time.sleep(wait)


@monitor_rss(
    print_interval=RSS_PRINT_INTERVAL,
    log_interval=RSS_LOG_INTERVAL,
)
def execute_workflows_serial(data: list[ExportEntriesUserInput]):
    # idle for some time to allow worker RSS monitoring to start
    # and log baseline RSS
    time.sleep(RSS_BASELINE_SECONDS)
    time_taken = []
    workflow_intervals = []
    for index, d in enumerate(data, start=1):
        duration, started_at_utc, stopped_at_utc = execute_workflow(d)
        time_taken.append(duration)
        workflow_intervals.append(
            {
                'workflow': index,
                'page_size': d.search_settings.page_size,
                'start_timestamp_utc': started_at_utc,
                'stop_timestamp_utc': stopped_at_utc,
                'reported_duration_seconds': float(duration),
                'measured_duration_seconds': (
                    datetime.fromisoformat(stopped_at_utc)
                    - datetime.fromisoformat(started_at_utc)
                ).total_seconds(),
            }
        )
        print(
            f'Pausing for {RSS_PAUSE_SECONDS:.1f} seconds after workflow {index}.',
            flush=True,
        )
        time.sleep(RSS_PAUSE_SECONDS)

    return time_taken, workflow_intervals


if __name__ == '__main__':
    data: list[ExportEntriesUserInput] = []
    export_entries_user_input = ExportEntriesUserInput.model_validate(
        {
            'upload_id': 'GDnrdwJrSrCOlZzlhZBbCw',
            'user_id': 'be691000-501a-462a-99b2-873aa11fbe1e',
            'search_settings': {
                'owner': 'public',  ## changed this
                'page_size': 100,
                # 'query': "{'entry_type': 'CatalyticReaction'}",  ## changed this
                'query': "{'entry_type': 'PerovskiteSolarCell'}",  ## changed this
                'required': [
                    {
                        'type': 'include',
                        'path': 'results',
                        'resolve_references': False,
                    },
                    {
                        'type': 'include',
                        'path': 'data',
                        'resolve_references': False,
                    },
                ],
            },
            'output_settings': {
                'output_file_format': 'parquet',
                'zip_output': False,
            },
        }
    )

    # Generate data for different page sizes
    # for page_size in reversed([10, 100, 1000] * 1):
    for page_size in [10000] * 1:
        model = export_entries_user_input.model_copy(deep=True)
        model.search_settings.page_size = page_size
        data.append(model)

    time_taken, workflow_intervals = execute_workflows_serial(data)
    rss_values_mb = execute_workflows_serial.rss_values
    rss_timestamps_utc = execute_workflows_serial.rss_timestamps_utc

    save_rss_results(
        rss_values_mb,
        rss_timestamps_utc,
        workflow_intervals,
        RSS_LOG_INTERVAL,
        baseline_seconds=RSS_BASELINE_SECONDS,
    )

    print(f'Peak RSS: {max(rss_values_mb):.2f} MB')
    print(f'time_taken: {time_taken}')
