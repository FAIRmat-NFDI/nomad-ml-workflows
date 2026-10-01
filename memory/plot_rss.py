"""Plot CPU worker RSS measurements saved by memory_rss.py."""

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path


def plot_rss_results(json_path: Path) -> Path:
    """Create a PNG beside a saved RSS JSON file and return its path."""
    json_path = Path(json_path)
    with json_path.open(encoding='utf-8') as input_file:
        result = json.load(input_file)

    rss_values_mb = result['rss_values_mb']
    rss_timestamps_utc = result['rss_timestamps_utc']
    if not rss_values_mb:
        raise ValueError('No RSS values were collected.')
    if len(rss_values_mb) != len(rss_timestamps_utc):
        raise ValueError('RSS values and timestamps must have the same length.')

    sample_timestamps = [
        datetime.fromisoformat(timestamp) for timestamp in rss_timestamps_utc
    ]
    plot_intervals = []
    for interval in result['workflow_intervals']:
        start_timestamp = datetime.fromisoformat(interval['start_timestamp_utc'])
        stop_timestamp = datetime.fromisoformat(interval['stop_timestamp_utc'])
        if stop_timestamp < start_timestamp:
            raise ValueError('Workflow stop timestamp must not precede its start.')
        plot_intervals.append((interval, start_timestamp, stop_timestamp))

    from matplotlib import dates as mdates
    from matplotlib import pyplot as plt

    plot_path = json_path.with_suffix('.png')
    figure, axis = plt.subplots(figsize=(12, 6))
    try:
        axis.plot(sample_timestamps, rss_values_mb, color='black', linewidth=1.5)

        for index, (interval, start_timestamp, stop_timestamp) in enumerate(
            plot_intervals
        ):
            color = f'C{index % 10}'
            axis.axvspan(
                start_timestamp,
                stop_timestamp,
                color=color,
                alpha=0.15,
                label=(
                    f'Workflow {interval["workflow"]}: '
                    f'num_entries {interval["num_entries"]}'
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
    finally:
        plt.close(figure)

    print(f'Saved RSS plot to: {plot_path}', flush=True)
    return plot_path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path, help='RSS results directory')
    args = parser.parse_args()
    json_path = args.directory / 'cpuworker_rss.json'

    try:
        plot_rss_results(json_path)
    except (OSError, ValueError, KeyError, TypeError) as exc:
        parser.exit(1, f'Could not plot {json_path}: {exc}\n')


if __name__ == '__main__':
    main()
