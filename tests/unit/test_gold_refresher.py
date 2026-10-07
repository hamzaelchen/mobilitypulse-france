"""Gold refresher loop: one process per cycle, clean stop, survives a failing run."""

import threading

from streaming.gold_refresher import run_loop


class Clock:
    """Stand-in for the stop event: records waits and stops after ``runs`` runs."""

    def __init__(self, runs):
        self.runs_left = runs
        self.waits = []
        self.stopped = False

    def is_set(self):
        return self.stopped

    def wait(self, seconds):
        self.waits.append(seconds)
        if self.runs_left <= 0:
            self.stopped = True
        return self.stopped

    def set(self):
        self.stopped = True


def test_runs_then_waits_the_interval_and_repeats():
    clock = Clock(runs=3)
    calls = []

    def run_once():
        calls.append(1)
        clock.runs_left -= 1
        return 0

    run_loop(run_once, interval=900, stop=clock)
    assert len(calls) == 3 and clock.waits == [900, 900, 900]


def test_a_failing_run_does_not_stop_the_loop():
    clock = Clock(runs=2)

    def run_once():
        clock.runs_left -= 1
        if clock.runs_left == 1:
            raise RuntimeError("spark failed")
        return 3  # non-zero exit code

    run_loop(run_once, interval=10, stop=clock)
    assert clock.waits == [10, 10]


def test_no_run_starts_once_stop_is_requested():
    stop = threading.Event()
    stop.set()
    calls = []
    run_loop(lambda: calls.append(1) or 0, interval=10, stop=stop)
    assert calls == []
