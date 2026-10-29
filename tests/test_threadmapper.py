import os
import subprocess
import sys
from textwrap import dedent

import pytest

from dfb.threadmapper import thread_map_unordered


@pytest.mark.parametrize(
    "mode", ["worker_error", "source_error", "source_empty_error", "close", "interrupt"]
)
def test_failure_cleanup(mode):
    # Isolate hang regressions so a broken mapper cannot hang pytest itself.
    script = dedent("""\
        import itertools
        import sys
        import threading
        import time
        from dfb.threadmapper import thread_map_unordered

        mode = sys.argv[1]

        def source():
            if mode == "source_empty_error":
                raise ValueError("source failed")
            for value in itertools.count():
                if mode == "source_error" and value == 5:
                    raise ValueError("source failed")
                yield value

        def worker(value):
            if mode == "worker_error" and value == 5:
                raise ValueError("worker failed")
            if mode == "interrupt" and value == 5:
                raise KeyboardInterrupt("worker interrupted")
            return value

        mapped = thread_map_unordered(worker, source(), Nt=3, Nout_buffer=1)
        if mode == "close":
            next(mapped)
            time.sleep(0.1)  # Let workers and producer fill both bounded queues.
            mapped.close()
        else:
            expected = KeyboardInterrupt if mode == "interrupt" else ValueError
            try:
                list(mapped)
            except expected:
                pass
            else:
                raise AssertionError("Expected failure was not propagated")

        assert threading.enumerate() == [threading.main_thread()]
        """)
    subprocess.run(
        [sys.executable, "-c", script, mode],
        check=True,
        timeout=10,
        env=os.environ.copy(),
    )


@pytest.mark.parametrize("buffer", [1, -1])
def test_results_and_returned_errors(buffer):
    def worker(value):
        if value == 3:
            raise ValueError("failed")
        return value * 2

    results = list(
        thread_map_unordered(
            worker, range(10), Nt=3, Nin_buffer=buffer, raise_exceptions=False
        )
    )
    errors = [result for result in results if isinstance(result, Exception)]
    assert len(errors) == 1
    assert errors[0].seq_index == 3
    assert errors[0].thread_map_unordered_exception is True
    assert sorted(
        result for result in results if not isinstance(result, Exception)
    ) == [value * 2 for value in range(10) if value != 3]
    # Returning an exception as a value does not turn it into a raised error.
    error = ValueError("ordinary value")
    assert list(thread_map_unordered(lambda _: error, [0], Nt=2)) == [error]
    assert list(thread_map_unordered(worker, [], Nt=2)) == []
