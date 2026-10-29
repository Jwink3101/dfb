import logging
import os
from queue import Empty, Full, Queue
from threading import Event, Thread

logger = logging.getLogger(__name__)


class _KILL:
    pass


class ReturnThread(Thread):
    """
    Like a regular thread except when you `join`, it returns the function
    result. And .start() will return itself to enable cleaner code.

        >>> mythread = ReturnThread(...).start() # instantiate and start

    Note that target is a required keyword argument.
    """

    def __init__(self, *, target, **kwargs):
        self.target = target
        super().__init__(target=self._target, **kwargs)
        self._res = None

    def start(self, *args, **kwargs):
        super().start(*args, **kwargs)
        return self

    def _target(self, *args, **kwargs):
        self._res = self.target(*args, **kwargs)

    def join(self, *args, **kwargs):
        super().join(*args, **kwargs)
        return self._res


# I like my thread_map_unordered more since it provides better control of the buffers
# and I am pretty sure it works just fine. I have an alternative in comments that I used
# when I was getting a deadlock. It turned out to be an sqlite3 one (due to an
# executemany and a select further up the chain). I will keep it in comments here in case
# I need to go back to it.

# import multiprocessing.dummy as mpd
# def thread_map_unordered(fun,seq,Nt=None,**_):
#     with mpd.Pool(processes=Nt) as pool:
#         yield from pool.imap_unordered(fun,seq)


def thread_map_unordered(
    fun,
    seq,
    Nt=None,
    *,
    Nin_buffer=1,
    Nout_buffer=None,
    raise_exceptions=True,
):
    """
    Simple parallel mapping function.

    Inputs:
    -------
    fun
        Function to map

    seq
        Sequence

    Nt [None]
        Number of threads. Defaults to os.cpu_count()

    Nin_buffer [1]
        Number of input items to pull at a time. Set to -1 to be infinite (will exhaust
        the iterator as fast as possible). A value of 1 will most closely resemble
        a regular map() call

    Nout_buffer [None]
        Amount to buffer out. It is good to make this a bit larger than the input
        so that the threads can keep working. If None, will be Nt meaning 2*Nt can be
        cached

    raise_exceptions [True]
        If True (default), will raise exceptions.

        If false, it will *return* the exception object Note that an additional attribute
        'seq_index' will be set with the offending index and 'thread_map_unordered_exception' will be True.
        This can also be used, if needed, to know whether the exception came from this function or 'fun'
        in the off chance that an exception is a valid return from 'fun'.

    Notes
    -----
    Source iteration errors always propagate. On failure or explicit iterator
    close(), queued work is cancelled and all threads are joined. Calls already
    running (including source iteration) must finish before cleanup can return.
    Close the iterator explicitly if stopping consumption early.


    Why:
    ----
    This function seems uneccesary since there is ThreadPoolExecutor and multiprocessing.dummy.Pool.
    Both of those functions are perfectly fine but this function has two defining differences:

    (1) The ability to control how quickly the input sequence is exhausted and results yielded.
        The other tools will pull all items as fast as possible which can be very memory
        intensive. The default value of one means that it will iterate just as fast as it can but
        never faster. This could bottleneck upstream but that is a good thing for memory usage and control

    (2) Can automatically wrap exceptions without an additional code. See options above
    """
    kill = _KILL()
    stopped = Event()

    Nt = Nt or os.cpu_count()

    # Limit the input queue so as to not pull the input iterator too quickly
    qin = Queue(maxsize=Nin_buffer)
    qout = Queue(maxsize=Nout_buffer or Nt)

    def _put(queue, item):
        while not stopped.is_set():
            try:
                queue.put(item, timeout=0.05)
                return True
            except Full:
                pass
        return False

    def _adder():
        try:
            for ii, item in enumerate(seq):
                if not _put(qin, (ii, item)):
                    break
        except BaseException as error:
            # Source iteration errors must reach the consumer too; otherwise
            # workers would wait forever for input or completion sentinels.
            _put(qout, (None, error, True))
        finally:
            for _ in range(Nt):
                if not _put(qin, (-1, kill)):
                    break

    def _worker():
        while not stopped.is_set():
            try:
                ii, item = qin.get(timeout=0.05)
            except Empty:
                continue
            if item is kill:
                _put(qout, kill)
                break

            if stopped.is_set():
                break
            failed = False
            try:
                res = fun(item)
            except BaseException as error:
                res = error
                failed = True

            if not _put(qout, (ii, res, failed)):
                break

    adder_thread = Thread(target=_adder)
    worker_threads = [Thread(target=_worker) for _ in range(Nt)]
    started_threads = []
    try:
        for thread in [*worker_threads, adder_thread]:
            thread.start()
            started_threads.append(thread)

        tcount = 0
        while tcount < Nt:
            outcome = qout.get()
            if outcome is kill:
                tcount += 1
                continue
            ii, res, failed = outcome
            if failed:
                res.seq_index = ii
                res.thread_map_unordered_exception = True
                if raise_exceptions or ii is None or not isinstance(res, Exception):
                    raise res
            yield res
    finally:
        stopped.set()
        # Running function calls/source iteration must finish, but no thread
        # remains blocked on a full output/input queue after the consumer exits.
        for thread in started_threads:
            thread.join()
