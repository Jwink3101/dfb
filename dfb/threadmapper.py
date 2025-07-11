import logging
import os
from queue import Queue
from threading import Thread

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
    # Use to know if an exceptoion was raised here and ONLY here. Will be replaced
    thread_map_unordered_exception = os.urandom(5)
    kill = _KILL()

    Nt = Nt or os.cpu_count()

    # Limit the input queue so as to not pull the input iterator too quickly
    qin = Queue(maxsize=Nin_buffer)
    qout = Queue(maxsize=Nout_buffer or Nt)

    def _adder():
        for ii, item in enumerate(seq):
            qin.put((ii, item))
        for _ in range(Nt):
            qin.put((-1, kill))

    adder_thread = Thread(target=_adder)
    adder_thread.start()

    def _worker():
        while True:
            ii, item = qin.get()
            if item is kill:
                qout.put(item)
                qin.task_done()
                break

            try:
                res = fun(item)
            except Exception as _res:
                res = _res  # I don't get it but this is needed
                res.seq_index = ii
                res.thread_map_unordered_exception = thread_map_unordered_exception

            qout.put(res)
            qin.task_done()

    worker_threads = [Thread(target=_worker) for _ in range(Nt)]
    for worker_thread in worker_threads:
        worker_thread.start()

    tcount = 0
    while tcount < Nt:
        res = qout.get()

        # Handle exceptions back in the main thread
        if (
            isinstance(res, Exception)
            and getattr(res, "thread_map_unordered_exception", False)
            == thread_map_unordered_exception
        ):
            res.thread_map_unordered_exception = True  # reset
            if raise_exceptions:
                raise res

        if res is kill:
            tcount += 1
            qout.task_done()
            continue
        yield res
        qout.task_done()

    qin.join()
    qout.join()
    adder_thread.join()
    for worker_thread in worker_threads:
        worker_thread.join()
