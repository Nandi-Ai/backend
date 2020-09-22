import multiprocessing
from concurrent.futures.thread import ThreadPoolExecutor

executor = ThreadPoolExecutor(max_workers=multiprocessing.cpu_count() * 2 - 1)
