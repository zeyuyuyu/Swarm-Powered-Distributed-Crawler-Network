import time
import random
from typing import List, Tuple
from .tasks import crawl_site, CrawlResult

class DistributedCrawler:
    def __init__(self, num_workers: int = 10):
        self.workers = [Worker(id=i) for i in range(num_workers)]
        self.task_queue: List[Tuple[str, int]] = []
        self.results: List[CrawlResult] = []

    def add_task(self, url: str, priority: int = 1):
        self.task_queue.append((url, priority))

    def run(self):
        while self.task_queue or any(worker.is_busy for worker in self.workers):
            self.assign_tasks()
            self.collect_results()
            time.sleep(0.1)

    def assign_tasks(self):
        idle_workers = [worker for worker in self.workers if not worker.is_busy]
        if idle_workers:
            self.task_queue.sort(key=lambda x: x[1], reverse=True)
            for worker in idle_workers:
                if self.task_queue:
                    url, priority = self.task_queue.pop(0)
                    worker.start_task(url, priority)

    def collect_results(self):
        for worker in self.workers:
            if worker.has_result:
                self.results.append(worker.get_result())

class Worker:
    def __init__(self, id: int):
        self.id = id
        self.is_busy = False
        self.current_task: Tuple[str, int] = ('', 0)
        self.result: CrawlResult = None

    def start_task(self, url: str, priority: int):
        self.is_busy = True
        self.current_task = (url, priority)
        self.result = crawl_site(url)

    def get_result(self) -> CrawlResult:
        self.is_busy = False
        result = self.result
        self.result = None
        return result

    @property
    def has_result(self) -> bool:
        return self.result is not None
