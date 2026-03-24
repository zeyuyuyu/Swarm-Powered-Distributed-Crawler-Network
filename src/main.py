import asyncio
import random
import time

class TaskCoordinator:
    def __init__(self, num_workers):
        self.num_workers = num_workers
        self.task_queue = asyncio.Queue()
        self.worker_tasks = []

    async def add_task(self, task):
        await self.task_queue.put(task)

    async def worker(self):
        while True:
            task = await self.task_queue.get()
            print(f'Worker {id(self)} processing task: {task}')
            await asyncio.sleep(random.uniform(1, 5))
            self.task_queue.task_done()

    async def run(self):
        self.worker_tasks = [asyncio.create_task(self.worker()) for _ in range(self.num_workers)]
        await self.task_queue.join()
        for task in self.worker_tasks:
            task.cancel()

async def main():
    coordinator = TaskCoordinator(num_workers=5)

    # Add some tasks to the queue
    for i in range(20):
        await coordinator.add_task(f'Task {i}')

    await coordinator.run()

if __name__ == '__main__':
    asyncio.run(main())