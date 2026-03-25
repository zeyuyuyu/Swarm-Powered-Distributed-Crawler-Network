import asyncio
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional, Dict, List
from uuid import uuid4

class TaskPriority(Enum):
    LOW = 0
    MEDIUM = 1
    HIGH = 2
    CRITICAL = 3

@dataclass
class Task:
    id: str
    url: str
    priority: TaskPriority
    created_at: datetime
    completed_at: Optional[datetime] = None
    retries: int = 0
    max_retries: int = 3
    metadata: Dict = None

class DistributedTaskQueue:
    def __init__(self):
        self.tasks: Dict[TaskPriority, List[Task]] = {
            p: [] for p in TaskPriority
        }
        self.processing: Dict[str, Task] = {}
        self.completed: Dict[str, Task] = {}
        self._lock = asyncio.Lock()
    
    async def push(self, url: str, priority: TaskPriority = TaskPriority.MEDIUM, metadata: Dict = None) -> str:
        """Add a new task to the queue with specified priority"""
        task = Task(
            id=str(uuid4()),
            url=url,
            priority=priority,
            created_at=datetime.utcnow(),
            metadata=metadata or {}
        )
        async with self._lock:
            self.tasks[priority].append(task)
        return task.id

    async def pop(self) -> Optional[Task]:
        """Get highest priority task available"""
        async with self._lock:
            for priority in reversed(list(TaskPriority)):
                if self.tasks[priority]:
                    task = self.tasks[priority].pop(0)
                    self.processing[task.id] = task
                    return task
        return None

    async def complete(self, task_id: str) -> None:
        """Mark task as completed"""
        async with self._lock:
            if task_id in self.processing:
                task = self.processing.pop(task_id)
                task.completed_at = datetime.utcnow()
                self.completed[task_id] = task

    async def retry(self, task_id: str) -> bool:
        """Retry a failed task if retries remain"""
        async with self._lock:
            if task_id in self.processing:
                task = self.processing.pop(task_id)
                if task.retries < task.max_retries:
                    task.retries += 1
                    self.tasks[task.priority].append(task)
                    return True
        return False

    async def stats(self) -> Dict:
        """Get queue statistics"""
        return {
            'queued': sum(len(tasks) for tasks in self.tasks.values()),
            'processing': len(self.processing),
            'completed': len(self.completed),
            'by_priority': {
                p.name: len(tasks) for p, tasks in self.tasks.items()
            }
        }
