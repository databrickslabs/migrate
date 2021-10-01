from abc import ABC, abstractmethod


class AbstractTask(ABC):
    """Abstract base class for a task within a pipeline."""
    def __init__(self, name):
        super().__init__()
        self.name = name

    @abstractmethod
    def run(self):
        """Run the task."""
        pass
