from abc import ABC, abstractmethod


class AbstractTask(ABC):
    """Abstract base class for a task within a pipeline."""
    def __init__(self, name, action_type, object_type):
        super().__init__()
        self.name = name
        self.action_type = action_type
        self.object_type = object_type

    @abstractmethod
    def run(self):
        """Run the task."""
        pass
