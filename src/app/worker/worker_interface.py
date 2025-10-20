from abc import ABC, abstractmethod

class IWorker(ABC):

    @abstractmethod
    def run_task(self):
        pass