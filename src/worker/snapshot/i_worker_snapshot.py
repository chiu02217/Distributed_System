from abc import ABC, abstractmethod

class IWorkerSnapshotHandler(ABC):
    
    @abstractmethod
    def handle_snapshot(self, snapshot_id):
        pass
    
    @abstractmethod
    def save_current_progress(self):
        pass