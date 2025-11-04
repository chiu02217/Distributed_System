from abc import ABC, abstractmethod

class ICoordinatorSnapshotHandler(ABC):
    
    @abstractmethod
    def initiate_snapshot(self):
        pass

    @abstractmethod
    def process_result_from_worker(self, request):
        pass

    @abstractmethod
    def save_state_to_afs(self, filename, state_data):
        pass

    @abstractmethod
    def get_all_worker_ids(self):
        pass

    @abstractmethod
    def send_snapshot_id_to_worker(self, response):
        pass