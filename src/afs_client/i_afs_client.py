from abc import ABC, abstractmethod

class IAFSClient(ABC):

    @abstractmethod
    def open_file(self, filename):
        """
        as name
        
        Args:
            filename (str): The name of the remote file to open.

        Returns:
            A file u want to open
        """
        pass

    @abstractmethod
    def write_file(self, open_file_handle, data):
        """
        as name
        
        Args:
            handle: The handle obtained from open_file or create_file.
            data (str or int): The data to write.

        Returns:
            bool: Whether the write was successful.
        """
        pass

    @abstractmethod
    def read_file(self, open_file_handle):
        """
         as name
        """
        pass

    @abstractmethod
    def create_file(self, filename):
        """
       as name 
        """
        pass

    @abstractmethod
    def close_file(self, open_file_handle):
        """
        as name
        """
        pass

    @abstractmethod
    def list_files(self, file_store_path):
        """
        List all files stored on the AFS server.

        Returns:
            list: A list of filenames.
        """
        pass

    @abstractmethod
    def find_latest_coordinator_snapshot(self):
        """
        Find the latest coordinator snapshot file in AFS.

        Returns:
            str | None : The filename of the latest snapshot, or None if not found.
        """
        pass

    @abstractmethod
    def read_json_file(self, open_file):
        """
        For reading snapshot file
        """
        pass

    @abstractmethod
    def find_latest_worker_snapshot(self, worker_id:str):
        """
        as name
        """
        pass