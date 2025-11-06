from abc import ABC, abstractmethod

class IAFSClient(ABC):
    """
    AFS Client Interface
    .
    """

    @abstractmethod
    def open_file(self, filename):
        """
        Open a remote file, cache it locally, and return a handle.
        
        Args:
            filename (str): The name of the remote file to open.

        Returns:
            A handle, or None on failure.
        """
        pass

    @abstractmethod
    def write_file(self, handle, data):
        """
        Write a line of data to the local cache file corresponding to the handle.
        
        Args:
            handle: The handle obtained from open_file or create_file.
            data (str or int): The data to write.

        Returns:
            bool: Whether the write was successful.
        """
        pass

    @abstractmethod
    def read_file(self, handle):
        """
        from local cache read a line of data (a number).
        
        Args:
            handle: from open_file get handle.
        
        Returns:
            str: number.
            None: if end of file is reached or line is empty.
        """
        pass

    @abstractmethod
    def create_file(self, filename):
        """
        Create a new file on the remote server and an empty copy in local cache.
        
        Args:
            filename (str): remote file name.

        Returns:
            A handle, or None on failure.
        """
        pass

    @abstractmethod
    def get_local_path(self, handle):
        """
        """
        pass

    @abstractmethod
    def mark_modified(self, handle):
        """
        """
        pass

    @abstractmethod
    def close_file(self, handle):
        """
        Close the handle. If the file is marked as 'modified',
        upload the local cache content back to the server.

        Args:
            handle: The handle to close.
        
        Returns:
            bool: Whether the close/upload was successful.
        """
        pass

    @abstractmethod
    def list_files(self):
        """
        List all files stored on the AFS server.

        Returns:
            list: A list of filenames.
        """
        pass

    @abstractmethod
    def find_latest_coordinator_snapshot(self, input_dir):
        """
        Find the latest coordinator snapshot file in AFS.

        Returns:
            str: The filename of the latest snapshot, or None if not found.
        """
        pass

    @abstractmethod
    def read_json_file(self, handle):
        """
        Read the entire content of a JSON file from AFS.

        Args:
            handle: The handle of the JSON file to read.

        Returns:
            dict: The content of the JSON file, or None if not found.
        """
        pass