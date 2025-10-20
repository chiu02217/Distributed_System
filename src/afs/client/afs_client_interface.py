from abc import ABC, abstractmethod

class IAFSCClient(ABC):
    """
    AFSClient 的抽象介面 (Interface)。

    它定義了 AFS 客戶端必須提供的所有公開功能，
    讓 Worker 和 Coordinator 可以依賴此介面，
    而不是依賴具體的實作 (implementation)。
    """

    @abstractmethod
    def open_file(self, filename):
        """
        開啟一個遠端檔案，將其快取到本地，並回傳一個 handle (控制代碼)。
        
        Args:
            filename (str): 要開啟的遠端檔案名稱。
        
        Returns:
            一個 handle，或在失敗時回傳 None。
        """
        pass

    @abstractmethod
    def write_file(self, handle, data):
        """
        將一行資料寫入 handle 所對應的本地快取檔案。
        
        Args:
            handle: 從 open_file 或 create_file 取得的 handle。
            data (str or int): 要寫入的資料。
        
        Returns:
            bool: 寫入是否成功。
        """
        pass

    @abstractmethod
    def read_file(self, handle):
        """
        從 handle 所對應的本地快取檔案讀取一行資料（一個數字）。
        
        Args:
            handle: 從 open_file 取得的 handle。
        
        Returns:
            str: 讀取到的數字字串。
            None: 如果已達檔案結尾或該行為空。
        """
        pass

    @abstractmethod
    def create_file(self, filename):
        """
        在遠端伺服器上建立一個新檔案，並在本地快取中建立一個空的副本。
        回傳一個 handle。
        
        Args:
            filename (str): 要建立的遠端檔案名稱。
        
        Returns:
            一個 handle，或在失敗時回傳 None。
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
        關閉 handle。如果檔案被標記為 'modified' (已修改)，
        則將本地快取內容上傳回伺服器。
        
        Args:
            handle: 要關閉的 handle。
        
        Returns:
            bool: 關閉/上傳是否成功。
        """
        pass