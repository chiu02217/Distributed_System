import unittest
import json
import sys
from unittest.mock import patch, mock_open
import importlib

# (同樣，不要在這裡匯入 app_config)

class TestAppConfig(unittest.TestCase):
    
    # 變數來存放 "Patcher" (用來 stop)
    patcher_sys_exit = None
    patcher_path_exists = None
    patcher_file_open = None
    
    # 變數來存放 "Mock" (用來 reset 和 assert)
    mock_sys_exit = None
    mock_path_exists = None
    mock_file_open = None
    
    app_config_module = None 

    @classmethod
    def setUpClass(cls):
        """
        This runs ONCE before any tests.
        """
        
        # 1. 建立 Patcher 並 "start" 它，然後儲存回傳的 "Mock" 物件
        
        cls.patcher_sys_exit = patch('sys.exit')
        cls.mock_sys_exit = cls.patcher_sys_exit.start() # <-- 儲存回傳的 Mock

        cls.patcher_path_exists = patch('os.path.exists', return_value=True)
        cls.mock_path_exists = cls.patcher_path_exists.start() # <-- 儲存回傳的 Mock

        valid_default_json = '{"COORDINATOR_ADDRESS": "default_value"}'
        cls.patcher_file_open = patch('builtins.open', mock_open(read_data=valid_default_json))
        cls.mock_file_open = cls.patcher_file_open.start() # <-- 儲存回傳的 Mock

        # 2. 安全地匯入模組
        try:
            from src.common import utils as app_config
            cls.app_config_module = app_config
        except ImportError as e:
            print(f"--- IMPORT FAILED ---: {e}")
            sys.exit(1)
    
    @classmethod
    def tearDownClass(cls):
        """This runs ONCE after all tests."""
        # 使用 "Patcher" 物件來 "stop"
        cls.patcher_file_open.stop()
        cls.patcher_path_exists.stop()
        cls.patcher_sys_exit.stop()

    def setUp(self):
        """Runs before each individual test."""
        # 現在我們在正確的 "Mock" 物件上呼叫 reset_mock()
        self.mock_sys_exit.reset_mock()
        self.mock_path_exists.return_value = True
        
        valid_json = '{"test": "default"}'
        # 我們需要重設 mock_open 的 "read data"
        self.mock_file_open.return_value = mock_open(read_data=valid_json).return_value

    # --- 您的所有測試函式 (test_01... test_04) 保持不變 ---
    # (貼上您之前的 test_01 到 test_04 函式到這裡)

    def test_01_autoloader_nested_dict(self):
        print("\n--- 測試：自動載入器 (巢狀) ---")
        test_dict = {
            "AFS_SERVER_ADDRESS": "localhost:8000",
            "worker": {
                "WORKER_SNAPSHOT_PORT": 9091
            }
        }
        loader = self.app_config_module._ConfigLoader(test_dict)
        self.assertEqual(loader.AFS_SERVER_ADDRESS, "localhost:8000")
        self.assertEqual(loader.worker.WORKER_SNAPSHOT_PORT, 9091)

    def test_02_load_config_file_not_found(self):
        print("\n--- 測試：載入失敗 (檔案不存在) ---")
        self.mock_path_exists.return_value = False
        
        # 我們必須重新載入模組，才能觸發 load_config
        # 這裡我們不使用 reload()，而是直接呼叫函式
        self.app_config_module.load_config()
        
        self.mock_sys_exit.assert_called_with(1)

    def test_03_load_config_bad_json(self):
        print("\n--- 測試：載入失敗 (JSON 格式錯誤) ---")
        bad_json_data = '{"key": "value", }' # Extra comma
        self.mock_file_open.return_value = mock_open(read_data=bad_json_data).return_value

        self.app_config_module.load_config()
        self.mock_sys_exit.assert_called_with(1)
        
    def test_04_global_settings_object_success(self):
        print("\n--- 測試：全域 settings 物件 (成功) ---")
        
        test_config_str = json.dumps({
            "COORDINATOR_ADDRESS": "mock_coord:1234",
            "worker": { "WORKER_SNAPSHOT_PORT": 9999 }
        })
        self.mock_file_open.return_value = mock_open(read_data=test_config_str).return_value
        
        importlib.reload(self.app_config_module)
        
        self.assertEqual(self.app_config_module.settings.COORDINATOR_ADDRESS, "mock_coord:1234")
        self.assertEqual(self.app_config_module.settings.worker.WORKER_SNAPSHOT_PORT, 9999)

# (if __name__ == '__main__' 保持不變)
if __name__ == '__main__':
    unittest.main()