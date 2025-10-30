import unittest
import grpc
import threading
import time
import os
import sys

# --- 關鍵的路徑設定 ---
# 1. 取得此檔案 (test_worker_server_startup.py) 的目錄
#    (e.g., C:\Users\p1382\Desktop\DS\tests)
test_dir = os.path.dirname(os.path.abspath(__file__))

# 2. 取得專案的根目錄 (e.g., C:\Users\p1382\Desktop\DS)
project_root = os.path.dirname(test_dir)

# 3. 將根目錄加入到 Python 的 sys.path 中
#    這樣我們才能 import "src.common.utils"
sys.path.append(project_root)

try:
    from src.app.worker.worker_server.worker_server import run_worker_server
    from src.common.utils import CONFIG
except ImportError as e:
    print(f"--- ImportError ---")

    sys.exit(1)


class TestWorkerServerStartup(unittest.TestCase):
    def dummy_snapshot_callback_for_test(self, snapshot_id):
        print(f"[TestCallback] receive snapshot: {snapshot_id}")

    def test_worker_server(self):
        server = None
        server_exception = None

        def start_server_in_thread():
            nonlocal server, server_exception
            try:

                server = run_worker_server(
                    trigger_snapshot_callback=self.dummy_snapshot_callback_for_test
                )
                print("[Worker Server] started...")
            except Exception as e:
                print(f"[Worker Server] starting failed: {e}")
                server_exception = e

        server_thread = threading.Thread(target=start_server_in_thread, daemon=True)
        server_thread.start()
        print("[MainThread] waiting for startup...")
        time.sleep(1)
        # Assertions
        self.assertIsNone(server_exception, f"exception: {server_exception}")
        self.assertIsNotNone(server, "server startup not successful")
        self.assertIsInstance(server, grpc.Server, "object is not a gRPC server")

        print("[MainThread] server started successfully.")

        print("[MainThread] sending stop signal...")
        server.stop(0)
        
        # wait for the server thread to finish
        server_thread.join(timeout=2)

        self.assertFalse(server_thread.is_alive(), "server thread did not shut down properly")
        print("[MainThread] server stopped successfully. Test passed.")


if __name__ == '__main__':
    unittest.main()