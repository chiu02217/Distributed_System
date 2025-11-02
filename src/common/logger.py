import sys
import os
import threading
from datetime import datetime, timezone

log_lock = threading.Lock()

LOG_PATH_FILE = os.path.join("logs", ".current_log_path")

class Logger:
    def __init__(self, filepath, original_stdout):
        self.terminal = original_stdout
        self.log_file = open(filepath, "a", encoding='utf-8')

    # when print() is called, this function will be triggered.
    def write(self, message):
        with log_lock:
            self.terminal.write(message)
            self.log_file.write(message)
            # flush disk buffer immediately
            self.log_file.flush()

    def flush(self):
        with log_lock:
            self.terminal.flush()
            self.log_file.flush()
            
    def __del__(self):
        if self.log_file:
            self.log_file.close()

def logger(role_name: str, worker_id: str = None):
    log_directory = "logs"
    os.makedirs(log_directory, exist_ok=True)
    
    log_filepath = ""

    if role_name == "coordinator":
        now_utc = datetime.now(timezone.utc)
        timestamp = now_utc.strftime("%y%m%d%H%M%S") 
        
        log_filepath = os.path.join(log_directory, f"system_run_{timestamp}.txt")

        try:
            with open(LOG_PATH_FILE, "w", encoding='utf-8') as f:
                f.write(log_filepath)
        except Exception as e:
            print(f"can not write '{LOG_PATH_FILE}': {e}", file=sys.__stderr__)

            
    elif role_name == "worker":
        try:
            with open(LOG_PATH_FILE, "r", encoding='utf-8') as f:
                log_filepath = f.read().strip()
        except Exception as e:
            print(f"can not read '{LOG_PATH_FILE}': {e}", file=sys.__stderr__)
            if not log_filepath:
                log_filepath = os.path.join(log_directory, f"worker_{worker_id}_fallback.txt")
    
    # 儲存原始的終端機輸出
    original_stdout = sys.stdout
    
    # 攔截print
    sys.stdout = Logger(log_filepath, original_stdout)
    
    print(f"--- Logger start {log_filepath} ---")