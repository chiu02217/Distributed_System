import json
import os
import sys

# get settings from config file
# no default values , please define all required settings in config file
class _ConfigLoader:
    def __init__(self, config_dict: dict):

        for key, value in config_dict.items():
            if isinstance(value, dict):
                setattr(self, key, _ConfigLoader(value))
            else:
                setattr(self, key, value)



    def load_config(config_path='config/config.json'):
        # 取得目前檔案的絕對路徑
        current_file_path = os.path.abspath(__file__)
        common_dir = os.path.dirname(current_file_path)
        src_dir = os.path.dirname(common_dir)
        project_root = os.path.dirname(src_dir)
        config_path = os.path.join(project_root, 'config', 'config.json')

        if not os.path.exists(config_path):
            print(f"error: cannot find config file: {config_path}")
            sys.exit(1)
            
        with open(config_path, 'r') as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                print(f"error: invalid JSON format in config file: {config_path}")
                sys.exit(1)

_loaded_config_dict = _ConfigLoader.load_config()
CONFIG = _ConfigLoader(config_dict=_loaded_config_dict)