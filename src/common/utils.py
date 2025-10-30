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
    if not os.path.exists(config_path):
        print(f"error: config file not found: {config_path}")
        sys.exit(1)
        
    with open(config_path, 'r') as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            print(f"{config_path} format not correct。")
            sys.exit(1)

_loaded_config_dict = load_config()
CONFIG = _ConfigLoader(config_dict=_loaded_config_dict)