import json
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# get settings from config file
# no default values , please define all required settings in config file
class _ConfigLoader:
    def __init__(self, config_dict: dict):
        # recursive loop to scan all the config file {"key","value"}
        for key, value in config_dict.items():
            if isinstance(value, dict):
                setattr(self, key, _ConfigLoader(value))
            else:
                setattr(self, key, value)


    #
    def load_config(config_path='config/config.json'):
        # get file abs route
        config_path = os.path.join(PROJECT_ROOT, 'config', 'config.json')

        if not os.path.exists(config_path):
            print(f"error: cannot find config file: {config_path}")
            sys.exit(1)
            
        with open(config_path, 'r') as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                print(f"invalid JSON format in config file: {config_path}")
                sys.exit(1)

_loaded_config_dict = _ConfigLoader.load_config()
CONFIG = _ConfigLoader(config_dict=_loaded_config_dict)