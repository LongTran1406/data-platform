# read config from config/*.yaml files

import yaml, os

def load_config(env: str = None):
    env = env or os.environ.get("DATAPLATFORM_ENV", "staging")
    path = os.path.join(os.path.dirname(__file__), "..", "..", "config", f"{env}.yaml")
    with open(path, 'r') as f:
        data = yaml.load(f, Loader=yaml.SafeLoader)
    return data

def load_dq_rules():
    path = os.path.join(os.path.dirname(__file__), "..", "..", "config", "dq_rules.yaml")
    with open(path, 'r') as f:
        data = yaml.load(f, Loader=yaml.SafeLoader)
    return data