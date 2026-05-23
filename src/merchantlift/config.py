from typing import Any
from pathlib import Path
import yaml

def load_yaml_config(config_path: Path) -> dict[str, Any]:
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    with config_path.open("r", encoding="utf-8") as file:
        config = yaml.safe_load(file)
    
    if config is None:
        raise ValueError(f"Config file is empty: {config_path}")
    return config
