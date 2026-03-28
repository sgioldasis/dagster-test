"""Configuration resolution utilities for Sling components.

This module provides functions for resolving environment variables in
Sling configuration files and processing replication configs.
"""

import os
import re
from pathlib import Path
from typing import Any

import yaml


def resolve_env_vars(value: Any) -> Any:
    """Resolve environment variables in a value.

    Replaces ${ENV_VAR} or ${ENV_VAR:-default} placeholders with values.

    Args:
        value: Value to resolve (str, dict, list, or other).

    Returns:
        Value with environment variables resolved.
    """
    if isinstance(value, str):
        pattern = r'\$\{([^}]+)\}'

        def replace_env_var(match: re.Match) -> str:
            env_expr = match.group(1)
            if ':-' in env_expr:
                var_name, default = env_expr.split(':-', 1)
                return os.environ.get(var_name, default)
            return os.environ.get(env_expr, '')

        return re.sub(pattern, replace_env_var, value)
    elif isinstance(value, dict):
        return {resolve_env_vars(k): resolve_env_vars(v) for k, v in value.items()}
    elif isinstance(value, list):
        return [resolve_env_vars(item) for item in value]
    return value


def process_replication_config(config_path: Path) -> Path:
    """Process replication config and resolve environment variables.

    Reads the YAML, resolves ${ENV_VAR} in stream keys, and writes
    a processed version if needed.

    Args:
        config_path: Path to the replication YAML file.

    Returns:
        Path to the config file to use (original or processed).
    """
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)

    # Check if any stream keys contain environment variable references
    needs_processing = False
    if 'streams' in config:
        for key in config['streams'].keys():
            if '${' in str(key):
                needs_processing = True
                break

    if not needs_processing:
        return config_path

    # Resolve environment variables in the config
    config = resolve_env_vars(config)

    # Write to a processed config file in the same directory
    processed_path = config_path.parent / f".{config_path.stem}_processed.yaml"
    with open(processed_path, 'w') as f:
        yaml.dump(config, f, default_flow_style=False)

    return processed_path
