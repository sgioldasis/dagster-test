"""Configuration resolution utilities for Sling components.

This module provides functions for resolving environment variables in
Sling configuration files and processing replication configs.
"""

import os
import re
from pathlib import Path
from typing import Any

import yaml


# Jinja2-style env variable pattern: {{ env.VAR }} or {{ env('VAR') }} or {{ env('VAR', 'default') }}
JINJA_ENV_PATTERN = re.compile(
    r'\{\{\s*env\s*(?:\.(\w+)|\(\s*[\'\"](\w+)[\'\"](?:\s*,\s*[\'\"]([^\'\"]*)[\'\"])?\s*\))\s*\}\}'
)
# Shell-style env variable pattern: ${ENV_VAR} or ${ENV_VAR:-default}
SHELL_ENV_PATTERN = re.compile(r'\$\{([^}]+)\}')


def resolve_env_vars(value: Any) -> Any:
    """Resolve environment variables in a value.

    Supports:
    - Jinja2-style: {{ env.VAR }}, {{ env('VAR') }}, {{ env('VAR', 'default') }}
    - Shell-style: ${ENV_VAR}, ${ENV_VAR:-default}
    - Legacy env: prefix: env:VAR (for backward compatibility)

    Args:
        value: Value to resolve (str, dict, list, or other).

    Returns:
        Value with environment variables resolved.
    """
    if isinstance(value, str):
        # Handle legacy env: prefix (for backward compatibility)
        if value.startswith("env:"):
            return os.environ.get(value[4:], "")

        # Handle Jinja2-style {{ env.VAR }} or {{ env('VAR', 'default') }}
        def replace_jinja_env(match: re.Match) -> str:
            # Group 1: env.VAR syntax (dot notation)
            # Group 2: env('VAR') syntax - variable name
            # Group 3: env('VAR', 'default') syntax - default value
            var_name = match.group(1) or match.group(2)
            default = match.group(3) or ""
            if var_name:
                return os.environ.get(var_name, default)
            return match.group(0)  # Return original if no match

        result = JINJA_ENV_PATTERN.sub(replace_jinja_env, value)

        # Handle shell-style ${ENV_VAR} or ${ENV_VAR:-default}
        def replace_shell_env(match: re.Match) -> str:
            env_expr = match.group(1)
            if ':-' in env_expr:
                var_name, default = env_expr.split(':-', 1)
                return os.environ.get(var_name, default)
            return os.environ.get(env_expr, '')

        result = SHELL_ENV_PATTERN.sub(replace_shell_env, result)
        return result

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
