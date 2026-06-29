"""Load local secrets and resolve per-repository Hugging Face tokens."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Mapping


def load_env_file(path: str | Path = ".env", override: bool = False) -> dict[str, str]:
    """Load a simple KEY=VALUE .env file into os.environ."""
    env_path = Path(path)

    if not env_path.exists():
        return {}

    loaded: dict[str, str] = {}

    for line_number, raw_line in enumerate(
        env_path.read_text(encoding="utf-8").splitlines(),
        start=1,
    ):
        line = raw_line.strip()

        if not line or line.startswith("#"):
            continue

        if line.startswith("export "):
            line = line[len("export ") :].strip()

        if "=" not in line:
            raise ValueError(f"Invalid .env line {line_number}: {raw_line}")

        key, raw_value = line.split("=", 1)
        key = key.strip()
        value = _strip_quotes(raw_value.strip())

        if not key:
            raise ValueError(f"Invalid .env line {line_number}: {raw_line}")

        loaded[key] = value

        if override or key not in os.environ:
            os.environ[key] = value

    return loaded


def resolve_repo_token(
    config: Mapping[str, Any],
    repo_id: str,
    token_env_var: str | None = None,
    env_file: str | Path | None = None,
    required: bool = True,
) -> str | None:
    """Resolve the Hugging Face token for a repo from env vars or .env."""
    storage = config.get("storage", {})
    selected_env_file = env_file or storage.get("env_file")

    if selected_env_file:
        load_env_file(selected_env_file)

    selected_token_env_var = token_env_var or token_env_var_for_repo(config, repo_id)

    if selected_token_env_var is None:
        if required:
            raise RuntimeError(f"No token_env_var configured for repo: {repo_id}")

        return None

    token = os.environ.get(selected_token_env_var)

    if token:
        return token

    if required:
        raise RuntimeError(
            f"Missing Hugging Face token for repo {repo_id}. "
            f"Set {selected_token_env_var} in your .env file or environment."
        )

    return None


def token_env_var_for_repo(config: Mapping[str, Any], repo_id: str) -> str | None:
    """Return the env var name that stores the token for a Hugging Face repo."""
    storage = config.get("storage", {})

    for repo in storage.get("repositories", []):
        if repo.get("repo_id") == repo_id and repo.get("token_env_var"):
            return str(repo["token_env_var"])

    default_token_env_var = storage.get("default_token_env_var")

    if default_token_env_var:
        return str(default_token_env_var)

    return None


def _strip_quotes(value: str) -> str:
    if len(value) < 2:
        return value

    if value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]

    return value
