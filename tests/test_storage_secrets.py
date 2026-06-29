import os

import pytest

from src.storage import load_env_file, resolve_repo_token, token_env_var_for_repo


def test_load_env_file_sets_environment_without_overriding_existing(tmp_path, monkeypatch) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "# comment",
                "HF_TOKEN_ORDERFLOW_ES_001=file-token",
                "QUOTED_TOKEN=\"quoted-token\"",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("HF_TOKEN_ORDERFLOW_ES_001", "existing-token")
    monkeypatch.delenv("QUOTED_TOKEN", raising=False)

    loaded = load_env_file(env_file)

    assert loaded["HF_TOKEN_ORDERFLOW_ES_001"] == "file-token"
    assert os.environ["HF_TOKEN_ORDERFLOW_ES_001"] == "existing-token"
    assert os.environ["QUOTED_TOKEN"] == "quoted-token"


def test_resolve_repo_token_uses_repo_specific_env_var(tmp_path, monkeypatch) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "HF_TOKEN=default-token",
                "HF_TOKEN_ORDERFLOW_ES_002=repo-token",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.delenv("HF_TOKEN", raising=False)
    monkeypatch.delenv("HF_TOKEN_ORDERFLOW_ES_002", raising=False)
    config = {
        "storage": {
            "env_file": str(env_file),
            "default_token_env_var": "HF_TOKEN",
            "repositories": [
                {
                    "repo_id": "other-username/orderflow-es-002",
                    "token_env_var": "HF_TOKEN_ORDERFLOW_ES_002",
                }
            ],
        }
    }

    assert token_env_var_for_repo(config, "other-username/orderflow-es-002") == (
        "HF_TOKEN_ORDERFLOW_ES_002"
    )
    assert resolve_repo_token(config, "other-username/orderflow-es-002") == "repo-token"
    assert resolve_repo_token(config, "unknown/repo") == "default-token"


def test_resolve_repo_token_raises_when_missing(tmp_path, monkeypatch) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("HF_TOKEN_ORDERFLOW_ES_001=\n", encoding="utf-8")
    monkeypatch.delenv("HF_TOKEN_ORDERFLOW_ES_001", raising=False)
    config = {
        "storage": {
            "env_file": str(env_file),
            "repositories": [
                {
                    "repo_id": "your-username/orderflow-es-001",
                    "token_env_var": "HF_TOKEN_ORDERFLOW_ES_001",
                }
            ],
        }
    }

    with pytest.raises(RuntimeError, match="HF_TOKEN_ORDERFLOW_ES_001"):
        resolve_repo_token(config, "your-username/orderflow-es-001")
