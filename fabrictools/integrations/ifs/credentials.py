"""Fabric Key Vault helpers for IFS credentials."""

from __future__ import annotations

from typing import Any

from fabrictools.integrations.ifs.config import IFSConfig


def _get_keyvault_secret(keyvault_url: str, secret_name: str) -> str:
    try:
        from notebookutils import mssparkutils  # type: ignore[import-untyped]  # noqa: PLC0415
    except ImportError as exc:
        raise ImportError(
            "notebookutils.mssparkutils is not available — are you running inside "
            "Microsoft Fabric? Use IFSConfig with an explicit client_secret instead."
        ) from exc

    secret = mssparkutils.credentials.getSecret(keyvault_url, secret_name)
    if secret is None or not str(secret).strip():
        raise ValueError(f"Key Vault secret '{secret_name}' is empty or missing")
    return str(secret)


def ifs_config_with_keyvault_secret(
    *,
    host: str,
    client_id: str,
    keyvault_url: str,
    client_secret_name: str,
    **kwargs: Any,
) -> IFSConfig:
    """Build an :class:`IFSConfig` using a client secret stored in Fabric Key Vault.

    :param host: IFS host URL.
    :param client_id: IAM client identifier.
    :param keyvault_url: Azure Key Vault URL linked to the Fabric workspace.
    :param client_secret_name: Secret name holding the IAM client secret.
    :param kwargs: Additional :class:`IFSConfig` fields (``layer``, ``token_endpoint``, etc.).
    """
    client_secret = _get_keyvault_secret(keyvault_url, client_secret_name)
    return IFSConfig(
        host=host,
        client_id=client_id,
        client_secret=client_secret,
        **kwargs,
    )
