"""IFS Cloud integration for Microsoft Fabric."""

from fabrictools.integrations.ifs.client import IFSClient
from fabrictools.integrations.ifs.config import IFSConfig
from fabrictools.integrations.ifs.credentials import ifs_config_with_keyvault_secret
from fabrictools.integrations.ifs.errors import IFSError
from fabrictools.integrations.ifs.read import read_ifs_entity, read_ifs_to_lakehouse

__all__ = [
    "IFSClient",
    "IFSConfig",
    "IFSError",
    "ifs_config_with_keyvault_secret",
    "read_ifs_entity",
    "read_ifs_to_lakehouse",
]
