"""IFS Cloud integration for Microsoft Fabric."""

from fabrictools.integrations.ifs.api import ifs_request_json, read_ifs_entity_with_token
from fabrictools.integrations.ifs.client import IFSClient
from fabrictools.integrations.ifs.config import IFSConfig
from fabrictools.integrations.ifs.connectivity import diagnose_ifs_connectivity
from fabrictools.integrations.ifs.credentials import ifs_config_with_keyvault_secret
from fabrictools.integrations.ifs.errors import IFSError
from fabrictools.integrations.ifs.dataframe import ifs_data_to_dataframe, parse_ifs_data
from fabrictools.integrations.ifs.read import read_ifs_entity, read_ifs_to_lakehouse

__all__ = [
    "IFSClient",
    "IFSConfig",
    "IFSError",
    "diagnose_ifs_connectivity",
    "ifs_data_to_dataframe",
    "ifs_config_with_keyvault_secret",
    "parse_ifs_data",
    "ifs_request_json",
    "read_ifs_entity",
    "read_ifs_entity_with_token",
    "read_ifs_to_lakehouse",
]
