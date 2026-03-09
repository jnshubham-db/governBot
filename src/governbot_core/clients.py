"""
Client factory: WorkspaceClient and AccountClient from GovernanceConfig + get_secret.
No dbutils in package; notebook supplies get_secret.
"""
from typing import Callable

from governbot_core.config import GovernanceConfig

try:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk import AccountClient
except ImportError:
    WorkspaceClient = None  # type: ignore
    AccountClient = None  # type: ignore


class ClientFactory:
    """Create Databricks workspace and account clients using config and secret getter."""

    def __init__(self, config: GovernanceConfig, get_secret: Callable[[str, str], str]):
        self._config = config
        self._get_secret = get_secret

    def create_workspace_client(self, workspace_url: str):
        """Create a WorkspaceClient for the given workspace URL (Azure or PAT from config)."""
        if WorkspaceClient is None:
            raise ImportError("databricks-sdk is required for create_workspace_client")
        c = self._config
        if not c.kv_scope or not workspace_url:
            return WorkspaceClient()
        if c.auth_type == "azure-client-secret":
            client_secret = self._get_secret(c.kv_scope, c.kv_client_secret_key)
            tenant_id = self._get_secret(c.kv_scope, c.kv_tenant_id_key)
            return WorkspaceClient(
                host=workspace_url,
                azure_client_id=c.kv_client_id_key,
                azure_client_secret=client_secret,
                azure_tenant_id=tenant_id,
                auth_type="azure-client-secret",
            )
        if c.auth_type == "pat":
            use_key2 = any(wid in workspace_url for wid in c.workspace_ids_use_pat_key2)
            key = c.kv_client_secret_key2 if (use_key2 and c.kv_client_secret_key2) else c.kv_client_secret_key
            token = self._get_secret(c.kv_scope, key)
            return WorkspaceClient(host=workspace_url, token=token, auth_type="pat")
        return WorkspaceClient()

    def create_account_client(self, account_url: str, account_id: str):
        """Create an AccountClient for the given account URL and ID."""
        if AccountClient is None:
            raise ImportError("databricks-sdk is required for create_account_client")
        c = self._config
        if not c.kv_scope or not account_id or not account_url:
            return AccountClient()
        if c.auth_type == "azure-client-secret":
            client_secret = self._get_secret(c.kv_scope, c.kv_client_secret_key)
            tenant_id = self._get_secret(c.kv_scope, c.kv_tenant_id_key)
            return AccountClient(
                host=account_url,
                account_id=account_id,
                azure_client_id=c.kv_client_id_key,
                azure_client_secret=client_secret,
                azure_tenant_id=tenant_id,
                auth_type="azure-client-secret",
            )
        if c.auth_type == "pat":
            token = self._get_secret(c.kv_scope, c.kv_client_secret_key)
            return AccountClient(host=account_url, account_id=account_id, token=token, auth_type="pat")
        return AccountClient()
