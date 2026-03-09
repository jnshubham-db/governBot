"""
GovernanceConfig: built from environment variables only. No hardcoded workspace IDs or catalog names.
"""
import json
import os
from dataclasses import dataclass, field
from typing import Dict, Optional

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


@dataclass
class GovernanceConfig:
    """Configuration for governance catalog, schema, and auth (from env)."""
    catalog: str
    schema: str
    auth_type: str
    kv_scope: str
    kv_client_id_key: str
    kv_client_secret_key: str
    kv_tenant_id_key: str
    kv_client_secret_key2: Optional[str] = None
    account_id: Optional[str] = None
    account_url: Optional[str] = None
    lookback_hours_default: int = 24
    workspace_catalog_overrides: Dict[str, str] = field(default_factory=dict)
    excluded_object_types: Optional[set] = None
    # Workspace IDs that use KV_CLIENT_SECRET_KEY2 for PAT (comma-separated in env)
    workspace_ids_use_pat_key2: set = field(default_factory=set)

    def catalog_for_workspace(self, workspace_id: str) -> str:
        """Return catalog for the given workspace (override from env or default)."""
        return self.workspace_catalog_overrides.get(workspace_id, self.catalog)

    @classmethod
    def from_env(cls) -> "GovernanceConfig":
        """Build config from environment variables only."""
        catalog = os.environ.get("GOVERNANCE_CATALOG", "")
        schema = os.environ.get("GOVERNANCE_SCHEMA", "sch_mng_admon")
        auth_type = os.environ.get("AUTH_TYPE", "azure-client-secret")
        kv_scope = os.environ.get("KV_SCOPE", "")
        kv_client_id_key = os.environ.get("KV_CLIENT_ID_KEY", "")
        kv_client_secret_key = os.environ.get("KV_CLIENT_SECRET_KEY", "")
        kv_tenant_id_key = os.environ.get("KV_TENANT_ID_KEY", "")
        kv_client_secret_key2 = os.environ.get("KV_CLIENT_SECRET_KEY2") or None
        account_id = os.environ.get("ACCOUNT_ID") or None
        account_url = os.environ.get("ACCOUNT_URL") or None
        lookback_str = os.environ.get("LOOKBACK_HOURS_DEFAULT", "24")
        try:
            lookback_hours_default = int(lookback_str)
        except ValueError:
            lookback_hours_default = 24

        overrides_raw = os.environ.get("WORKSPACE_CATALOG_OVERRIDES", "")
        workspace_catalog_overrides: Dict[str, str] = {}
        if overrides_raw.strip():
            try:
                workspace_catalog_overrides = json.loads(overrides_raw)
            except json.JSONDecodeError:
                pass

        excluded_raw = os.environ.get("GOVERNANCE_EXCLUDED_OBJECT_TYPES", "")
        excluded_object_types = None
        if excluded_raw.strip():
            excluded_object_types = {t.strip() for t in excluded_raw.split(",") if t.strip()}

        pat_key2_raw = os.environ.get("WORKSPACE_IDS_USE_PAT_KEY2", "")
        workspace_ids_use_pat_key2 = {w.strip() for w in pat_key2_raw.split(",") if w.strip()}

        return cls(
            catalog=catalog,
            schema=schema,
            auth_type=auth_type,
            kv_scope=kv_scope,
            kv_client_id_key=kv_client_id_key,
            kv_client_secret_key=kv_client_secret_key,
            kv_tenant_id_key=kv_tenant_id_key,
            kv_client_secret_key2=kv_client_secret_key2,
            account_id=account_id,
            account_url=account_url,
            lookback_hours_default=lookback_hours_default,
            workspace_catalog_overrides=workspace_catalog_overrides,
            excluded_object_types=excluded_object_types,
            workspace_ids_use_pat_key2=workspace_ids_use_pat_key2,
        )
