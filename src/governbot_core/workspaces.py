"""
Load enabled workspaces from governance_config_workspaces. Uses constants for table name.
"""
from typing import Any, List

from governbot_core.constants import TABLE_CONFIG_WORKSPACES, full_table_name


def load_enabled_workspaces(spark: Any, catalog: str, schema: str) -> List[Any]:
    """
    Load workspace rows where enforcement_enabled = true.
    Returns list of Row-like objects with workspace_id, workspace_url, workspace_name, etc.
    """
    table = full_table_name(catalog, schema, TABLE_CONFIG_WORKSPACES)
    df = spark.sql(f"""
        SELECT
            workspace_id,
            workspace_url,
            workspace_name,
            warehouse_id,
            enabled_object_types,
            max_retry_attempts
        FROM {table}
        WHERE enforcement_enabled = true
    """)
    return df.collect()
