"""
Load governance filters from Delta table and group by violation_type.
"""
from typing import Any, Dict, List

from governbot_core.constants import (
    TABLE_FILTERS,
    full_table_name,
    VIOLATION_UNAPPROVED_CREATION,
    VIOLATION_UNAUTHORIZED_PERMISSION_CHANGE,
    VIOLATION_UNAUTHORIZED_DELETION,
    VIOLATION_UNAUTHORIZED_ENTITLEMENT_CHANGE,
    VIOLATION_UNAUTHORIZED_OBJECT_CHANGE,
)


def load_filters(spark: Any, catalog: str, schema: str) -> Dict[str, List[Any]]:
    """
    Load active filters from governance_filters and return grouped by violation_type.
    Keys: UNAPPROVED_CREATION, UNAUTHORIZED_PERMISSION_CHANGE, UNAUTHORIZED_DELETION,
          UNAUTHORIZED_ENTITLEMENT_CHANGE, UNAUTHORIZED_OBJECT_CHANGE.
    """
    table = full_table_name(catalog, schema, TABLE_FILTERS)
    df = spark.sql(f"""
        SELECT
            filter_name,
            service_name,
            action_name,
            object_type,
            object_id_expr,
            object_name_expr,
            extra_columns,
            violation_type,
            remediation_action
        FROM {table}
        WHERE is_active = true
        ORDER BY filter_name
    """)
    rows = df.collect()
    result: Dict[str, List[Any]] = {
        VIOLATION_UNAPPROVED_CREATION: [],
        VIOLATION_UNAUTHORIZED_PERMISSION_CHANGE: [],
        VIOLATION_UNAUTHORIZED_DELETION: [],
        VIOLATION_UNAUTHORIZED_ENTITLEMENT_CHANGE: [],
        VIOLATION_UNAUTHORIZED_OBJECT_CHANGE: [],
    }
    for row in rows:
        vt = getattr(row, "violation_type", None) or ""
        if vt in result:
            result[vt].append(row)
    return result
