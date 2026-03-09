"""
GovernBot constants: table names, violation types, remediation actions, approved action groups.
Single source of truth; no hardcoded values for environment-specific config.
"""
from typing import List

# Governance Delta table base names (no catalog/schema prefix)
TABLE_CONFIG_WORKSPACES = "governance_config_workspaces"
TABLE_PREAPPROVED_OBJECTS = "governance_preapproved_objects"
TABLE_PREAPPROVED_IDENTITIES = "governance_preapproved_identities"
TABLE_VIOLATIONS_STAGING = "governance_violations_staging"
TABLE_CONTROL_ACTIONS = "governance_control_actions"
TABLE_FILTERS = "governance_filters"

GOVERNANCE_TABLES = [
    TABLE_CONFIG_WORKSPACES,
    TABLE_PREAPPROVED_OBJECTS,
    TABLE_PREAPPROVED_IDENTITIES,
    TABLE_VIOLATIONS_STAGING,
    TABLE_CONTROL_ACTIONS,
    TABLE_FILTERS,
]

# Violation types (must match governance_filters.violation_type)
VIOLATION_UNAPPROVED_CREATION = "UNAPPROVED_CREATION"
VIOLATION_UNAUTHORIZED_PERMISSION_CHANGE = "UNAUTHORIZED_PERMISSION_CHANGE"
VIOLATION_UNAUTHORIZED_DELETION = "UNAUTHORIZED_DELETION"
VIOLATION_UNAUTHORIZED_ENTITLEMENT_CHANGE = "UNAUTHORIZED_ENTITLEMENT_CHANGE"
VIOLATION_UNAUTHORIZED_OBJECT_CHANGE = "UNAUTHORIZED_OBJECT_CHANGE"

VIOLATION_TYPES = [
    VIOLATION_UNAPPROVED_CREATION,
    VIOLATION_UNAUTHORIZED_PERMISSION_CHANGE,
    VIOLATION_UNAUTHORIZED_DELETION,
    VIOLATION_UNAUTHORIZED_ENTITLEMENT_CHANGE,
    VIOLATION_UNAUTHORIZED_OBJECT_CHANGE,
]

# Remediation actions (must match governance_filters.remediation_action)
REMEDIATION_DELETE_RESOURCE = "DELETE_RESOURCE"
REMEDIATION_REVERT_PERMISSION = "REVERT_PERMISSION"
REMEDIATION_REPORT_DELETION = "REPORT_DELETION"

REMEDIATION_ACTIONS = [
    REMEDIATION_DELETE_RESOURCE,
    REMEDIATION_REVERT_PERMISSION,
    REMEDIATION_REPORT_DELETION,
]

# Group alias expansions for approved_actions (identity capability).
# Keys must match object_type values used in governance_filters / object_types registry.
APPROVED_ACTION_GROUPS = {
    "ALL": ["ALL"],
    "UC_DATA_OBJECTS": ["catalog", "schema", "table", "volume", "function", "tableConstraint"],
    "UC_SECURITY": ["storageCredential", "externalLocation", "connection"],
    "UC_ALL": [
        "catalog", "schema", "table", "volume", "function", "connection",
        "externalLocation", "storageCredential", "ucRegisteredModel", "ucModelVersion",
        "abacPolicy", "recipient", "share", "provider", "tableConstraint",
    ],
    "COMPUTE": ["cluster", "clusterPolicy", "instancePool", "warehouse"],
    "ML_AI": [
        "mlflowExperiments", "servingEndpoint", "registeredModel", "featureSpec",
        "featureTable", "ucRegisteredModel", "ucModelVersion",
    ],
    "DATA_SHARING": ["share", "recipient", "provider"],
    "DASHBOARDS_BI": ["dashboard", "genieSpace", "alert", "query"],
    "ORCHESTRATION": ["jobs", "pipelines"],
    "SECRETS": ["secretScope"],
    "VECTOR_SEARCH": ["vectorSearchEndpoint", "vectorIndex"],
    "APPS": ["apps"],
    "MONITORING": ["monitors"],
    "CLEAN_ROOMS": ["cleanRoom"],
}


def full_table_name(catalog: str, schema: str, table_base: str) -> str:
    """Return fully qualified table name."""
    return f"{catalog}.{schema}.{table_base}"
