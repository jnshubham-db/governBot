# Databricks notebook source
# MAGIC %md
# MAGIC # Discover Existing Resources
# MAGIC
# MAGIC Discovers existing resources in workspace(s) and catalogs them in governance_preapproved_objects.
# MAGIC Config from env (GovernanceConfig). Workspace list from governance_config_workspaces or widgets.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

# %pip install /Workspace/GovernBot/src/governbot_core-0.1.0-py3-none-any.whl

# COMMAND ----------

import os
import json
from datetime import datetime

if os.environ.get("DATABRICKS_RUNTIME_VERSION", "").startswith("client."):
    spark.conf.set("spark.sql.session.timeZone", "America/Mexico_City")

try:
    auth_type = dbutils.widgets.get("auth_type")
except Exception:
    auth_type = os.environ.get("AUTH_TYPE", "azure-client-secret")

try:
    enable_discover = dbutils.widgets.get("enable_discover") in ("Y", "S")
except Exception:
    enable_discover = True

try:
    workspace_id = dbutils.widgets.get("workspace_id")
except Exception:
    workspace_id = None
try:
    workspace_url = dbutils.widgets.get("workspace_url")
except Exception:
    workspace_url = None

all_objects = "workspace_objects,query,dashboard,jobs,cluster,pipelines,apps,mlflowExperiments,monitors,alerts,alertsv2,warehouses,clusterPolicies,instancePools,servingEndpoints,registeredModels,secretScopes,vectorSearchEndpoints,catalogs,schemas,tables,volumes,functions,connections,externalLocations,storageCredentials,shares,recipients,providers,cleanRooms,metastores,genieSpaces,ucRegisteredModels,featureTables,groups,tokensAcls,users,servicePrincipals,anyFiles"
try:
    object_types_str = dbutils.widgets.get("object_types")
except Exception:
    object_types_str = all_objects
if object_types_str == "ALL" or not object_types_str:
    object_types_str = all_objects
object_types = [ot.strip() for ot in object_types_str.split(",")]

try:
    use_selective_filter = dbutils.widgets.get("use_selective_filter")
except Exception:
    use_selective_filter = "Y"
try:
    max_threads = int(dbutils.widgets.get("max_threads"))
except Exception:
    max_threads = 20
try:
    debug_permissions = dbutils.widgets.get("debug_permissions")
except Exception:
    debug_permissions = "N"
try:
    account_id = dbutils.widgets.get("account_id")
except Exception:
    account_id = os.environ.get("ACCOUNT_ID", "")

if not enable_discover:
    dbutils.notebook.exit(json.dumps({
        "status": "SKIPPED",
        "reason": "enable_discover not set (Y/S)",
        "timestamp": datetime.utcnow().isoformat(),
    }, indent=2))

# COMMAND ----------

from governbot_core import GovernanceConfig, ClientFactory
from governbot_core.workspaces import load_enabled_workspaces
from governbot_core.constants import TABLE_PREAPPROVED_OBJECTS, full_table_name

config = GovernanceConfig.from_env()
catalog = config.catalog
schema = config.schema
table_name = full_table_name(catalog, schema, TABLE_PREAPPROVED_OBJECTS)
get_secret = lambda s, k: dbutils.secrets.get(scope=s, key=k)
factory = ClientFactory(config, get_secret)
account_url = config.account_url or "https://accounts.azuredatabricks.net/"

# Resolve workspace(s): widget override or from table
workspaces_to_run = []
if workspace_id and workspace_url:
    workspaces_to_run.append({"workspace_id": workspace_id, "workspace_url": workspace_url})
else:
    rows = load_enabled_workspaces(spark, catalog, schema)
    for r in rows:
        workspaces_to_run.append({
            "workspace_id": r.workspace_id,
            "workspace_url": getattr(r, "workspace_url", None) or f"https://adb-{r.workspace_id}.x.azuredatabricks.net/",
        })
if not workspaces_to_run:
    dbutils.notebook.exit(json.dumps({
        "status": "ERROR",
        "reason": "No workspace_id/workspace_url and no enabled workspaces in table",
        "timestamp": datetime.utcnow().isoformat(),
    }, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run discovery (discovery_impl)

# COMMAND ----------

from governbot_core import discovery_impl

all_discovered = []
all_counts = {}
for w in workspaces_to_run:
    wid = w["workspace_id"]
    wurl = w["workspace_url"]
    cat = config.catalog_for_workspace(wid)
    client = factory.create_workspace_client(wurl)
    account_client = None
    if config.account_id and account_id:
        account_client = factory.create_account_client(account_url, account_id or config.account_id)
    discovered, counts = discovery_impl.run_discovery(
        spark, cat, schema, client, wid, object_types,
        account_client=account_client,
        account_id=account_id or config.account_id or "",
        use_selective_filter=use_selective_filter,
        max_threads=max_threads,
        debug_permissions=debug_permissions,
    )
    all_discovered.extend(discovered)
    for k, v in counts.items():
        all_counts[k] = all_counts.get(k, 0) + v

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to table

# COMMAND ----------

if all_discovered:
    from pyspark.sql.types import StructType, StructField, StringType, BooleanType, TimestampType, MapType, ArrayType
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number, desc

    discovered_schema = StructType([
        StructField("object_id", StringType(), True),
        StructField("workspace_id", StringType(), True),
        StructField("object_type", StringType(), True),
        StructField("object_name", StringType(), True),
        StructField("object_path", StringType(), True),
        StructField("owner_email", StringType(), True),
        StructField("permissions", ArrayType(StructType([
            StructField("principal_email", StringType(), True),
            StructField("principal_type", StringType(), True),
            StructField("permission_level", StringType(), True),
        ])), True),
        StructField("metadata", MapType(StringType(), StringType()), True),
        StructField("is_active", BooleanType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
    ])
    df = spark.createDataFrame(all_discovered, schema=discovered_schema)
    window_spec = Window.partitionBy("workspace_id", "object_id").orderBy(desc("updated_at"))
    df_deduped = df.withColumn("row_num", row_number().over(window_spec)).filter("row_num = 1").drop("row_num")
    df_deduped.createOrReplaceTempView("discovered_resources")
    spark.sql(f"""
        MERGE INTO {table_name} AS target
        USING discovered_resources AS source
        ON target.workspace_id = source.workspace_id AND target.object_id = source.object_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"✓ Written {df_deduped.count()} objects to {table_name}")
else:
    print("No objects discovered")

# COMMAND ----------

summary_df = spark.sql(f"""
    SELECT object_type, COUNT(*) AS count, COUNT(DISTINCT owner_email) AS unique_owners
    FROM {table_name}
    WHERE is_active = true
    GROUP BY object_type
    ORDER BY count DESC
""")
display(summary_df)

# COMMAND ----------

dbutils.notebook.exit(json.dumps({
    "status": "SUCCESS",
    "workspaces": [w["workspace_id"] for w in workspaces_to_run],
    "counts": all_counts,
    "total": sum(all_counts.values()),
    "timestamp": datetime.utcnow().isoformat(),
}, indent=2))
