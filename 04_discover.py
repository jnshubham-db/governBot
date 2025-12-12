# Databricks notebook source
# MAGIC %md
# MAGIC # Discover Existing Resources
# MAGIC
# MAGIC This notebook discovers all existing resources in a workspace and catalogs them
# MAGIC in the governance_preapproved_objects table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog", "sjdatabricks", "Catalog Name")
dbutils.widgets.text("schema", "governance", "Schema Name")
dbutils.widgets.text("workspace_id", "3592773542550038", "Workspace ID (required)")
dbutils.widgets.text("workspace_url", "https://adb-3592773542550038.18.azuredatabricks.net/", "Workspace URL (required)")
dbutils.widgets.multiselect("object_types", "workspace_objects", 
                           ["workspace_objects", "query", "dashboard", "jobs", "cluster", "pipelines", "apps", 
                            "mlflowExperiments", "monitors", "alerts", "warehouses", "clusterPolicies", 
                            "instancePools", "servingEndpoints", "registeredModels", "secretScopes",
                            "vectorSearchEndpoints", "vectorIndexes", "catalogs", "schemas", "tables", "volumes", 
                            "functions", "connections", "externalLocations", "storageCredentials", "shares", 
                            "recipients", "providers", "cleanRooms", "metastores", "genieSpaces", 
                            "ucRegisteredModels", "featureTables"],
                           "Object Types to Discover")
dbutils.widgets.dropdown("use_selective_filter", "Y", ["Y", "N"], "Use Selective Filtering")
dbutils.widgets.text("max_threads", "10", "Max Threads for Workspace Discovery")
dbutils.widgets.dropdown("debug_permissions", "N", ["Y", "N"], "Debug Permission Fetching")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
workspace_id = dbutils.widgets.get("workspace_id")
workspace_url = dbutils.widgets.get("workspace_url")
object_types_str = dbutils.widgets.get("object_types")
object_types = [ot.strip() for ot in object_types_str.split(",")]
use_selective_filter = dbutils.widgets.get("use_selective_filter")
max_threads = int(dbutils.widgets.get("max_threads"))
debug_permissions = dbutils.widgets.get("debug_permissions")

if not workspace_id or not workspace_url:
    raise ValueError("workspace_id and workspace_url are required")

print(f"Catalog: {catalog}")
print(f"Schema: {schema}")
print(f"Workspace ID: {workspace_id}")
print(f"Object Types: {', '.join(object_types)}")
print(f"Use Selective Filter: {use_selective_filter}")
print(f"Max Threads: {max_threads}")
print(f"Debug Permissions: {debug_permissions}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Databricks SDK Client

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from datetime import datetime
from typing import List, Dict, Any
from pyspark.sql.types import *
from pyspark.sql import Row
from concurrent.futures import ThreadPoolExecutor, as_completed
import json

# Initialize workspace client
client = WorkspaceClient()
print("✓ Workspace client initialized")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Discovery Functions

# COMMAND ----------

import re

def _detect_principal_type(principal: str) -> str:
    """
    Detect principal type based on the principal identifier.
    
    Rules:
    - Users: contain '@' in email format (e.g., user@company.com)
    - Service Principals: follow UUID/GUID pattern (e.g., d118594b-a1db-41b2-a6e2-a201377c2aec)
    - Groups: everything else (e.g., "admins", "data_engineers", "users")
    
    Returns: 'user', 'service_principal', or 'group'
    """
    if not principal:
        return 'user'
    
    # UUID/GUID pattern for service principals
    # Format: 8-4-4-4-12 hexadecimal characters
    uuid_pattern = r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
    
    if re.match(uuid_pattern, principal):
        # Service principal (UUID format)
        return 'service_principal'
    elif '@' in principal:
        # User (email format)
        return 'user'
    else:
        # Group (e.g., "admins", "data_engineers")
        return 'group'


def get_permissions_safe(client, object_type: str, object_id: str, debug_sample: bool = False) -> tuple:
    """
    Get permissions for an object, returning (owner_email, permissions_list).
    Returns ('unknown', []) on error.
    """
    try:
        permissions = client.permissions.get(object_type, object_id)
        acl_list = []
        owner_email = 'unknown'
        
        # Debug: Print first few permission responses
        if debug_sample:
            print(f"    DEBUG: Fetching permissions for {object_type}/{object_id}")
            print(f"    DEBUG: Response has ACL: {permissions.access_control_list is not None}")
            if permissions.access_control_list:
                print(f"    DEBUG: ACL length: {len(permissions.access_control_list)}")
        
        if permissions.access_control_list:
            for acl in permissions.access_control_list:
                principal_email = None
                if acl.user_name:
                    principal_email = acl.user_name
                elif acl.service_principal_name:
                    principal_email = acl.service_principal_name
                elif acl.group_name:
                    # Also capture group permissions
                    principal_email = acl.group_name
                
                # Determine principal type for more accurate remediation
                principal_type = 'user'
                if acl.user_name:
                    principal_type = 'user'
                elif acl.service_principal_name:
                    principal_type = 'service_principal'
                elif acl.group_name:
                    principal_type = 'group'
                
                if principal_email and acl.all_permissions:
                    for perm in acl.all_permissions:
                        # Debug: Show what we're seeing
                        if debug_sample:
                            inherited_str = "inherited" if perm.inherited else "direct"
                            print(f"      - {principal_email} ({principal_type}): {perm.permission_level.value} ({inherited_str})")
                        
                        # Check if this principal is the owner (has CAN_MANAGE, prefer non-inherited)
                        if perm.permission_level.value == 'CAN_MANAGE':
                            if not perm.inherited:
                                owner_email = principal_email
                            elif owner_email == 'unknown':
                                # Fallback to inherited owner if no direct owner found
                                owner_email = principal_email
                        
                        # Include ALL permissions (both inherited and direct)
                        # This matches the behavior of the original sample.py
                        # Now also includes principal_type for more accurate remediation
                        acl_list.append(Row(
                            principal_email=principal_email,
                            principal_type=principal_type,
                            permission_level=perm.permission_level.value
                        ))
        
        if debug_sample:
            print(f"    DEBUG: Extracted {len(acl_list)} permissions, owner: {owner_email}")
        
        return (owner_email, acl_list)
    except Exception as e:
        # More detailed error logging
        print(f"  ⚠️  Permission fetch failed for {object_type}/{object_id}: {str(e)}")
        return ('unknown', [])

# COMMAND ----------

def get_uc_grants_safe(client, securable_type: str, full_name: str) -> tuple:
    """
    Get Unity Catalog grants for a securable object using grants.get_effective API.
    Returns (owner_email, permissions_list).
    
    Args:
        client: WorkspaceClient instance
        securable_type: Type of securable (e.g., 'CATALOG', 'SCHEMA', 'TABLE', 'VOLUME', 'FUNCTION', 'REGISTERED_MODEL')
        full_name: Full name of the object (e.g., 'catalog.schema.table')
    
    Returns:
        Tuple of (owner_email, list of permission Rows)
    """
    try:
        from databricks.sdk.service.catalog import SecurableType
        
        # Map string to SecurableType enum
        securable_type_map = {
            'CATALOG': SecurableType.CATALOG,
            'SCHEMA': SecurableType.SCHEMA,
            'TABLE': SecurableType.TABLE,
            'VOLUME': SecurableType.VOLUME,
            'FUNCTION': SecurableType.FUNCTION,
            'REGISTERED_MODEL': SecurableType.FUNCTION,  # Models use FUNCTION type
            'EXTERNAL_LOCATION': SecurableType.EXTERNAL_LOCATION,
            'STORAGE_CREDENTIAL': SecurableType.STORAGE_CREDENTIAL,
            'CONNECTION': SecurableType.CONNECTION,
            'SHARE': SecurableType.SHARE,
            'RECIPIENT': SecurableType.RECIPIENT,
            'PROVIDER': SecurableType.PROVIDER,
            'METASTORE': SecurableType.METASTORE,
        }
        
        sec_type = securable_type_map.get(securable_type.upper())
        if not sec_type:
            return ('unknown', [])
        
        grants = client.grants.get_effective(securable_type=sec_type, full_name=full_name)
        
        acl_list = []
        owner_email = 'unknown'
        
        if grants and grants.privilege_assignments:
            for assignment in grants.privilege_assignments:
                principal = assignment.principal if hasattr(assignment, 'principal') else None
                if principal and assignment.privileges:
                    # Determine principal type based on naming conventions
                    # UC grants don't explicitly tell us if it's user/group/sp
                    principal_type = _detect_principal_type(principal)
                    
                    for privilege in assignment.privileges:
                        priv_name = privilege.privilege.value if hasattr(privilege.privilege, 'value') else str(privilege.privilege)
                        
                        # Check for ownership
                        if priv_name in ['ALL_PRIVILEGES', 'OWNER']:
                            owner_email = principal
                        
                        acl_list.append(Row(
                            principal_email=principal,
                            principal_type=principal_type,
                            permission_level=priv_name
                        ))
        
        return (owner_email, acl_list)
    except Exception as e:
        # Silently fail for permission errors
        return ('unknown', [])

# COMMAND ----------

def get_secret_acls_safe(client, scope_name: str) -> tuple:
    """
    Get secret scope ACLs using secrets.list_acls API.
    Returns (owner_email, permissions_list).
    """
    try:
        acl_list = []
        owner_email = 'unknown'
        
        for acl in client.secrets.list_acls(scope=scope_name):
            principal = acl.principal if hasattr(acl, 'principal') else None
            permission = acl.permission.value if hasattr(acl, 'permission') and hasattr(acl.permission, 'value') else str(acl.permission)
            
            if principal:
                # Determine principal type
                principal_type = _detect_principal_type(principal)
                
                # MANAGE permission indicates owner
                if permission == 'MANAGE':
                    owner_email = principal
                
                acl_list.append(Row(
                    principal_email=principal,
                    principal_type=principal_type,
                    permission_level=permission
                ))
        
        return (owner_email, acl_list)
    except Exception as e:
        return ('unknown', [])

# COMMAND ----------

def discover_workspace_objects(client, workspace_id: str, use_selective_filter: str = "Y", max_threads: int = 10, debug_mode: bool = False) -> List[Dict[str, Any]]:
    """
    Discover all workspace objects (notebooks, directories, repos, files) with intelligent filtering.
    
    Filtering logic:
    - /Workspace/Repos/: Get permissions for directories only, list files without permissions
    - /Workspace/Users/: Get all files/folders with permissions
    - /Workspace/* (root): Get all files/folders with permissions
    - Skip .git paths and excluded object types
    
    Args:
        client: WorkspaceClient instance
        workspace_id: Workspace ID
        use_selective_filter: "Y" or "N" to enable selective filtering
        max_threads: Maximum number of threads for parallel processing
        debug_mode: If True, print detailed debug information for permission fetching
    """
    print("Discovering workspace objects...")
    print(f"  Selective filtering: {use_selective_filter}")
    print(f"  Max threads: {max_threads}")
    print(f"  Debug mode: {debug_mode}")
    
    discovered = []
    discovered_lock = __import__('threading').Lock()
    
    # Statistics counters
    stats = {
        'total_processed': 0,
        'permissions_fetched': 0,
        'permissions_skipped': 0,
        'permissions_success': 0,
        'permissions_failed': 0
    }
    stats_lock = __import__('threading').Lock()
    
    # Excluded object types (these are handled by other discovery functions)
    exclusions = ["QUERY", "ALERT", "GENIE", "DASHBOARD", "ALERTV2"]
    
    def should_process_object(obj_path: str, obj_type: str) -> tuple:
        """
        Determine if object should be processed and if permissions should be fetched.
        Returns: (should_process, fetch_permissions)
        """
        # Skip .git related paths
        if '.git' in obj_path.split("/"):
            return (False, False)
        
        # Skip excluded object types
        if obj_type in exclusions:
            return (False, False)
        
        if use_selective_filter == "Y":
            # Apply path-specific rules for selective filtering
            if obj_path.startswith('/Workspace/Repos/') or obj_path == '/Workspace/Repos':
                # Repos: Process all objects, but only get permissions for directories/repos
                if obj_type in ['DIRECTORY', 'REPO']:
                    return (True, True)
                else:
                    # List files without permissions
                    return (True, False)
            elif obj_path.startswith('/Workspace/Users/') or obj_path == '/Workspace/Users':
                # Users: All objects with permissions
                return (True, True)
            else:
                # Root Workspace path (excluding Repos and Users subdirectories)
                path_parts = obj_path.split('/')
                if len(path_parts) > 2 and path_parts[1] == 'Workspace':
                    if path_parts[2] not in ['Repos', 'Users', '']:
                        # Workspace root: All objects with permissions
                        return (True, True)
                elif len(path_parts) == 2 and path_parts[1] == 'Workspace':
                    # Root /Workspace/ itself
                    return (True, True)
                
                return (False, False)
        else:
            # No selective filtering - process all objects with permissions
            return (True, True)
    
    def get_object_type_name(obj_type_value: str) -> str:
        """Convert SDK object type to our naming convention."""
        type_mapping = {
            'NOTEBOOK': 'notebook',
            'DIRECTORY': 'directory',
            'REPO': 'repo',
            'FILE': 'file',
            'LIBRARY': 'library'
        }
        return type_mapping.get(obj_type_value, obj_type_value.lower())
    
    def get_permissions_type(obj_type_value: str) -> str:
        """Get the permission type string for API calls."""
        type_mapping = {
            'NOTEBOOK': 'notebooks',
            'DIRECTORY': 'directories',
            'REPO': 'repos',
            'FILE': 'files',
            'LIBRARY': 'libraries'
        }
        return type_mapping.get(obj_type_value, obj_type_value.lower())
    
    def process_workspace_object(obj):
        """Process a single workspace object."""
        try:
            if not obj.object_type:
                return
            
            obj_type_value = obj.object_type.value
            obj_path = obj.path or 'unknown'
            obj_id = str(obj.object_id) if obj.object_id else 'unknown'
            
            should_process, fetch_permissions = should_process_object(obj_path, obj_type_value)
            
            if not should_process:
                return
            
            with stats_lock:
                stats['total_processed'] += 1
            
            # Get permissions and owner if needed
            permissions = []
            owner_email = 'unknown'
            if fetch_permissions:
                with stats_lock:
                    stats['permissions_fetched'] += 1
                
                perm_type = get_permissions_type(obj_type_value)
                owner_email, permissions = get_permissions_safe(client, perm_type, obj_id, debug_mode)
                with stats_lock:
                    if permissions:
                        stats['permissions_success'] += 1
                    else:
                        stats['permissions_failed'] += 1
            else:
                with stats_lock:
                    stats['permissions_skipped'] += 1
            
            # Build metadata
            metadata = {}
            if hasattr(obj, 'created_at') and obj.created_at:
                metadata['created_at'] = str(obj.created_at)
            if hasattr(obj, 'modified_at') and obj.modified_at:
                metadata['modified_at'] = str(obj.modified_at)
            if hasattr(obj, 'language') and obj.language:
                metadata['language'] = str(obj.language)
            
            # Create discovered object
            discovered_obj = {
                'object_id': obj_id,
                'workspace_id': workspace_id,
                'object_type': get_object_type_name(obj_type_value),
                'object_name': obj_path.split('/')[-1] if obj_path and obj_path != '/' else 'unknown',
                'object_path': obj_path,
                'owner_email': owner_email,  # Extracted from permissions (CAN_MANAGE)
                'permissions': permissions,
                'metadata': metadata,
                'is_active': True,
                'created_at': datetime.utcnow(),
                'updated_at': datetime.utcnow()
            }
            
            with discovered_lock:
                discovered.append(discovered_obj)
        
        except Exception as e:
            print(f"  Warning: Error processing object {obj.path if hasattr(obj, 'path') else 'unknown'}: {str(e)}")
    
    # Use a queue-based approach to avoid thread pool recursion
    from queue import Queue
    import threading
    
    path_queue = Queue()
    queue_lock = threading.Lock()
    active_workers = [0]  # Use list for mutable reference in closure
    
    def worker():
        """Worker thread that processes paths from the queue."""
        while True:
            try:
                path = path_queue.get(timeout=1.0)
            except:
                # Check if we should exit (no more work and no active workers)
                with queue_lock:
                    if path_queue.empty() and active_workers[0] <= 1:
                        active_workers[0] -= 1
                        return
                continue
            
            try:
                with queue_lock:
                    active_workers[0] += 1
                
                objects = list(client.workspace.list(path, recursive=False))
                
                # Process objects
                for obj in objects:
                    process_workspace_object(obj)
                
                # Add directories to queue for further processing
                directories = [obj for obj in objects if obj.object_type and obj.object_type.value in ['DIRECTORY', 'REPO']]
                for dir_obj in directories:
                    if dir_obj.path:
                        path_queue.put(dir_obj.path)
                
            except Exception as e:
                print(f"  Warning: Error listing path {path}: {str(e)}")
            finally:
                with queue_lock:
                    active_workers[0] -= 1
                path_queue.task_done()
    
    try:
        # Start paths to traverse
        start_paths = ["/Workspace/", "/Workspace/Repos/", "/Workspace/Users/"]
        
        if use_selective_filter == "Y":
            print("  Using SELECTIVE FILTERING mode:")
            print("    - /Workspace/Repos/: Permissions for directories only, list all files")
            print("    - /Workspace/Users/: All files/folders with permissions")
            print("    - /Workspace/*: All files/folders with permissions")
        else:
            print("  Using STANDARD mode: All objects with permissions")
        
        # Add initial paths to queue
        for path in start_paths:
            path_queue.put(path)
        
        # Start worker threads
        workers = []
        for _ in range(max_threads):
            t = threading.Thread(target=worker, daemon=True)
            t.start()
            workers.append(t)
        
        # Wait for all paths to be processed
        path_queue.join()
        
        # Wait for workers to finish
        for t in workers:
            t.join(timeout=2.0)
        
        print(f"✓ Discovered {len(discovered)} workspace objects")
        
        # Print breakdown by type
        type_counts = {}
        for obj in discovered:
            obj_type = obj['object_type']
            type_counts[obj_type] = type_counts.get(obj_type, 0) + 1
        
        for obj_type, count in sorted(type_counts.items()):
            print(f"    {obj_type}: {count}")
        
        # Print statistics
        print(f"\n  Permission Statistics:")
        print(f"    Total objects processed: {stats['total_processed']}")
        print(f"    Permissions fetched: {stats['permissions_fetched']}")
        print(f"    Permissions skipped: {stats['permissions_skipped']}")
        print(f"    Permissions successful: {stats['permissions_success']}")
        print(f"    Permissions failed: {stats['permissions_failed']}")
    
    except Exception as e:
        print(f"✗ Error discovering workspace objects: {str(e)}")
    
    return discovered

# COMMAND ----------

def discover_queries(client, workspace_id: str) -> List[Dict[str, Any]]:
    """Discover all SQL queries."""
    print("Discovering queries...")
    discovered = []
    
    try:
        for query in client.queries.list():
            owner_email, permissions = get_permissions_safe(client, "queries", query.id)
            # Prefer the owner from query.user if available
            if query.user and query.user.email:
                owner_email = query.user.email
            
            discovered.append({
                'object_id': query.id,
                'workspace_id': workspace_id,
                'object_type': 'query',
                'object_name': query.name or 'Untitled Query',
                'object_path': None,
                'owner_email': owner_email,
                'permissions': permissions,
                'metadata': {'description': query.description or '', 'created_at': str(query.created_at)},
                'is_active': True,
                'created_at': datetime.utcnow(),
                'updated_at': datetime.utcnow()
            })
        
        print(f"✓ Discovered {len(discovered)} queries")
    except Exception as e:
        print(f"✗ Error discovering queries: {str(e)}")
    
    return discovered

# COMMAND ----------

def discover_dashboards(client, workspace_id: str) -> List[Dict[str, Any]]:
    """Discover all dashboards (both Lakeview/AI-BI and legacy SQL dashboards)."""
    print("Discovering dashboards...")
    discovered = []
    
    # Discover Lakeview (AI/BI) dashboards
    try:
        lakeview_count = 0
        for dashboard in client.lakeview.list():
            # Lakeview dashboards don't support standard permissions API
            # Extract owner from dashboard metadata
            owner_email = 'unknown'
            if hasattr(dashboard, 'creator_user_name') and dashboard.creator_user_name:
                owner_email = dashboard.creator_user_name
            
            discovered.append({
                'object_id': dashboard.dashboard_id,
                'workspace_id': workspace_id,
                'object_type': 'lakeview_dashboard',
                'object_name': dashboard.display_name or 'Untitled Dashboard',
                'object_path': dashboard.path if hasattr(dashboard, 'path') else None,
                'owner_email': owner_email,
                'permissions': [],  # Lakeview dashboards use workspace object permissions
                'metadata': {
                    'lifecycle_state': dashboard.lifecycle_state.value if hasattr(dashboard, 'lifecycle_state') and dashboard.lifecycle_state else 'UNKNOWN',
                    'create_time': str(dashboard.create_time) if hasattr(dashboard, 'create_time') else ''
                },
                'is_active': True,
                'created_at': datetime.utcnow(),
                'updated_at': datetime.utcnow()
            })
            lakeview_count += 1
        
        print(f"  ✓ Discovered {lakeview_count} Lakeview (AI/BI) dashboards")
    except AttributeError:
        print(f"  ⚠ Lakeview API not available in SDK - skipping Lakeview dashboards")
    except Exception as e:
        print(f"  ⚠ Error discovering Lakeview dashboards: {str(e)}")
    
    # Discover legacy SQL dashboards using the legacy API
    try:
        legacy_count = 0
        # The legacy dashboards API uses sql/dashboards for permissions
        for dashboard in client.dashboards.list():
            # Use sql/dashboards for permission type (legacy SQL dashboards)
            owner_email, permissions = get_permissions_safe(client, "sql/dashboards", dashboard.id)
            # Prefer the owner from dashboard.user if available
            if hasattr(dashboard, 'user') and dashboard.user and hasattr(dashboard.user, 'email'):
                owner_email = dashboard.user.email
            
            discovered.append({
                'object_id': dashboard.id,
                'workspace_id': workspace_id,
                'object_type': 'dashboard',
                'object_name': dashboard.name or 'Untitled Dashboard',
                'object_path': None,
                'owner_email': owner_email,
                'permissions': permissions,
                'metadata': {'created_at': str(dashboard.created_at) if hasattr(dashboard, 'created_at') else ''},
                'is_active': True,
                'created_at': datetime.utcnow(),
                'updated_at': datetime.utcnow()
            })
            legacy_count += 1
        
        print(f"  ✓ Discovered {legacy_count} legacy SQL dashboards")
    except Exception as e:
        print(f"  ⚠ Error discovering legacy SQL dashboards: {str(e)}")
    
    print(f"✓ Discovered {len(discovered)} total dashboards")
    return discovered

# COMMAND ----------

def discover_jobs(client, workspace_id: str) -> List[Dict[str, Any]]:
    """Discover all jobs."""
    print("Discovering jobs...")
    discovered = []
    
    try:
        for job in client.jobs.list():
            owner_email, permissions = get_permissions_safe(client, "jobs", str(job.job_id))
            # Prefer the owner from job.creator_user_name if available
            if job.creator_user_name:
                owner_email = job.creator_user_name
            
            discovered.append({
                'object_id': str(job.job_id),
                'workspace_id': workspace_id,
                'object_type': 'jobs',  # Changed from 'job' to 'jobs' to match watcher
                'object_name': job.settings.name if job.settings else 'Unnamed Job',
                'object_path': None,
                'owner_email': owner_email,
                'permissions': permissions,
                'metadata': {'created_time': str(job.created_time)},
                'is_active': True,
                'created_at': datetime.utcnow(),
                'updated_at': datetime.utcnow()
            })
        
        print(f"✓ Discovered {len(discovered)} jobs")
    except Exception as e:
        print(f"✗ Error discovering jobs: {str(e)}")
    
    return discovered

# COMMAND ----------

def discover_clusters(client, workspace_id: str) -> List[Dict[str, Any]]:
    """Discover all clusters."""
    print("Discovering clusters...")
    discovered = []
    
    try:
        for cluster in client.clusters.list():
            owner_email, permissions = get_permissions_safe(client, "clusters", cluster.cluster_id)
            # Prefer the owner from cluster.creator_user_name if available
            if cluster.creator_user_name:
                owner_email = cluster.creator_user_name
            
            discovered.append({
                'object_id': cluster.cluster_id,
                'workspace_id': workspace_id,
                'object_type': 'cluster',
                'object_name': cluster.cluster_name or 'Unnamed Cluster',
                'object_path': None,
                'owner_email': owner_email,
                'permissions': permissions,
                'metadata': {'state': cluster.state.value if cluster.state else 'UNKNOWN'},
                'is_active': True,
                'created_at': datetime.utcnow(),
                'updated_at': datetime.utcnow()
            })
        
        print(f"✓ Discovered {len(discovered)} clusters")
    except Exception as e:
        print(f"✗ Error discovering clusters: {str(e)}")
    
    return discovered

# COMMAND ----------

def discover_pipelines(client, workspace_id: str) -> List[Dict[str, Any]]:
    """Discover all Delta Live Tables pipelines."""
    print("Discovering pipelines...")
    discovered = []
    
    try:
        for pipeline in client.pipelines.list_pipelines():
            owner_email, permissions = get_permissions_safe(client, "pipelines", pipeline.pipeline_id)
            # Prefer the owner from pipeline.creator_user_name if available
            if pipeline.creator_user_name:
                owner_email = pipeline.creator_user_name
            
            discovered.append({
                'object_id': pipeline.pipeline_id,
                'workspace_id': workspace_id,
                'object_type': 'pipelines',  # Changed from 'pipeline' to 'pipelines' to match watcher
                'object_name': pipeline.name or 'Unnamed Pipeline',
                'object_path': None,
                'owner_email': owner_email,
                'permissions': permissions,
                'metadata': {'state': pipeline.state.value if pipeline.state else 'UNKNOWN'},
                'is_active': True,
                'created_at': datetime.utcnow(),
                'updated_at': datetime.utcnow()
            })
        
        print(f"✓ Discovered {len(discovered)} pipelines")
    except Exception as e:
        print(f"✗ Error discovering pipelines: {str(e)}")
    
    return discovered

# COMMAND ----------

def discover_apps(client, workspace_id: str) -> List[Dict[str, Any]]:
    """Discover all Databricks apps."""
    print("Discovering apps...")
    discovered = []
    
    try:
        # Check if apps API is available
        if not hasattr(client, 'apps'):
            print(f"  ⚠ Apps API not available in SDK")
            print(f"    Consider upgrading databricks-sdk to 0.20.0 or later")
            return discovered
        
        # Check if list method exists
        apps_api = client.apps
        if not hasattr(apps_api, 'list'):
            print(f"  ⚠ Apps API 'list' method not available in SDK")
            print(f"    Consider upgrading databricks-sdk to 0.20.0 or later")
            return discovered
        
        # Apps API list() returns an iterator
        apps_iterator = apps_api.list()
        
        for app in apps_iterator:
            # Apps use get_permissions() method, not the standard permissions API
            permissions = []
            try:
                if hasattr(apps_api, 'get_permissions'):
                    app_perms = apps_api.get_permissions(app.name)
                    if app_perms and app_perms.access_control_list:
                        for acl in app_perms.access_control_list:
                            principal_email = None
                            principal_type = 'user'
                            if hasattr(acl, 'user_name') and acl.user_name:
                                principal_email = acl.user_name
                                principal_type = 'user'
                            elif hasattr(acl, 'service_principal_name') and acl.service_principal_name:
                                principal_email = acl.service_principal_name
                                principal_type = 'service_principal'
                            elif hasattr(acl, 'group_name') and acl.group_name:
                                principal_email = acl.group_name
                                principal_type = 'group'
                            
                            if principal_email and hasattr(acl, 'all_permissions') and acl.all_permissions:
                                for perm in acl.all_permissions:
                                    permissions.append(Row(
                                        principal_email=principal_email,
                                        principal_type=principal_type,
                                        permission_level=perm.permission_level.value if hasattr(perm.permission_level, 'value') else str(perm.permission_level)
                                    ))
            except Exception as perm_error:
                print(f"  Warning: Could not get permissions for app {app.name}: {str(perm_error)}")
            
            # Extract status from app_status attribute (not status)
            status_str = 'UNKNOWN'
            if hasattr(app, 'app_status') and app.app_status and hasattr(app.app_status, 'state') and app.app_status.state:
                status_str = app.app_status.state.value if hasattr(app.app_status.state, 'value') else str(app.app_status.state)
            
            # Extract compute status
            compute_status_str = 'UNKNOWN'
            if hasattr(app, 'compute_status') and app.compute_status and hasattr(app.compute_status, 'state') and app.compute_status.state:
                compute_status_str = app.compute_status.state.value if hasattr(app.compute_status.state, 'value') else str(app.compute_status.state)
            
            # Get owner - could be 'creator' or 'owner' depending on SDK version
            owner_email = 'unknown'
            if hasattr(app, 'creator') and app.creator:
                owner_email = app.creator
            elif hasattr(app, 'owner') and app.owner:
                owner_email = app.owner
            
            discovered.append({
                'object_id': app.name,  # Apps use name as ID
                'workspace_id': workspace_id,
                'object_type': 'apps',
                'object_name': app.name,
                'object_path': None,
                'owner_email': owner_email,
                'permissions': permissions,
                'metadata': {
                    'app_status': status_str,
                    'compute_status': compute_status_str,
                    'description': app.description if hasattr(app, 'description') and app.description else '',
                    'create_time': str(app.create_time) if hasattr(app, 'create_time') and app.create_time else '',
                    'url': app.url if hasattr(app, 'url') and app.url else ''
                },
                'is_active': True,
                'created_at': datetime.utcnow(),
                'updated_at': datetime.utcnow()
            })
        
        print(f"✓ Discovered {len(discovered)} apps")
    except AttributeError as ae:
        print(f"  ⚠ Apps API not available in this SDK version: {str(ae)}")
        print(f"    Consider upgrading databricks-sdk to 0.20.0 or later")
    except Exception as e:
        error_msg = str(e)
        if "No API found" in error_msg or "404" in error_msg:
            print(f"  ⚠ Apps API not available: {error_msg}")
            print(f"    Apps may not be enabled for this workspace")
        else:
            print(f"✗ Error discovering apps: {error_msg}")
    
    return discovered

# COMMAND ----------

def discover_mlflow_experiments(client, workspace_id: str) -> List[Dict[str, Any]]:
    """Discover all MLflow experiments."""
    print("Discovering MLflow experiments...")
    discovered = []
    
    try:
        for experiment in client.experiments.list_experiments():
            # Use "mlflow-experiments" for permission type (not "experiments")
            owner_email, permissions = get_permissions_safe(client, "mlflow-experiments", experiment.experiment_id)
            
            discovered.append({
                'object_id': experiment.experiment_id,
                'workspace_id': workspace_id,
                'object_type': 'mlflowExperiments',
                'object_name': experiment.name or 'Unnamed Experiment',
                'object_path': None,
                'owner_email': owner_email,  # Extracted from permissions
                'permissions': permissions,
                'metadata': {'artifact_location': experiment.artifact_location or ''},
                'is_active': True,
                'created_at': datetime.utcnow(),
                'updated_at': datetime.utcnow()
            })
        
        print(f"✓ Discovered {len(discovered)} MLflow experiments")
    except Exception as e:
        print(f"✗ Error discovering MLflow experiments: {str(e)}")
    
    return discovered

# COMMAND ----------

def discover_monitors(client, workspace_id: str) -> List[Dict[str, Any]]:
    """Discover all data quality monitors (Lakehouse Monitoring)."""
    print("Discovering data quality monitors...")
    discovered = []
    
    try:
        # The correct SDK attribute is 'lakehouse_monitoring' (not 'quality_monitors')
        # Note: This API may not be available in all SDK versions
        if not hasattr(client, 'lakehouse_monitoring'):
            print(f"  ⚠ Lakehouse Monitoring API not available in SDK")
            print(f"    Consider upgrading databricks-sdk to 0.20.0 or later")
            return discovered
        
        # List monitors for all tables by iterating through Unity Catalog
        # The lakehouse_monitoring API requires a table_name to get specific monitors
        # We'll discover monitors by scanning tables
        monitor_count = 0
        for catalog_info in client.catalogs.list():
            catalog_name = catalog_info.name
            try:
                for schema_info in client.schemas.list(catalog_name=catalog_name):
                    schema_name = schema_info.name
                    try:
                        for table in client.tables.list(catalog_name=catalog_name, schema_name=schema_name):
                            full_name = f"{catalog_name}.{schema_name}.{table.name}"
                            try:
                                # Try to get monitor for this table
                                monitor = client.lakehouse_monitoring.get(table_name=full_name)
                                if monitor:
                                    # Monitors don't have traditional permissions API
                                    # Permissions are derived from the monitored table
                                    permissions = []
                                    owner_email = 'unknown'
                                    
                                    discovered.append({
                                        'object_id': full_name,  # Monitors use table name as ID
                                        'workspace_id': workspace_id,
                                        'object_type': 'monitors',
                                        'object_name': full_name,
                                        'object_path': full_name,
                                        'owner_email': owner_email,
                                        'permissions': permissions,
                                        'metadata': {
                                            'status': monitor.status.value if hasattr(monitor, 'status') and monitor.status else 'UNKNOWN',
                                            'output_schema_name': monitor.output_schema_name if hasattr(monitor, 'output_schema_name') else '',
                                            'assets_dir': monitor.assets_dir if hasattr(monitor, 'assets_dir') else ''
                                        },
                                        'is_active': True,
                                        'created_at': datetime.utcnow(),
                                        'updated_at': datetime.utcnow()
                                    })
                                    monitor_count += 1
                            except Exception:
                                # Table doesn't have a monitor, skip
                                pass
                    except Exception:
                        pass  # Skip schemas without access
            except Exception:
                pass  # Skip catalogs without access
        
        print(f"✓ Discovered {monitor_count} data quality monitors")
    except AttributeError as ae:
        print(f"  ⚠ Lakehouse Monitoring API not available in SDK: {str(ae)}")
        print(f"    Consider upgrading databricks-sdk to 0.20.0 or later")
    except Exception as e:
        print(f"✗ Error discovering data quality monitors: {str(e)}")
    
    return discovered

# COMMAND ----------

def discover_alerts(client, workspace_id: str) -> List[Dict[str, Any]]:
    """Discover all SQL alerts with permissions."""
    print("Discovering alerts...")
    discovered = []
    
    try:
        for alert in client.alerts.list():
            owner_email = 'unknown'
            if hasattr(alert, 'user') and alert.user and hasattr(alert.user, 'email'):
                owner_email = alert.user.email
            
            # Fetch permissions for the alert
            owner_from_perms, permissions = get_permissions_safe(client, "alerts", alert.id)
            if owner_email == 'unknown' and owner_from_perms != 'unknown':
                owner_email = owner_from_perms
            
            discovered.append({
                'object_id': alert.id,
                'workspace_id': workspace_id,
                'object_type': 'alert',
                'object_name': alert.name or 'Unnamed Alert',
                'object_path': None,
                'owner_email': owner_email,
                'permissions': permissions,
                'metadata': {
                    'state': alert.state.value if hasattr(alert, 'state') and alert.state else 'UNKNOWN',
                    'created_at': str(alert.created_at) if hasattr(alert, 'created_at') else ''
                },
                'is_active': True,
                'created_at': datetime.utcnow(),
                'updated_at': datetime.utcnow()
            })
        
        print(f"✓ Discovered {len(discovered)} alerts")
    except Exception as e:
        print(f"✗ Error discovering alerts: {str(e)}")
    
    return discovered

# COMMAND ----------

def discover_warehouses(client, workspace_id: str) -> List[Dict[str, Any]]:
    """Discover all SQL warehouses."""
    print("Discovering SQL warehouses...")
    discovered = []
    
    try:
        for warehouse in client.warehouses.list():
            owner_email, permissions = get_permissions_safe(client, "sql/warehouses", warehouse.id)
            if hasattr(warehouse, 'creator_name') and warehouse.creator_name:
                owner_email = warehouse.creator_name
            
            discovered.append({
                'object_id': warehouse.id,
                'workspace_id': workspace_id,
                'object_type': 'warehouse',
                'object_name': warehouse.name or 'Unnamed Warehouse',
                'object_path': None,
                'owner_email': owner_email,
                'permissions': permissions,
                'metadata': {
                    'state': warehouse.state.value if hasattr(warehouse, 'state') and warehouse.state else 'UNKNOWN',
                    'cluster_size': warehouse.cluster_size if hasattr(warehouse, 'cluster_size') else ''
                },
                'is_active': True,
                'created_at': datetime.utcnow(),
                'updated_at': datetime.utcnow()
            })
        
        print(f"✓ Discovered {len(discovered)} SQL warehouses")
    except Exception as e:
        print(f"✗ Error discovering SQL warehouses: {str(e)}")
    
    return discovered

# COMMAND ----------

def discover_cluster_policies(client, workspace_id: str) -> List[Dict[str, Any]]:
    """Discover all cluster policies."""
    print("Discovering cluster policies...")
    discovered = []
    
    try:
        for policy in client.cluster_policies.list():
            owner_email, permissions = get_permissions_safe(client, "cluster-policies", policy.policy_id)
            if hasattr(policy, 'creator_user_name') and policy.creator_user_name:
                owner_email = policy.creator_user_name
            
            discovered.append({
                'object_id': policy.policy_id,
                'workspace_id': workspace_id,
                'object_type': 'clusterPolicy',
                'object_name': policy.name or 'Unnamed Policy',
                'object_path': None,
                'owner_email': owner_email,
                'permissions': permissions,
                'metadata': {
                    'policy_family_id': policy.policy_family_id if hasattr(policy, 'policy_family_id') else '',
                    'is_default': str(policy.is_default) if hasattr(policy, 'is_default') else 'false'
                },
                'is_active': True,
                'created_at': datetime.utcnow(),
                'updated_at': datetime.utcnow()
            })
        
        print(f"✓ Discovered {len(discovered)} cluster policies")
    except Exception as e:
        print(f"✗ Error discovering cluster policies: {str(e)}")
    
    return discovered

# COMMAND ----------

def discover_instance_pools(client, workspace_id: str) -> List[Dict[str, Any]]:
    """Discover all instance pools."""
    print("Discovering instance pools...")
    discovered = []
    
    try:
        for pool in client.instance_pools.list():
            owner_email, permissions = get_permissions_safe(client, "instance-pools", pool.instance_pool_id)
            
            discovered.append({
                'object_id': pool.instance_pool_id,
                'workspace_id': workspace_id,
                'object_type': 'instancePool',
                'object_name': pool.instance_pool_name or 'Unnamed Pool',
                'object_path': None,
                'owner_email': owner_email,
                'permissions': permissions,
                'metadata': {
                    'state': pool.state.value if hasattr(pool, 'state') and pool.state else 'UNKNOWN',
                    'node_type_id': pool.node_type_id if hasattr(pool, 'node_type_id') else ''
                },
                'is_active': True,
                'created_at': datetime.utcnow(),
                'updated_at': datetime.utcnow()
            })
        
        print(f"✓ Discovered {len(discovered)} instance pools")
    except Exception as e:
        print(f"✗ Error discovering instance pools: {str(e)}")
    
    return discovered

# COMMAND ----------

def discover_serving_endpoints(client, workspace_id: str) -> List[Dict[str, Any]]:
    """Discover all model serving endpoints."""
    print("Discovering serving endpoints...")
    discovered = []
    
    try:
        for endpoint in client.serving_endpoints.list():
            owner_email, permissions = get_permissions_safe(client, "serving-endpoints", endpoint.name)
            if hasattr(endpoint, 'creator') and endpoint.creator:
                owner_email = endpoint.creator
            
            discovered.append({
                'object_id': endpoint.name,
                'workspace_id': workspace_id,
                'object_type': 'servingEndpoint',
                'object_name': endpoint.name,
                'object_path': None,
                'owner_email': owner_email,
                'permissions': permissions,
                'metadata': {
                    'state': endpoint.state.config_update.value if hasattr(endpoint, 'state') and endpoint.state else 'UNKNOWN',
                    'creation_timestamp': str(endpoint.creation_timestamp) if hasattr(endpoint, 'creation_timestamp') else ''
                },
                'is_active': True,
                'created_at': datetime.utcnow(),
                'updated_at': datetime.utcnow()
            })
        
        print(f"✓ Discovered {len(discovered)} serving endpoints")
    except Exception as e:
        print(f"✗ Error discovering serving endpoints: {str(e)}")
    
    return discovered

# COMMAND ----------

def discover_registered_models(client, workspace_id: str) -> List[Dict[str, Any]]:
    """Discover all registered models from Unity Catalog with grants."""
    print("Discovering registered models...")
    discovered = []
    
    try:
        # List all catalogs first
        for catalog_info in client.catalogs.list():
            catalog_name = catalog_info.name
            try:
                # List schemas in each catalog
                for schema_info in client.schemas.list(catalog_name=catalog_name):
                    schema_name = schema_info.name
                    try:
                        # List models in each schema
                        for model in client.registered_models.list(
                            catalog_name=catalog_name,
                            schema_name=schema_name
                        ):
                            full_name = f"{catalog_name}.{schema_name}.{model.name}"
                            owner_email = model.owner if hasattr(model, 'owner') and model.owner else 'unknown'
                            
                            # Get UC grants for the model
                            owner_from_grants, permissions = get_uc_grants_safe(client, 'REGISTERED_MODEL', full_name)
                            if owner_email == 'unknown' and owner_from_grants != 'unknown':
                                owner_email = owner_from_grants
                            
                            discovered.append({
                                'object_id': full_name,
                                'workspace_id': workspace_id,
                                'object_type': 'registeredModel',
                                'object_name': model.name,
                                'object_path': full_name,
                                'owner_email': owner_email,
                                'permissions': permissions,
                                'metadata': {
                                    'catalog': catalog_name,
                                    'schema': schema_name,
                                    'created_at': str(model.created_at) if hasattr(model, 'created_at') else ''
                                },
                                'is_active': True,
                                'created_at': datetime.utcnow(),
                                'updated_at': datetime.utcnow()
                            })
                    except Exception as model_error:
                        pass  # Skip schemas without model access
            except Exception as schema_error:
                pass  # Skip catalogs without schema access
        
        print(f"✓ Discovered {len(discovered)} registered models")
    except Exception as e:
        print(f"✗ Error discovering registered models: {str(e)}")
    
    return discovered

# COMMAND ----------

def discover_secret_scopes(client, workspace_id: str) -> List[Dict[str, Any]]:
    """Discover all secret scopes with ACLs using secrets.list_acls API."""
    print("Discovering secret scopes...")
    discovered = []
    
    try:
        for scope in client.secrets.list_scopes():
            # Get ACLs for this secret scope
            owner_email, permissions = get_secret_acls_safe(client, scope.name)
            
            discovered.append({
                'object_id': scope.name,
                'workspace_id': workspace_id,
                'object_type': 'secretScope',
                'object_name': scope.name,
                'object_path': None,
                'owner_email': owner_email,
                'permissions': permissions,
                'metadata': {
                    'backend_type': scope.backend_type.value if hasattr(scope, 'backend_type') and scope.backend_type else 'UNKNOWN'
                },
                'is_active': True,
                'created_at': datetime.utcnow(),
                'updated_at': datetime.utcnow()
            })
        
        print(f"✓ Discovered {len(discovered)} secret scopes")
    except Exception as e:
        print(f"✗ Error discovering secret scopes: {str(e)}")
    
    return discovered

# COMMAND ----------

def discover_vector_search_endpoints(client, workspace_id: str) -> List[Dict[str, Any]]:
    """Discover all vector search endpoints with permissions."""
    print("Discovering vector search endpoints...")
    discovered = []
    
    try:
        for endpoint in client.vector_search_endpoints.list_endpoints():
            owner_email = endpoint.creator if hasattr(endpoint, 'creator') and endpoint.creator else 'unknown'
            
            # Get permissions using the permissions API
            permissions = []
            try:
                perms = client.permissions.get(object_type="vector-search-endpoints", object_id=endpoint.name)
                if perms and perms.access_control_list:
                    for acl in perms.access_control_list:
                        principal_email = None
                        principal_type = 'user'
                        if acl.user_name:
                            principal_email = acl.user_name
                            principal_type = 'user'
                        elif acl.service_principal_name:
                            principal_email = acl.service_principal_name
                            principal_type = 'service_principal'
                        elif acl.group_name:
                            principal_email = acl.group_name
                            principal_type = 'group'
                        
                        if principal_email and acl.all_permissions:
                            for perm in acl.all_permissions:
                                if perm.permission_level.value == 'CAN_MANAGE' and not perm.inherited:
                                    owner_email = principal_email
                                permissions.append(Row(
                                    principal_email=principal_email,
                                    principal_type=principal_type,
                                    permission_level=perm.permission_level.value
                                ))
            except Exception as perm_error:
                pass  # Silently fail for permission errors
            
            discovered.append({
                'object_id': endpoint.name,
                'workspace_id': workspace_id,
                'object_type': 'vectorSearchEndpoint',
                'object_name': endpoint.name,
                'object_path': None,
                'owner_email': owner_email,
                'permissions': permissions,
                'metadata': {
                    'endpoint_type': endpoint.endpoint_type.value if hasattr(endpoint, 'endpoint_type') and endpoint.endpoint_type else 'UNKNOWN',
                    'endpoint_status': endpoint.endpoint_status.state.value if hasattr(endpoint, 'endpoint_status') and endpoint.endpoint_status else 'UNKNOWN'
                },
                'is_active': True,
                'created_at': datetime.utcnow(),
                'updated_at': datetime.utcnow()
            })
        
        print(f"✓ Discovered {len(discovered)} vector search endpoints")
    except Exception as e:
        print(f"✗ Error discovering vector search endpoints: {str(e)}")
    
    return discovered

# COMMAND ----------

def discover_catalogs(client, workspace_id: str) -> List[Dict[str, Any]]:
    """Discover all Unity Catalog catalogs with grants."""
    print("Discovering Unity Catalog catalogs...")
    discovered = []
    
    try:
        for catalog_info in client.catalogs.list():
            owner_email = catalog_info.owner if hasattr(catalog_info, 'owner') and catalog_info.owner else 'unknown'
            
            # Get UC grants for the catalog
            owner_from_grants, permissions = get_uc_grants_safe(client, 'CATALOG', catalog_info.name)
            if owner_email == 'unknown' and owner_from_grants != 'unknown':
                owner_email = owner_from_grants
            
            discovered.append({
                'object_id': catalog_info.name,
                'workspace_id': workspace_id,
                'object_type': 'catalog',
                'object_name': catalog_info.name,
                'object_path': None,
                'owner_email': owner_email,
                'permissions': permissions,
                'metadata': {
                    'catalog_type': catalog_info.catalog_type.value if hasattr(catalog_info, 'catalog_type') and catalog_info.catalog_type else 'MANAGED',
                    'isolation_mode': catalog_info.isolation_mode.value if hasattr(catalog_info, 'isolation_mode') and catalog_info.isolation_mode else 'OPEN',
                    'created_at': str(catalog_info.created_at) if hasattr(catalog_info, 'created_at') else ''
                },
                'is_active': True,
                'created_at': datetime.utcnow(),
                'updated_at': datetime.utcnow()
            })
        
        print(f"✓ Discovered {len(discovered)} catalogs")
    except Exception as e:
        print(f"✗ Error discovering catalogs: {str(e)}")
    
    return discovered

# COMMAND ----------

def discover_schemas(client, workspace_id: str) -> List[Dict[str, Any]]:
    """Discover all Unity Catalog schemas with grants."""
    print("Discovering Unity Catalog schemas...")
    discovered = []
    
    try:
        for catalog_info in client.catalogs.list():
            catalog_name = catalog_info.name
            try:
                for schema_info in client.schemas.list(catalog_name=catalog_name):
                    full_name = f"{catalog_name}.{schema_info.name}"
                    owner_email = schema_info.owner if hasattr(schema_info, 'owner') and schema_info.owner else 'unknown'
                    
                    # Get UC grants for the schema
                    owner_from_grants, permissions = get_uc_grants_safe(client, 'SCHEMA', full_name)
                    if owner_email == 'unknown' and owner_from_grants != 'unknown':
                        owner_email = owner_from_grants
                    
                    discovered.append({
                        'object_id': full_name,
                        'workspace_id': workspace_id,
                        'object_type': 'schema',
                        'object_name': schema_info.name,
                        'object_path': full_name,
                        'owner_email': owner_email,
                        'permissions': permissions,
                        'metadata': {
                            'catalog': catalog_name,
                            'created_at': str(schema_info.created_at) if hasattr(schema_info, 'created_at') else ''
                        },
                        'is_active': True,
                        'created_at': datetime.utcnow(),
                        'updated_at': datetime.utcnow()
                    })
            except Exception as schema_error:
                pass  # Skip catalogs without access
        
        print(f"✓ Discovered {len(discovered)} schemas")
    except Exception as e:
        print(f"✗ Error discovering schemas: {str(e)}")
    
    return discovered

# COMMAND ----------

def discover_volumes(client, workspace_id: str) -> List[Dict[str, Any]]:
    """Discover all Unity Catalog volumes with grants."""
    print("Discovering Unity Catalog volumes...")
    discovered = []
    
    try:
        for catalog_info in client.catalogs.list():
            catalog_name = catalog_info.name
            try:
                for schema_info in client.schemas.list(catalog_name=catalog_name):
                    schema_name = schema_info.name
                    try:
                        for volume in client.volumes.list(
                            catalog_name=catalog_name,
                            schema_name=schema_name
                        ):
                            full_name = f"{catalog_name}.{schema_name}.{volume.name}"
                            owner_email = volume.owner if hasattr(volume, 'owner') and volume.owner else 'unknown'
                            
                            # Get UC grants for the volume
                            owner_from_grants, permissions = get_uc_grants_safe(client, 'VOLUME', full_name)
                            if owner_email == 'unknown' and owner_from_grants != 'unknown':
                                owner_email = owner_from_grants
                            
                            discovered.append({
                                'object_id': full_name,
                                'workspace_id': workspace_id,
                                'object_type': 'volume',
                                'object_name': volume.name,
                                'object_path': full_name,
                                'owner_email': owner_email,
                                'permissions': permissions,
                                'metadata': {
                                    'catalog': catalog_name,
                                    'schema': schema_name,
                                    'volume_type': volume.volume_type.value if hasattr(volume, 'volume_type') and volume.volume_type else 'MANAGED',
                                    'created_at': str(volume.created_at) if hasattr(volume, 'created_at') else ''
                                },
                                'is_active': True,
                                'created_at': datetime.utcnow(),
                                'updated_at': datetime.utcnow()
                            })
                    except Exception as vol_error:
                        pass  # Skip schemas without volume access
            except Exception as schema_error:
                pass  # Skip catalogs without schema access
        
        print(f"✓ Discovered {len(discovered)} volumes")
    except Exception as e:
        print(f"✗ Error discovering volumes: {str(e)}")
    
    return discovered

# COMMAND ----------

def discover_connections(client, workspace_id: str) -> List[Dict[str, Any]]:
    """Discover all Unity Catalog connections with grants."""
    print("Discovering Unity Catalog connections...")
    discovered = []
    
    try:
        for connection in client.connections.list():
            owner_email = connection.owner if hasattr(connection, 'owner') and connection.owner else 'unknown'
            
            # Get UC grants for the connection
            owner_from_grants, permissions = get_uc_grants_safe(client, 'CONNECTION', connection.name)
            if owner_email == 'unknown' and owner_from_grants != 'unknown':
                owner_email = owner_from_grants
            
            discovered.append({
                'object_id': connection.name,
                'workspace_id': workspace_id,
                'object_type': 'connection',
                'object_name': connection.name,
                'object_path': None,
                'owner_email': owner_email,
                'permissions': permissions,
                'metadata': {
                    'connection_type': connection.connection_type.value if hasattr(connection, 'connection_type') and connection.connection_type else 'UNKNOWN',
                    'created_at': str(connection.created_at) if hasattr(connection, 'created_at') else ''
                },
                'is_active': True,
                'created_at': datetime.utcnow(),
                'updated_at': datetime.utcnow()
            })
        
        print(f"✓ Discovered {len(discovered)} connections")
    except Exception as e:
        print(f"✗ Error discovering connections: {str(e)}")
    
    return discovered

# COMMAND ----------

def discover_tables(client, workspace_id: str) -> List[Dict[str, Any]]:
    """Discover all Unity Catalog tables with grants."""
    print("Discovering Unity Catalog tables...")
    discovered = []
    
    try:
        for catalog_info in client.catalogs.list():
            catalog_name = catalog_info.name
            try:
                for schema_info in client.schemas.list(catalog_name=catalog_name):
                    schema_name = schema_info.name
                    try:
                        for table in client.tables.list(
                            catalog_name=catalog_name,
                            schema_name=schema_name
                        ):
                            full_name = f"{catalog_name}.{schema_name}.{table.name}"
                            owner_email = table.owner if hasattr(table, 'owner') and table.owner else 'unknown'
                            
                            # Get UC grants for the table
                            owner_from_grants, permissions = get_uc_grants_safe(client, 'TABLE', full_name)
                            if owner_email == 'unknown' and owner_from_grants != 'unknown':
                                owner_email = owner_from_grants
                            
                            discovered.append({
                                'object_id': full_name,
                                'workspace_id': workspace_id,
                                'object_type': 'table',
                                'object_name': table.name,
                                'object_path': full_name,
                                'owner_email': owner_email,
                                'permissions': permissions,
                                'metadata': {
                                    'catalog': catalog_name,
                                    'schema': schema_name,
                                    'table_type': table.table_type.value if hasattr(table, 'table_type') and table.table_type else 'MANAGED',
                                    'created_at': str(table.created_at) if hasattr(table, 'created_at') else ''
                                },
                                'is_active': True,
                                'created_at': datetime.utcnow(),
                                'updated_at': datetime.utcnow()
                            })
                    except Exception as table_error:
                        pass  # Skip schemas without table access
            except Exception as schema_error:
                pass  # Skip catalogs without schema access
        
        print(f"✓ Discovered {len(discovered)} tables")
    except Exception as e:
        print(f"✗ Error discovering tables: {str(e)}")
    
    return discovered

# COMMAND ----------

def discover_functions(client, workspace_id: str) -> List[Dict[str, Any]]:
    """Discover all Unity Catalog functions with grants."""
    print("Discovering Unity Catalog functions...")
    discovered = []
    
    try:
        for catalog_info in client.catalogs.list():
            catalog_name = catalog_info.name
            try:
                for schema_info in client.schemas.list(catalog_name=catalog_name):
                    schema_name = schema_info.name
                    try:
                        for func in client.functions.list(
                            catalog_name=catalog_name,
                            schema_name=schema_name
                        ):
                            full_name = f"{catalog_name}.{schema_name}.{func.name}"
                            owner_email = func.owner if hasattr(func, 'owner') and func.owner else 'unknown'
                            
                            # Get UC grants for the function
                            owner_from_grants, permissions = get_uc_grants_safe(client, 'FUNCTION', full_name)
                            if owner_email == 'unknown' and owner_from_grants != 'unknown':
                                owner_email = owner_from_grants
                            
                            discovered.append({
                                'object_id': full_name,
                                'workspace_id': workspace_id,
                                'object_type': 'function',
                                'object_name': func.name,
                                'object_path': full_name,
                                'owner_email': owner_email,
                                'permissions': permissions,
                                'metadata': {
                                    'catalog': catalog_name,
                                    'schema': schema_name,
                                    'function_type': func.routine_type.value if hasattr(func, 'routine_type') and func.routine_type else 'UNKNOWN',
                                    'created_at': str(func.created_at) if hasattr(func, 'created_at') else ''
                                },
                                'is_active': True,
                                'created_at': datetime.utcnow(),
                                'updated_at': datetime.utcnow()
                            })
                    except Exception as func_error:
                        pass  # Skip schemas without function access
            except Exception as schema_error:
                pass  # Skip catalogs without schema access
        
        print(f"✓ Discovered {len(discovered)} functions")
    except Exception as e:
        print(f"✗ Error discovering functions: {str(e)}")
    
    return discovered

# COMMAND ----------

def discover_external_locations(client, workspace_id: str) -> List[Dict[str, Any]]:
    """Discover all Unity Catalog external locations with grants."""
    print("Discovering Unity Catalog external locations...")
    discovered = []
    
    try:
        for ext_loc in client.external_locations.list():
            owner_email = ext_loc.owner if hasattr(ext_loc, 'owner') and ext_loc.owner else 'unknown'
            
            # Get UC grants for the external location
            owner_from_grants, permissions = get_uc_grants_safe(client, 'EXTERNAL_LOCATION', ext_loc.name)
            if owner_email == 'unknown' and owner_from_grants != 'unknown':
                owner_email = owner_from_grants
            
            discovered.append({
                'object_id': ext_loc.name,
                'workspace_id': workspace_id,
                'object_type': 'externalLocation',
                'object_name': ext_loc.name,
                'object_path': ext_loc.url if hasattr(ext_loc, 'url') else None,
                'owner_email': owner_email,
                'permissions': permissions,
                'metadata': {
                    'url': ext_loc.url if hasattr(ext_loc, 'url') else '',
                    'credential_name': ext_loc.credential_name if hasattr(ext_loc, 'credential_name') else '',
                    'created_at': str(ext_loc.created_at) if hasattr(ext_loc, 'created_at') else ''
                },
                'is_active': True,
                'created_at': datetime.utcnow(),
                'updated_at': datetime.utcnow()
            })
        
        print(f"✓ Discovered {len(discovered)} external locations")
    except Exception as e:
        print(f"✗ Error discovering external locations: {str(e)}")
    
    return discovered

# COMMAND ----------

def discover_storage_credentials(client, workspace_id: str) -> List[Dict[str, Any]]:
    """Discover all Unity Catalog storage credentials with grants."""
    print("Discovering Unity Catalog storage credentials...")
    discovered = []
    
    try:
        for cred in client.storage_credentials.list():
            owner_email = cred.owner if hasattr(cred, 'owner') and cred.owner else 'unknown'
            
            # Get UC grants for the storage credential
            owner_from_grants, permissions = get_uc_grants_safe(client, 'STORAGE_CREDENTIAL', cred.name)
            if owner_email == 'unknown' and owner_from_grants != 'unknown':
                owner_email = owner_from_grants
            
            discovered.append({
                'object_id': cred.name,
                'workspace_id': workspace_id,
                'object_type': 'storageCredential',
                'object_name': cred.name,
                'object_path': None,
                'owner_email': owner_email,
                'permissions': permissions,
                'metadata': {
                    'created_at': str(cred.created_at) if hasattr(cred, 'created_at') else ''
                },
                'is_active': True,
                'created_at': datetime.utcnow(),
                'updated_at': datetime.utcnow()
            })
        
        print(f"✓ Discovered {len(discovered)} storage credentials")
    except Exception as e:
        print(f"✗ Error discovering storage credentials: {str(e)}")
    
    return discovered

# COMMAND ----------

def discover_shares(client, workspace_id: str) -> List[Dict[str, Any]]:
    """Discover all Unity Catalog shares with grants."""
    print("Discovering Unity Catalog shares...")
    discovered = []
    
    try:
        for share in client.shares.list():
            owner_email = share.owner if hasattr(share, 'owner') and share.owner else 'unknown'
            
            # Get UC grants for the share
            owner_from_grants, permissions = get_uc_grants_safe(client, 'SHARE', share.name)
            if owner_email == 'unknown' and owner_from_grants != 'unknown':
                owner_email = owner_from_grants
            
            discovered.append({
                'object_id': share.name,
                'workspace_id': workspace_id,
                'object_type': 'share',
                'object_name': share.name,
                'object_path': None,
                'owner_email': owner_email,
                'permissions': permissions,
                'metadata': {
                    'created_at': str(share.created_at) if hasattr(share, 'created_at') else ''
                },
                'is_active': True,
                'created_at': datetime.utcnow(),
                'updated_at': datetime.utcnow()
            })
        
        print(f"✓ Discovered {len(discovered)} shares")
    except Exception as e:
        print(f"✗ Error discovering shares: {str(e)}")
    
    return discovered

# COMMAND ----------

def discover_recipients(client, workspace_id: str) -> List[Dict[str, Any]]:
    """Discover all Unity Catalog recipients with grants."""
    print("Discovering Unity Catalog recipients...")
    discovered = []
    
    try:
        for recipient in client.recipients.list():
            owner_email = recipient.owner if hasattr(recipient, 'owner') and recipient.owner else 'unknown'
            
            # Get UC grants for the recipient
            owner_from_grants, permissions = get_uc_grants_safe(client, 'RECIPIENT', recipient.name)
            if owner_email == 'unknown' and owner_from_grants != 'unknown':
                owner_email = owner_from_grants
            
            discovered.append({
                'object_id': recipient.name,
                'workspace_id': workspace_id,
                'object_type': 'recipient',
                'object_name': recipient.name,
                'object_path': None,
                'owner_email': owner_email,
                'permissions': permissions,
                'metadata': {
                    'authentication_type': recipient.authentication_type.value if hasattr(recipient, 'authentication_type') and recipient.authentication_type else 'UNKNOWN',
                    'created_at': str(recipient.created_at) if hasattr(recipient, 'created_at') else ''
                },
                'is_active': True,
                'created_at': datetime.utcnow(),
                'updated_at': datetime.utcnow()
            })
        
        print(f"✓ Discovered {len(discovered)} recipients")
    except Exception as e:
        print(f"✗ Error discovering recipients: {str(e)}")
    
    return discovered

# COMMAND ----------

def discover_providers(client, workspace_id: str) -> List[Dict[str, Any]]:
    """Discover all Unity Catalog providers with grants."""
    print("Discovering Unity Catalog providers...")
    discovered = []
    
    try:
        for provider in client.providers.list():
            owner_email = provider.owner if hasattr(provider, 'owner') and provider.owner else 'unknown'
            
            # Get UC grants for the provider
            owner_from_grants, permissions = get_uc_grants_safe(client, 'PROVIDER', provider.name)
            if owner_email == 'unknown' and owner_from_grants != 'unknown':
                owner_email = owner_from_grants
            
            discovered.append({
                'object_id': provider.name,
                'workspace_id': workspace_id,
                'object_type': 'provider',
                'object_name': provider.name,
                'object_path': None,
                'owner_email': owner_email,
                'permissions': permissions,
                'metadata': {
                    'authentication_type': provider.authentication_type.value if hasattr(provider, 'authentication_type') and provider.authentication_type else 'UNKNOWN',
                    'created_at': str(provider.created_at) if hasattr(provider, 'created_at') else ''
                },
                'is_active': True,
                'created_at': datetime.utcnow(),
                'updated_at': datetime.utcnow()
            })
        
        print(f"✓ Discovered {len(discovered)} providers")
    except Exception as e:
        print(f"✗ Error discovering providers: {str(e)}")
    
    return discovered

# COMMAND ----------

def discover_clean_rooms(client, workspace_id: str) -> List[Dict[str, Any]]:
    """Discover all clean rooms."""
    print("Discovering clean rooms...")
    discovered = []
    
    try:
        # Check if clean_rooms API is available
        if not hasattr(client, 'clean_rooms'):
            print(f"  ⚠ Clean Rooms API not available in SDK")
            print(f"    Consider upgrading databricks-sdk to 0.20.0 or later")
            return discovered
        
        # The clean_rooms.list() method returns an iterator of CleanRoom objects
        for clean_room in client.clean_rooms.list():
            owner_email = 'unknown'
            if hasattr(clean_room, 'owner'):
                owner_email = clean_room.owner
            elif hasattr(clean_room, 'creator'):
                owner_email = clean_room.creator
            
            # Get clean room name safely
            room_name = getattr(clean_room, 'name', 'Unknown')
            
            discovered.append({
                'object_id': room_name,
                'workspace_id': workspace_id,
                'object_type': 'cleanRoom',
                'object_name': room_name,
                'object_path': None,
                'owner_email': owner_email,
                'permissions': [],  # Clean rooms don't have standard UC grants
                'metadata': {
                    'created_at': str(clean_room.created_at) if hasattr(clean_room, 'created_at') else '',
                    'status': clean_room.status.value if hasattr(clean_room, 'status') and clean_room.status else 'UNKNOWN',
                    'comment': clean_room.comment if hasattr(clean_room, 'comment') else ''
                },
                'is_active': True,
                'created_at': datetime.utcnow(),
                'updated_at': datetime.utcnow()
            })
        
        print(f"✓ Discovered {len(discovered)} clean rooms")
    except AttributeError as ae:
        print(f"  ⚠ Clean Rooms API not available in SDK: {str(ae)}")
        print(f"    Consider upgrading databricks-sdk to 0.20.0 or later")
    except Exception as e:
        # More graceful handling for API not available errors
        error_msg = str(e)
        if "No API found" in error_msg or "404" in error_msg:
            print(f"  ⚠ Clean Rooms API not available: {error_msg}")
            print(f"    This feature may require a specific Databricks account configuration")
        else:
            print(f"✗ Error discovering clean rooms: {error_msg}")
    
    return discovered

# COMMAND ----------

def discover_metastores(client, workspace_id: str) -> List[Dict[str, Any]]:
    """Discover all Unity Catalog metastores with grants."""
    print("Discovering Unity Catalog metastores...")
    discovered = []
    
    try:
        for metastore in client.metastores.list():
            owner_email = metastore.owner if hasattr(metastore, 'owner') and metastore.owner else 'unknown'
            
            # Get UC grants for the metastore
            metastore_name = metastore.name if hasattr(metastore, 'name') else metastore.metastore_id
            owner_from_grants, permissions = get_uc_grants_safe(client, 'METASTORE', metastore_name)
            if owner_email == 'unknown' and owner_from_grants != 'unknown':
                owner_email = owner_from_grants
            
            discovered.append({
                'object_id': metastore.metastore_id,
                'workspace_id': workspace_id,
                'object_type': 'metastore',
                'object_name': metastore_name,
                'object_path': None,
                'owner_email': owner_email,
                'permissions': permissions,
                'metadata': {
                    'region': metastore.region if hasattr(metastore, 'region') else '',
                    'created_at': str(metastore.created_at) if hasattr(metastore, 'created_at') else ''
                },
                'is_active': True,
                'created_at': datetime.utcnow(),
                'updated_at': datetime.utcnow()
            })
        
        print(f"✓ Discovered {len(discovered)} metastores")
    except Exception as e:
        print(f"✗ Error discovering metastores: {str(e)}")
    
    return discovered

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Discovery Functions (Preview/New Features)

# COMMAND ----------

def get_genie_space_permissions(space_id: str) -> List[Any]:
    """
    Get permissions for a Genie space using the REST API.
    Uses dbutils to get the API token and workspace host.
    """
    try:
        import requests
        from dbruntime.databricks_repl_context import get_context
        
        ctx = get_context()
        host = ctx.browserHostName
        token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
        
        response = requests.get(
            f"https://{host}/api/2.0/permissions/genie-spaces/{space_id}",
            headers={"Authorization": f"Bearer {token}"}
        )
        
        permissions = []
        if response.status_code == 200:
            data = response.json()
            if 'access_control_list' in data:
                for acl in data['access_control_list']:
                    principal_email = None
                    principal_type = 'user'
                    if acl.get('user_name'):
                        principal_email = acl.get('user_name')
                        principal_type = 'user'
                    elif acl.get('service_principal_name'):
                        principal_email = acl.get('service_principal_name')
                        principal_type = 'service_principal'
                    elif acl.get('group_name'):
                        principal_email = acl.get('group_name')
                        principal_type = 'group'
                    
                    if principal_email and 'all_permissions' in acl:
                        for perm in acl['all_permissions']:
                            permissions.append(Row(
                                principal_email=principal_email,
                                principal_type=principal_type,
                                permission_level=perm.get('permission_level', 'UNKNOWN')
                            ))
        return permissions
    except Exception as e:
        return []

def discover_genie_spaces(client, workspace_id: str) -> List[Dict[str, Any]]:
    """Discover AI/BI Genie Spaces using genie.list_spaces API with permissions."""
    discovered = []
    print(f"  Discovering Genie Spaces...")
    
    try:
        # Use the correct API: genie.list_spaces()
        spaces = list(client.genie.list_spaces())
        
        for space in spaces:
            try:
                space_id = getattr(space, 'space_id', str(space.id) if hasattr(space, 'id') else 'unknown')
                space_name = getattr(space, 'name', getattr(space, 'title', 'Unknown'))
                owner_email = getattr(space, 'creator_user_name', 'unknown')
                
                # Get permissions using REST API
                permissions = get_genie_space_permissions(space_id)
                
                # Try to extract owner from permissions if not available
                if owner_email == 'unknown':
                    for perm in permissions:
                        if perm.permission_level == 'CAN_MANAGE':
                            owner_email = perm.principal_email
                            break
                
                discovered.append({
                    'object_id': space_id,
                    'workspace_id': workspace_id,
                    'object_type': 'genieSpace',
                    'object_name': space_name,
                    'object_path': f'/genie/{space_id}',
                    'owner_email': owner_email,
                    'permissions': permissions,
                    'metadata': {},
                    'is_active': True,
                    'created_at': datetime.utcnow(),
                    'updated_at': datetime.utcnow()
                })
            except Exception as e:
                print(f"    Warning: Error processing Genie space: {str(e)}")
        
        print(f"  ✓ Discovered {len(discovered)} Genie Spaces")
    except AttributeError:
        print(f"  ⚠ Genie API not available in SDK")
    except Exception as e:
        print(f"  ✗ Error discovering Genie Spaces: {str(e)}")
    
    return discovered

def discover_vector_indexes(client, workspace_id: str) -> List[Dict[str, Any]]:
    """Discover Vector Search Indexes with grants using TABLE securable type."""
    discovered = []
    print(f"  Discovering Vector Search Indexes...")
    
    try:
        # First get all endpoints
        endpoints = list(client.vector_search_endpoints.list_endpoints())
        
        for endpoint in endpoints:
            try:
                endpoint_name = endpoint.name
                # List indexes for each endpoint
                indexes = list(client.vector_search_indexes.list_indexes(endpoint_name=endpoint_name))
                
                for index in indexes:
                    index_name = index.name
                    owner_email = getattr(index, 'creator', 'unknown')
                    
                    # Vector indexes use TABLE grants (INDEX_FQN)
                    owner_from_grants, permissions = get_uc_grants_safe(client, 'TABLE', index_name)
                    if owner_email == 'unknown' and owner_from_grants != 'unknown':
                        owner_email = owner_from_grants
                    
                    discovered.append({
                        'object_id': index_name,
                        'workspace_id': workspace_id,
                        'object_type': 'vectorIndex',
                        'object_name': index_name,
                        'object_path': f'/vector-search/{endpoint_name}/{index_name}',
                        'owner_email': owner_email,
                        'permissions': permissions,
                        'metadata': {'endpoint_name': endpoint_name},
                        'is_active': True,
                        'created_at': datetime.utcnow(),
                        'updated_at': datetime.utcnow()
                    })
            except Exception as e:
                print(f"    Warning: Error listing indexes for endpoint {endpoint.name}: {str(e)}")
        
        print(f"  ✓ Discovered {len(discovered)} Vector Indexes")
    except Exception as e:
        print(f"  ✗ Error discovering Vector Indexes: {str(e)}")
    
    return discovered

def discover_uc_registered_models(client, workspace_id: str) -> List[Dict[str, Any]]:
    """Discover Unity Catalog Registered Models with grants."""
    discovered = []
    print(f"  Discovering UC Registered Models...")
    
    try:
        # List all catalogs first
        catalogs = list(client.catalogs.list())
        
        for catalog_obj in catalogs:
            try:
                catalog_name = catalog_obj.name
                # List models in each catalog
                models = list(client.registered_models.list(catalog_name=catalog_name))
                
                for model in models:
                    full_name = model.full_name
                    owner_email = getattr(model, 'owner', 'unknown')
                    
                    # Get UC grants for the registered model
                    owner_from_grants, permissions = get_uc_grants_safe(client, 'REGISTERED_MODEL', full_name)
                    if owner_email == 'unknown' and owner_from_grants != 'unknown':
                        owner_email = owner_from_grants
                    
                    discovered.append({
                        'object_id': full_name,
                        'workspace_id': workspace_id,
                        'object_type': 'ucRegisteredModel',
                        'object_name': model.name,
                        'object_path': full_name,
                        'owner_email': owner_email,
                        'permissions': permissions,
                        'metadata': {
                            'catalog_name': catalog_name,
                            'schema_name': getattr(model, 'schema_name', 'unknown')
                        },
                        'is_active': True,
                        'created_at': datetime.utcnow(),
                        'updated_at': datetime.utcnow()
                    })
            except Exception as e:
                # Skip catalogs we can't access
                continue
        
        print(f"  ✓ Discovered {len(discovered)} UC Registered Models")
    except Exception as e:
        print(f"  ✗ Error discovering UC Registered Models: {str(e)}")
    
    return discovered

def discover_feature_tables(client, workspace_id: str) -> List[Dict[str, Any]]:
    """
    Discover Feature Tables using TABLE grants.
    Feature tables are UC tables with feature metadata, so we use TABLE securable type.
    """
    discovered = []
    print(f"  Discovering Feature Tables...")
    
    try:
        # Feature tables in Unity Catalog are just tables with feature metadata
        # We'll scan tables and identify those that are feature tables
        # by checking for feature-specific properties or by querying the feature store
        
        try:
            from databricks.feature_engineering import FeatureEngineeringClient
            fe_client = FeatureEngineeringClient()
            
            # Try to list feature tables from the feature engineering client
            # Note: This API may vary by SDK version
            print(f"  ⚠ Feature tables use TABLE grants - discovered via 'tables' discovery")
            print(f"  ⚠ Use grants.get_effective(securable_type='TABLE', full_name=FEATURE_TABLE_FQN) for permissions")
        except ImportError:
            print(f"  ⚠ Feature Engineering client not available")
    except Exception as e:
        print(f"  ✗ Error discovering Feature Tables: {str(e)}")
    
    return discovered

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Discovery

# COMMAND ----------

debug_mode = debug_permissions == "Y"

discovery_functions = {
    'workspace_objects': lambda c, w: discover_workspace_objects(c, w, use_selective_filter, max_threads, debug_mode),
    'query': discover_queries,
    'dashboard': discover_dashboards,
    'jobs': discover_jobs,
    'cluster': discover_clusters,
    'pipelines': discover_pipelines,
    'apps': discover_apps,
    'mlflowExperiments': discover_mlflow_experiments,
    'monitors': discover_monitors,
    # Compute & SQL
    'alerts': discover_alerts,
    'warehouses': discover_warehouses,
    'clusterPolicies': discover_cluster_policies,
    'instancePools': discover_instance_pools,
    # ML & Serving
    'servingEndpoints': discover_serving_endpoints,
    'registeredModels': discover_registered_models,
    # Security
    'secretScopes': discover_secret_scopes,
    'vectorSearchEndpoints': discover_vector_search_endpoints,
    # Unity Catalog
    'catalogs': discover_catalogs,
    'schemas': discover_schemas,
    'tables': discover_tables,
    'volumes': discover_volumes,
    'functions': discover_functions,
    'connections': discover_connections,
    'externalLocations': discover_external_locations,
    'storageCredentials': discover_storage_credentials,
    # Delta Sharing
    'shares': discover_shares,
    'recipients': discover_recipients,
    'providers': discover_providers,
    # Other
    'cleanRooms': discover_clean_rooms,
    'metastores': discover_metastores,
    # Additional/Preview features
    'genieSpaces': discover_genie_spaces,
    'vectorIndexes': discover_vector_indexes,
    'ucRegisteredModels': discover_uc_registered_models,
    'featureTables': discover_feature_tables,
}

all_discovered = []
counts = {}

# Use ThreadPoolExecutor for parallel discovery
print(f"\nStarting parallel discovery for {len(object_types)} object types...")
print("="*80)

with ThreadPoolExecutor(max_workers=min(len(object_types), 7)) as executor:
    # Submit all discovery tasks
    future_to_type = {}
    for object_type in object_types:
        if object_type in discovery_functions:
            future = executor.submit(discovery_functions[object_type], client, workspace_id)
            future_to_type[future] = object_type
        else:
            print(f"Warning: Unknown object type: {object_type}")
            counts[object_type] = 0
    
    # Collect results as they complete
    for future in as_completed(future_to_type):
        object_type = future_to_type[future]
        try:
            discovered = future.result()
            all_discovered.extend(discovered)
            counts[object_type] = len(discovered)
        except Exception as e:
            print(f"✗ Error in {object_type} discovery: {str(e)}")
            counts[object_type] = 0

print("="*80)
print(f"Total discovered: {len(all_discovered)} objects")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Table

# COMMAND ----------

if all_discovered:
    # Define schema matching the table structure
    discovered_schema = StructType([
        StructField('object_id', StringType(), True),
        StructField('workspace_id', StringType(), True),
        StructField('object_type', StringType(), True),
        StructField('object_name', StringType(), True),
        StructField('object_path', StringType(), True),
        StructField('owner_email', StringType(), True),
        StructField('permissions', ArrayType(StructType([
            StructField('principal_email', StringType(), True),
            StructField('principal_type', StringType(), True),  # 'user', 'group', or 'service_principal'
            StructField('permission_level', StringType(), True)
        ])), True),
        StructField('metadata', MapType(StringType(), StringType()), True),
        StructField('is_active', BooleanType(), True),
        StructField('created_at', TimestampType(), True),
        StructField('updated_at', TimestampType(), True)
    ])
    
    # Create DataFrame with explicit schema
    df = spark.createDataFrame(all_discovered, schema=discovered_schema)
    
    # Deduplicate by workspace_id and object_id (keep the most recent)
    # This prevents MERGE conflicts when multiple source rows match the same target
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number, desc
    
    window_spec = Window.partitionBy("workspace_id", "object_id").orderBy(desc("updated_at"))
    df_deduped = df.withColumn("row_num", row_number().over(window_spec)) \
                   .filter("row_num = 1") \
                   .drop("row_num")
    
    deduped_count = df_deduped.count()
    if deduped_count < len(all_discovered):
        print(f"⚠️  Removed {len(all_discovered) - deduped_count} duplicate entries")
    
    # Write to table
    table_name = f"{catalog}.{schema}.governance_preapproved_objects"
    df_deduped.createOrReplaceTempView("discovered_resources")
    
    spark.sql(f"""
        MERGE INTO {table_name} AS target
        USING discovered_resources AS source
        ON target.workspace_id = source.workspace_id 
        AND target.object_id = source.object_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    
    print(f"✓ Written {deduped_count} objects to {table_name}")
else:
    print("No objects discovered")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("\n" + "="*80)
print("DISCOVERY COMPLETE")
print("="*80 + "\n")

for object_type, count in counts.items():
    print(f"  {object_type:20s}: {count:5d} objects")

print(f"\nTotal: {sum(counts.values())} objects discovered")

# Show summary
summary_df = spark.sql(f"""
    SELECT 
        object_type,
        COUNT(*) AS count,
        COUNT(DISTINCT owner_email) AS unique_owners
    FROM {catalog}.{schema}.governance_preapproved_objects
    WHERE workspace_id = '{workspace_id}'
    AND is_active = true
    GROUP BY object_type
    ORDER BY count DESC
""")

print("\nObjects in workspace:")
display(summary_df)

# COMMAND ----------

# Return status
dbutils.notebook.exit(json.dumps({
    'status': 'SUCCESS',
    'workspace_id': workspace_id,
    'counts': counts,
    'total': sum(counts.values()),
    'timestamp': datetime.utcnow().isoformat()
}))

