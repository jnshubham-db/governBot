# Databricks notebook source
# MAGIC %md
# MAGIC # Governance Notification
# MAGIC
# MAGIC This notebook sends notifications to users about remediation actions taken on their resources.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog", "governance", "Catalog Name")
dbutils.widgets.text("schema", "governance", "Schema Name")
dbutils.widgets.dropdown("dry_run", "false", ["true", "false"], "Dry Run Mode")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
dry_run = dbutils.widgets.get("dry_run").lower() == "true"

print(f"Catalog: {catalog}")
print(f"Schema: {schema}")
print(f"Dry Run: {dry_run}")

if dry_run:
    print("\n" + "="*80)
    print("DRY RUN MODE - No notifications will be sent")
    print("="*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Actions Needing Notification

# COMMAND ----------

control_actions_table = f"{catalog}.{schema}.governance_control_actions"

# Load actions that need notification
actions_to_notify_df = spark.sql(f"""
    SELECT 
        action_id,
        violation_id,
        workspace_id,
        action_type,
        object_id,
        object_type,
        object_name,
        violator_email,
        remediation_status,
        remediation_details,
        error_message,
        completed_at,
        created_at
    FROM {control_actions_table}
    WHERE notification_sent = false
    AND remediation_status IN ('SUCCESS', 'FAILED')
    AND completed_at IS NOT NULL
    ORDER BY completed_at ASC
    LIMIT 100
""")

notification_count = actions_to_notify_df.count()
print(f"Found {notification_count} actions needing notification")

if notification_count == 0:
    print("No actions need notification. Exiting.")
    dbutils.notebook.exit('{"status": "SUCCESS", "notifications_sent": 0}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Workspace Notification Settings

# COMMAND ----------

# Load notification settings from workspace config
workspace_configs_df = spark.sql(f"""
    SELECT 
        workspace_id,
        workspace_name,
        notification_email,
        notification_slack_webhook
    FROM {catalog}.{schema}.governance_config_workspaces
    WHERE enforcement_enabled = true
""")

workspace_configs = {}
for row in workspace_configs_df.collect():
    workspace_configs[row.workspace_id] = {
        'workspace_name': row.workspace_name,
        'notification_email': row.notification_email,
        'notification_slack_webhook': row.notification_slack_webhook
    }

print(f"Loaded notification settings for {len(workspace_configs)} workspace(s)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notification Functions

# COMMAND ----------

import requests
from typing import Dict, Any, Tuple, Optional

def send_email_notification(email: str, subject: str, body: str) -> Tuple[bool, Optional[str]]:
    """
    Send email notification.
    Note: This is a placeholder. In production, integrate with:
    - Databricks Jobs email_notifications
    - SendGrid, AWS SES, or Azure Communication Services
    """
    try:
        # Placeholder for actual email sending
        print(f"  Would send email to {email}")
        print(f"  Subject: {subject}")
        return (True, None)
    except Exception as e:
        return (False, str(e))

def send_slack_notification(webhook_url: str, message: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    """Send Slack notification via webhook."""
    try:
        response = requests.post(
            webhook_url,
            json=message,
            headers={'Content-Type': 'application/json'},
            timeout=10
        )
        
        if response.status_code == 200:
            return (True, None)
        else:
            return (False, f"Slack API returned {response.status_code}: {response.text}")
    except Exception as e:
        return (False, str(e))

# COMMAND ----------

def format_email_notification(action: Dict[str, Any], workspace_name: str) -> Dict[str, str]:
    """Format email notification message."""
    status = action['remediation_status']
    status_emoji = "✓" if status == 'SUCCESS' else "✗"
    
    subject = f"[Governance] {status_emoji} {action['action_type']} - {action['object_type']}"
    
    body = f"""
    <html>
    <body>
        <h2>Governance Remediation Action</h2>
        
        <p><strong>Status:</strong> {status}</p>
        
        <h3>Details:</h3>
        <ul>
            <li><strong>Workspace:</strong> {workspace_name}</li>
            <li><strong>Object Type:</strong> {action['object_type']}</li>
            <li><strong>Object Name:</strong> {action['object_name']}</li>
            <li><strong>Action Taken:</strong> {action['action_type']}</li>
            <li><strong>Your Email:</strong> {action['violator_email']}</li>
        </ul>
        
        <h3>Reason:</h3>
        <p>The resource was not pre-approved and you are not in the authorized identities list.</p>
        
        <h3>Result:</h3>
        <p>{action.get('remediation_details', 'Action completed')}</p>
        
        {f"<h3>Error:</h3><p style='color: red;'>{action['error_message']}</p>" if action.get('error_message') else ""}
        
        <hr>
        <p><em>Action ID: {action['action_id']}</em></p>
        <p><em>Time: {action['completed_at']}</em></p>
    </body>
    </html>
    """
    
    return {'subject': subject, 'body': body}

# COMMAND ----------

def format_slack_message(action: Dict[str, Any], workspace_name: str) -> Dict[str, Any]:
    """Format Slack notification message."""
    status = action['remediation_status']
    color = '#36a64f' if status == 'SUCCESS' else '#ff0000'
    status_emoji = ':white_check_mark:' if status == 'SUCCESS' else ':x:'
    
    message = {
        'text': f"Governance Alert: {action['action_type']}",
        'attachments': [{
            'color': color,
            'title': f"{status_emoji} {action['action_type']} - {action['object_type']}",
            'fields': [
                {
                    'title': 'Status',
                    'value': status,
                    'short': True
                },
                {
                    'title': 'Workspace',
                    'value': workspace_name,
                    'short': True
                },
                {
                    'title': 'Object',
                    'value': f"{action['object_type']}: {action['object_name']}",
                    'short': False
                },
                {
                    'title': 'User',
                    'value': action['violator_email'],
                    'short': True
                },
                {
                    'title': 'Result',
                    'value': action.get('remediation_details', 'Completed'),
                    'short': False
                }
            ],
            'footer': f"Action ID: {action['action_id']}",
            'ts': int(action['completed_at'].timestamp()) if action['completed_at'] else None
        }]
    }
    
    if action.get('error_message'):
        message['attachments'][0]['fields'].append({
            'title': 'Error',
            'value': action['error_message'],
            'short': False
        })
    
    return message

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process Notifications

# COMMAND ----------

from datetime import datetime

actions_list = actions_to_notify_df.collect()
notifications_sent = []
email_count = 0
slack_count = 0
failed_count = 0

print(f"\nProcessing {len(actions_list)} notifications...")
print("="*80)

for action_row in actions_list:
    action = action_row.asDict()
    workspace_id = action['workspace_id']
    
    # Get workspace config
    workspace_config = workspace_configs.get(workspace_id, {})
    workspace_name = workspace_config.get('workspace_name', workspace_id)
    
    notification_success = False
    
    # Send email to violator
    violator_email = action['violator_email']
    if violator_email and '@' in violator_email:
        notification_data = format_email_notification(action, workspace_name)
        
        if not dry_run:
            success, error = send_email_notification(
                violator_email,
                notification_data['subject'],
                notification_data['body']
            )
            if success:
                print(f"✓ Sent email to {violator_email} for {action['object_type']}:{action['object_name']}")
                email_count += 1
                notification_success = True
            else:
                print(f"✗ Failed to send email to {violator_email}: {error}")
                failed_count += 1
        else:
            print(f"DRY RUN: Would send email to {violator_email}")
            email_count += 1
            notification_success = True
    
    # Send notification to admin email
    admin_email = workspace_config.get('notification_email')
    if admin_email and admin_email != violator_email:
        notification_data = format_email_notification(action, workspace_name)
        
        if not dry_run:
            success, error = send_email_notification(
                admin_email,
                notification_data['subject'],
                notification_data['body']
            )
            if success:
                print(f"✓ Sent email to admin {admin_email}")
                notification_success = True
            else:
                print(f"✗ Failed to send email to admin {admin_email}: {error}")
        else:
            print(f"DRY RUN: Would send email to admin {admin_email}")
            notification_success = True
    
    # Send Slack notification
    slack_webhook = workspace_config.get('notification_slack_webhook')
    if slack_webhook:
        message = format_slack_message(action, workspace_name)
        
        if not dry_run:
            success, error = send_slack_notification(slack_webhook, message)
            if success:
                print(f"✓ Sent Slack notification")
                slack_count += 1
                notification_success = True
            else:
                print(f"✗ Failed to send Slack notification: {error}")
                failed_count += 1
        else:
            print(f"DRY RUN: Would send Slack notification")
            slack_count += 1
            notification_success = True
    
    if notification_success:
        notifications_sent.append(action['action_id'])
    
    print("-" * 40)

print("="*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update Notification Status

# COMMAND ----------

if not dry_run and notifications_sent:
    action_ids_str = "', '".join(notifications_sent)
    spark.sql(f"""
        UPDATE {control_actions_table}
        SET notification_sent = true,
            updated_at = current_timestamp()
        WHERE action_id IN ('{action_ids_str}')
    """)
    print(f"✓ Updated {len(notifications_sent)} actions as notified")
elif dry_run:
    print(f"DRY RUN: Would mark {len(notifications_sent)} actions as notified")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notification Summary

# COMMAND ----------

print("\n" + "="*80)
print("NOTIFICATION SUMMARY")
print("="*80)
print(f"Total Actions:         {len(actions_list)}")
print(f"Emails Sent:           {email_count}")
print(f"Slack Messages Sent:   {slack_count}")
print(f"Failed:                {failed_count}")
print(f"Successfully Notified: {len(notifications_sent)}")
print("="*80)

# Show recent notifications
recent_notifications_df = spark.sql(f"""
    SELECT 
        action_id,
        workspace_id,
        action_type,
        object_type,
        object_name,
        violator_email,
        remediation_status,
        notification_sent,
        completed_at
    FROM {control_actions_table}
    WHERE completed_at >= current_timestamp() - INTERVAL 1 HOUR
    ORDER BY completed_at DESC
    LIMIT 20
""")

print("\nRecent Notifications:")
display(recent_notifications_df)

# COMMAND ----------

# Return status
import json
from datetime import datetime

dbutils.notebook.exit(json.dumps({
    'status': 'SUCCESS',
    'notifications_sent': len(notifications_sent),
    'email_count': email_count,
    'slack_count': slack_count,
    'failed_count': failed_count,
    'timestamp': datetime.utcnow().isoformat()
}))

