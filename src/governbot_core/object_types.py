"""
Object-type registry: trackable types, opt-out, and handler registration.
Adding a new object type = registry entry + register handler(s).
Opt-out via enabled_object_types / excluded list (config-driven).
"""
from typing import Any, Callable, Dict, Optional, Set, Tuple


# Delete handler: (client, object_id_or_name, **kwargs) -> (success: bool, error_message: Optional[str])
DeleteHandler = Callable[[Any, str], Tuple[bool, Optional[str]]]

# Registry of object_type -> delete handler (populated by remediation module)
DELETE_HANDLERS: Dict[str, DeleteHandler] = {}

# All object types that can be tracked (discovery, watcher, remediation).
# Matches 06_remediation object types; extend by registering in remediation.py.
SUPPORTED_OBJECT_TYPES: Set[str] = set()


def register_delete_handler(object_type: str, handler: DeleteHandler) -> None:
    """Register a delete handler for an object type."""
    DELETE_HANDLERS[object_type] = handler
    SUPPORTED_OBJECT_TYPES.add(object_type)


def get_delete_handler(object_type: str) -> Optional[DeleteHandler]:
    """Return the delete handler for an object type, or None."""
    return DELETE_HANDLERS.get(object_type)


def is_tracked(object_type: str, enabled_object_types: Optional[Set[str]] = None) -> bool:
    """
    Return True if object_type should be tracked.
    If enabled_object_types is provided, only types in that set are tracked.
    If None, all SUPPORTED_OBJECT_TYPES are tracked.
    """
    if object_type not in SUPPORTED_OBJECT_TYPES:
        return False
    if enabled_object_types is None:
        return True
    return object_type in enabled_object_types


def filter_enabled(
    object_types: Set[str], enabled_object_types: Optional[Set[str]] = None
) -> Set[str]:
    """Return object_types restricted to enabled_object_types when provided."""
    if enabled_object_types is None:
        return object_types
    return object_types & enabled_object_types
