"""
Runtime abstraction: secret access only. No dbutils in package.
Notebook passes get_secret that calls dbutils.secrets.get(scope=..., key=...).
"""
from typing import Protocol


class GetSecret(Protocol):
    """Protocol for secret lookup. Implement with lambda s, k: dbutils.secrets.get(scope=s, key=k) in Databricks."""

    def __call__(self, scope: str, key: str) -> str:
        ...
