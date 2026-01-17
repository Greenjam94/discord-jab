"""Key manager for Torn API keys with permission tracking."""

import os
import json
from typing import Dict, Any, Optional, List
from datetime import datetime
from pathlib import Path
from .client import TornAPIClient, TornAPIError


class TornKeyManager:
    """Manages Torn API keys and their permissions."""
    
    def __init__(self, metadata_file: str = "data/torn_keys.json"):
        """Initialize the key manager."""
        self.metadata_file = Path(metadata_file)
        self.metadata_file.parent.mkdir(parents=True, exist_ok=True)
        self.client = TornAPIClient()
        self.metadata: Dict[str, Any] = self._load_metadata()
    
    def _load_metadata(self) -> Dict[str, Any]:
        """Load key metadata from JSON file."""
        if not self.metadata_file.exists():
            return {"keys": {}}
        
        try:
            with open(self.metadata_file, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return {"keys": {}}
    
    def _save_metadata(self):
        """Save key metadata to JSON file."""
        try:
            with open(self.metadata_file, 'w') as f:
                json.dump(self.metadata, f, indent=2)
        except IOError as e:
            raise Exception(f"Failed to save key metadata: {e}")
    
    def mask_key(self, key: str) -> str:
        """Mask a key for display (show only last 4 characters)."""
        if not key or len(key) <= 4:
            return "****"
        return f"****-****-****-{key[-4:]}"
    
    def get_key_value(self, key_alias: str) -> Optional[str]:
        """Get the actual key value from environment variable."""
        if key_alias not in self.metadata.get("keys", {}):
            return None
        
        env_var = self.metadata["keys"][key_alias].get("env_var")
        if not env_var:
            return None
        
        return os.getenv(env_var)
    
    def add_key(
        self,
        key_alias: str,
        env_var: str,
        owner: str,
        key_type: str = "user"
    ) -> Dict[str, Any]:
        """Add a new key to the registry."""
        if key_alias in self.metadata.get("keys", {}):
            raise ValueError(f"Key alias '{key_alias}' already exists")
        
        key_value = os.getenv(env_var)
        if not key_value:
            raise ValueError(f"Environment variable '{env_var}' not found")
        
        # Initialize metadata
        self.metadata.setdefault("keys", {})[key_alias] = {
            "owner": owner,
            "env_var": env_var,
            "access_level": "Unknown",
            "permissions": [],
            "last_validated": None,
            "key_type": key_type
        }
        
        self._save_metadata()
        return self.metadata["keys"][key_alias]
    
    async def validate_key(self, key_alias: str) -> Dict[str, Any]:
        """Validate a key and update its permissions."""
        if key_alias not in self.metadata.get("keys", {}):
            raise ValueError(f"Key alias '{key_alias}' not found")
        
        key_value = self.get_key_value(key_alias)
        if not key_value:
            raise ValueError(f"Key value not found for alias '{key_alias}'")
        
        try:
            key_info = await self.client.get_key_info(key_value)
            
            # Update metadata
            key_meta = self.metadata["keys"][key_alias]
            key_meta["access_level"] = key_info.get("access_level", "Unknown")
            key_meta["permissions"] = key_info.get("selections", [])
            key_meta["last_validated"] = datetime.utcnow().isoformat()
            
            self._save_metadata()
            
            return {
                "valid": True,
                "access_level": key_meta["access_level"],
                "permissions": key_meta["permissions"],
                "last_validated": key_meta["last_validated"]
            }
        except TornAPIError as e:
            return {
                "valid": False,
                "error": str(e)
            }
    
    def remove_key(self, key_alias: str):
        """Remove a key from the registry."""
        if key_alias not in self.metadata.get("keys", {}):
            raise ValueError(f"Key alias '{key_alias}' not found")
        
        del self.metadata["keys"][key_alias]
        self._save_metadata()
    
    def get_key_metadata(self, key_alias: str) -> Optional[Dict[str, Any]]:
        """Get metadata for a key."""
        return self.metadata.get("keys", {}).get(key_alias)
    
    def list_keys_for_user(self, user_id: str) -> List[Dict[str, Any]]:
        """List all keys accessible to a user (their own + shared)."""
        keys = []
        for alias, meta in self.metadata.get("keys", {}).items():
            if meta["owner"] == user_id or meta["owner"] == "shared":
                keys.append({
                    "alias": alias,
                    "owner": meta["owner"],
                    "key_type": meta.get("key_type", "user"),
                    "access_level": meta.get("access_level", "Unknown"),
                    "last_validated": meta.get("last_validated"),
                    "masked_key": self.mask_key(self.get_key_value(alias) or "")
                })
        return keys
    
    def has_permission(
        self,
        key_alias: str,
        required_selection: str
    ) -> bool:
        """Check if a key has permission for a specific selection."""
        key_meta = self.get_key_metadata(key_alias)
        if not key_meta:
            return False
        
        permissions = key_meta.get("permissions", [])
        
        # Full access or wildcard
        if "*" in permissions or key_meta.get("access_level") == "Full Access":
            return True
        
        # Check specific permission
        # Handle nested selections like "user.basic" or "faction.members"
        selection_parts = required_selection.split(".")
        base_selection = selection_parts[0]
        
        # Check if base selection is allowed
        if base_selection in permissions:
            return True
        
        # Check if full selection path is allowed
        if required_selection in permissions:
            return True
        
        return False
    
    def find_key_for_request(
        self,
        user_id: str,
        required_selection: str
    ) -> Optional[str]:
        """Find an appropriate key for a request (user's key first, then shared)."""
        # Try user's keys first
        for alias, meta in self.metadata.get("keys", {}).items():
            if meta["owner"] == user_id:
                if self.has_permission(alias, required_selection):
                    return alias
        
        # Try shared keys
        for alias, meta in self.metadata.get("keys", {}).items():
            if meta["owner"] == "shared":
                if self.has_permission(alias, required_selection):
                    return alias
        
        return None
    
    async def close(self):
        """Close the API client."""
        await self.client.close()
