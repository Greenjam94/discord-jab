"""Utility functions for competition commands."""

import discord
import asyncio
from discord.ext import commands
from typing import Optional, Tuple, Dict, Any, List, Set
from torn_api import TornKeyManager, TornAPIClient
from torn_api.client import TornAPIError


# Valid contributor stat names that can be tracked (from YATA BB_BRIDGE)
CONTRIBUTOR_STATS = [
    # Gym training stats
    "gymstrength",
    "gymspeed",
    "gymdefense",
    "gymdexterity",
    "gym_e_spent",  # Synthetic: sum of all 4 gym stats
    # Combat stats
    "attacksdamagehits",
    "attacksdamage",
    "attacksdamaging",
    "attacksrunaway",
    # Crime stats
    "criminaloffences",
    "busts",
    "jails",
    # Medical stats
    "revives",
    "medicalcooldownused",
    "medicalitemrecovery",
    "hosptimereceived",
    "hosptimegiven",
    # Item usage stats
    "drugsused",
    "drugoverdoses",
    "candyused",
    "alcoholused",
    "energydrinkused",
    # Other stats
    "traveltime",
    "hunting",
    "rehabs",
    "caymaninterest"
]


def get_status_emoji(status: str) -> str:
    """Get emoji for competition status.
    
    Args:
        status: Competition status string
        
    Returns:
        Emoji string
    """
    emoji_map = {
        "active": "🟢",
        "cancelled": "🔴",
        "completed": "⚪"
    }
    return emoji_map.get(status.lower(), "⚫")


async def check_database_available(bot: commands.Bot, interaction: discord.Interaction) -> bool:
    """Check if database is available and send error if not.
    
    Args:
        bot: Bot instance
        interaction: Discord interaction
        
    Returns:
        True if database is available, False otherwise
    """
    if not hasattr(bot, 'database') or not bot.database:
        await interaction.followup.send("❌ Database not available.", ephemeral=True)
        return False
    return True


async def validate_competition_exists(bot: commands.Bot, competition_id: int, interaction: discord.Interaction) -> Optional[Dict[str, Any]]:
    """Validate that a competition exists and return it.
    
    Args:
        bot: Bot instance
        competition_id: Competition ID
        interaction: Discord interaction (already deferred)
        
    Returns:
        Competition dict if found, None otherwise (error already sent)
    """
    comp = await bot.database.get_competition(competition_id)
    if not comp:
        await interaction.followup.send(
            f"❌ Competition with ID {competition_id} not found.",
            ephemeral=True
        )
        return None
    return comp


async def require_admin(interaction: discord.Interaction) -> bool:
    """Check if user has admin permissions.
    
    Args:
        interaction: Discord interaction
        
    Returns:
        True if admin, False otherwise (error already sent)
    """
    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message(
            "❌ You need administrator permissions to perform this action.",
            ephemeral=True
        )
        return False
    return True


async def find_faction_api_key(
    key_manager: TornKeyManager,
    faction_id: Optional[int] = None,
    client: Optional[TornAPIClient] = None
) -> Optional[Tuple[str, str]]:
    """Find an API key with faction permission, optionally from a specific faction.
    
    Args:
        key_manager: TornKeyManager instance
        faction_id: Optional faction ID to match key owner's faction
        client: Optional TornAPIClient instance (required if faction_id provided)
        
    Returns:
        Tuple of (key_alias, key_value) if found, None otherwise
    """
    for key_alias, key_meta in key_manager.metadata.get("keys", {}).items():
        if key_manager.has_permission(key_alias, "faction"):
            key_value = key_manager.get_key_value(key_alias)
            if key_value:
                if faction_id is None:
                    return (key_alias, key_value)
                
                # Check if key owner is in the requested faction
                if client:
                    try:
                        owner_data = await client.get_user(key_value, None, selections=["basic"])
                        owner_faction_id = owner_data.get("faction", {}).get("faction_id") if isinstance(owner_data.get("faction"), dict) else owner_data.get("faction")
                        if owner_faction_id == faction_id:
                            return (key_alias, key_value)
                    except Exception:
                        # Continue to next key if we can't check
                        continue
    
    # If no matching key found, return any key with faction permission
    if faction_id is not None:
        for key_alias, key_meta in key_manager.metadata.get("keys", {}).items():
            if key_manager.has_permission(key_alias, "faction"):
                key_value = key_manager.get_key_value(key_alias)
                if key_value:
                    return (key_alias, key_value)
    
    return None


async def process_faction_members(
    bot: commands.Bot,
    members: Dict[str, Any],
    faction_id: int
) -> None:
    """Process faction members dict and upsert players to database.
    
    Args:
        bot: Bot instance
        members: Faction members dict from API (member_id -> member_data)
        faction_id: Faction ID
    """
    if not isinstance(members, dict):
        return
    
    for member_id_str, member_data in members.items():
        try:
            member_id = int(member_id_str)
            member_name = member_data.get("name", "Unknown")
            member_level = member_data.get("level")
            
            await bot.database.upsert_player(
                player_id=member_id,
                name=member_name,
                level=member_level if isinstance(member_level, int) else None,
                faction_id=faction_id
            )
        except (ValueError, KeyError, TypeError) as e:
            print(f"Warning: Could not process member {member_id_str}: {e}")
            continue


def format_number_with_sign(value: Optional[float]) -> str:
    """Format a number with + sign if positive, or as-is if negative/zero.
    
    Args:
        value: Number to format
        
    Returns:
        Formatted string (e.g., "+1,000" or "-500" or "N/A")
    """
    if value is None:
        return "N/A"
    if value >= 0:
        return f"+{value:,.0f}"
    return f"{value:,.0f}"


async def get_all_faction_keys(
    key_manager: TornKeyManager,
    client: TornAPIClient
) -> Tuple[List[Tuple[str, str]], Dict[str, int]]:
    """Get all API keys with faction permission and their owner factions.
    
    Args:
        key_manager: TornKeyManager instance
        client: TornAPIClient instance
        
    Returns:
        Tuple of (list of (key_alias, key_value) tuples, dict of key_alias -> faction_id)
    """
    all_keys = []
    key_owner_factions = {}
    
    try:
        for key_alias, key_meta in key_manager.metadata.get("keys", {}).items():
            if key_manager.has_permission(key_alias, "faction"):
                key_value = key_manager.get_key_value(key_alias)
                if key_value:
                    all_keys.append((key_alias, key_value))
                    
                    # Get key owner's faction_id for matching
                    try:
                        owner_data = await client.get_user(key_value, None, selections=["basic"])
                        owner_faction_id = owner_data.get("faction", {}).get("faction_id") if isinstance(owner_data.get("faction"), dict) else owner_data.get("faction")
                        if owner_faction_id:
                            key_owner_factions[key_alias] = owner_faction_id
                        await asyncio.sleep(0.5)  # Small delay
                    except Exception as e:
                        print(f"Warning: Could not determine faction for key {key_alias}: {e}")
                        # Continue without faction info for this key
    except Exception as e:
        print(f"Warning: Error building key list: {e}")
    
    return all_keys, key_owner_factions


def find_best_key_for_faction(
    all_keys: List[Tuple[str, str]],
    key_manager: TornKeyManager,
    faction_id: int,
    key_owner_factions: Dict[str, int]
) -> Optional[Tuple[str, str]]:
    """Find the best API key for a specific faction.
    
    Prefers keys from the same faction, falls back to any key with faction permission.
    
    Args:
        all_keys: List of (key_alias, key_value) tuples
        key_manager: TornKeyManager instance
        faction_id: Target faction ID
        key_owner_factions: Map of key_alias -> faction_id
        
    Returns:
        Tuple of (key_alias, key_value) if found, None otherwise
    """
    # First try to find a key from the same faction
    for key_alias, key_value in all_keys:
        if key_manager.has_permission(key_alias, "faction"):
            key_owner_faction = key_owner_factions.get(key_alias)
            if key_owner_faction == faction_id:
                return (key_alias, key_value)
    
    # Fallback to any key with faction permission
    for key_alias, key_value in all_keys:
        if key_manager.has_permission(key_alias, "faction"):
            return (key_alias, key_value)
    
    return None
