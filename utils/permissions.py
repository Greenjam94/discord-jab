"""Permission checking utilities for Discord bot commands."""

import discord
from typing import Optional, Tuple, List
from database import TornDatabase


async def check_command_permission(
    interaction: discord.Interaction,
    command_name: str,
    database: Optional[TornDatabase] = None
) -> Tuple[bool, Optional[str]]:
    """
    Check if a user has permission to use a command based on database permissions.
    
    Args:
        interaction: The Discord interaction object
        command_name: Name of the command to check
        database: Optional database instance (will create one if not provided)
    
    Returns:
        Tuple of (has_permission: bool, error_message: Optional[str])
        - If has_permission is True, the user can use the command
        - If has_permission is False, error_message contains the reason
    """
    # Can't check permissions in DMs
    if not interaction.guild:
        return False, "This command can only be used in a server."
    
    # If no database, allow the command (fallback to default behavior)
    if not database:
        return True, None
    
    guild_id = str(interaction.guild.id)
    user = interaction.user
    
    try:
        # Get permissions for this command in this guild
        permissions = await database.get_command_permissions(guild_id)
        command_perms = permissions.get(command_name, [])
        
        # If no permissions are set, allow everyone (public command)
        if not command_perms:
            return True, None
        
        # Check each permission type
        for perm in command_perms:
            perm_type = perm['type']
            perm_value = perm['value']
            
            if perm_type == 'admin':
                # Check if user is a server administrator
                if user.guild_permissions.administrator:
                    return True, None
            
            elif perm_type == 'role':
                # Check if user has this role
                try:
                    role_id = int(perm_value)
                    role = interaction.guild.get_role(role_id)
                    if role and role in user.roles:
                        return True, None
                except (ValueError, TypeError):
                    # Invalid role ID, skip
                    continue
            
            elif perm_type == 'user':
                # Check if this is the specific user
                try:
                    user_id = int(perm_value)
                    if user.id == user_id:
                        return True, None
                except (ValueError, TypeError):
                    # Invalid user ID, skip
                    continue
        
        # No matching permissions found
        return False, f"You don't have permission to use `/{command_name}`. Contact a server administrator if you believe this is an error."
    
    except Exception as e:
        # On error, allow the command (fail open) but log the error
        print(f"⚠️  Error checking permission for {command_name}: {e}")
        return True, None


async def require_command_permission(
    interaction: discord.Interaction,
    command_name: str,
    database: Optional[TornDatabase] = None
) -> bool:
    """
    Check permission and send error message if not allowed.
    This is a convenience function that handles the response.
    
    Args:
        interaction: The Discord interaction object
        command_name: Name of the command to check
        database: Optional database instance
    
    Returns:
        True if permission granted (and already responded), False if denied (and already responded)
    """
    has_permission, error_message = await check_command_permission(
        interaction, command_name, database
    )
    
    if not has_permission:
        await interaction.response.send_message(
            error_message or "You don't have permission to use this command.",
            ephemeral=True
        )
        return False
    
    return True


def get_bot_database(bot) -> Optional[TornDatabase]:
    """Get the database instance from the bot."""
    if hasattr(bot, 'database'):
        return bot.database
    return None
