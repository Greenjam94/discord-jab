"""Torn API commands for Discord bot."""

import discord
from discord.ext import commands
from torn_api import TornAPIClient, TornKeyManager
from torn_api.client import TornAPIError
from typing import Optional


def setup(bot: commands.Bot):
    """Setup function to register Torn API commands."""
    
    # Initialize key manager (shared instance)
    key_manager = TornKeyManager()

    # API Key management commands
    @bot.tree.command(name="torn-key-add", description="Register a new Torn API key (Admin only)")
    @discord.app_commands.describe(
        key_alias="A name/alias for this key",
        env_var_name="Environment variable name containing the key",
        owner="Discord user ID (leave empty for personal key, or 'shared' for shared key)"
    )
    async def torn_key_add(
        interaction: discord.Interaction,
        key_alias: str,
        env_var_name: str,
        owner: Optional[str] = None
    ):
        """Add a new Torn API key to the registry."""
        # Check admin permission
        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message(
                "❌ You need administrator permissions to add API keys.",
                ephemeral=True
            )
            return
        
        await interaction.response.defer(ephemeral=True)
        
        try:
            # Determine owner
            if owner is None:
                owner = str(interaction.user.id)
            
            key_type = "shared" if owner == "shared" else "user"
            
            # Add key
            key_manager.add_key(key_alias, env_var_name, owner, key_type)
            
            # Validate key
            validation = await key_manager.validate_key(key_alias)
            
            if validation.get("valid"):
                embed = discord.Embed(
                    title="✅ Key Added Successfully",
                    color=discord.Color.green()
                )
                embed.add_field(name="Alias", value=key_alias, inline=True)
                embed.add_field(name="Owner", value=owner, inline=True)
                embed.add_field(name="Access Level", value=validation.get("access_level", "Unknown"), inline=True)
                embed.add_field(
                    name="Permission categories",
                    value=", ".join(list(validation.get("permissions", {}).keys())[:15]) or "None",
                    inline=True
                )
                
                await interaction.followup.send(embed=embed, ephemeral=True)
            else:
                await interaction.followup.send(
                    f"⚠️ Key added but validation failed: {validation.get('error', 'Unknown error')}\n"
                    f"Use `/torn-key-validate {key_alias}` to retry validation.",
                    ephemeral=True
                )
                
        except ValueError as e:
            await interaction.followup.send(
                f"❌ Error: {str(e)}",
                ephemeral=True
            )
        except Exception as e:
            await interaction.followup.send(
                f"❌ Unexpected error: {str(e)}",
                ephemeral=True
            )
    
    @bot.tree.command(name="torn-key-remove", description="Remove a Torn API key")
    @discord.app_commands.describe(
        key_alias="The alias of the key to remove"
    )
    async def torn_key_remove(interaction: discord.Interaction, key_alias: str):
        """Remove a Torn API key from the registry."""
        await interaction.response.defer(ephemeral=True)
        
        try:
            key_meta = key_manager.get_key_metadata(key_alias)
            if not key_meta:
                await interaction.followup.send(
                    f"❌ Key alias '{key_alias}' not found.",
                    ephemeral=True
                )
                return
            
            # Check permissions: owner or admin
            is_owner = key_meta["owner"] == str(interaction.user.id)
            is_admin = interaction.user.guild_permissions.administrator
            
            if not (is_owner or is_admin):
                await interaction.followup.send(
                    "❌ You can only remove your own keys or you need administrator permissions.",
                    ephemeral=True
                )
                return
            
            key_manager.remove_key(key_alias)
            await interaction.followup.send(
                f"✅ Key '{key_alias}' has been removed.",
                ephemeral=True
            )
            
        except ValueError as e:
            await interaction.followup.send(
                f"❌ Error: {str(e)}",
                ephemeral=True
            )
        except Exception as e:
            await interaction.followup.send(
                f"❌ Unexpected error: {str(e)}",
                ephemeral=True
            )
    
    @bot.tree.command(name="torn-key-list", description="List all Torn API keys you have access to")
    async def torn_key_list(interaction: discord.Interaction):
        """List all keys accessible to the user."""
        await interaction.response.defer(ephemeral=True)
        
        try:
            keys = key_manager.list_keys_for_user(str(interaction.user.id))
            
            if not keys:
                await interaction.followup.send(
                    "No API keys found. Use `/torn-key-add` to register a key.",
                    ephemeral=True
                )
                return
            
            embed = discord.Embed(
                title="Your Torn API Keys",
                color=discord.Color.blue()
            )
            
            for key in keys:
                key_type_emoji = "🔑" if key["key_type"] == "user" else "👥"
                value = (
                    f"**Type:** {key['key_type']}\n"
                    f"**Access Level:** {key['access_level']}\n"
                    f"**Last Validated:** {key['last_validated'] or 'Never'}\n"
                    f"**Key:** {key['masked_key']}"
                )
                embed.add_field(
                    name=f"{key_type_emoji} {key['alias']}",
                    value=value,
                    inline=False
                )
            
            await interaction.followup.send(embed=embed, ephemeral=True)
            
        except Exception as e:
            await interaction.followup.send(
                f"❌ Error: {str(e)}",
                ephemeral=True
            )
    
    @bot.tree.command(name="torn-key-check", description="Check a Torn API key's permissions and status")
    @discord.app_commands.describe(
        key_alias="The alias of the key to check"
    )
    async def torn_key_check(interaction: discord.Interaction, key_alias: str):
        """Check key permissions and status."""
        await interaction.response.defer(ephemeral=True)
        
        try:
            key_meta = key_manager.get_key_metadata(key_alias)
            if not key_meta:
                await interaction.followup.send(
                    f"❌ Key alias '{key_alias}' not found.",
                    ephemeral=True
                )
                return
            
            # Check if user has access
            is_owner = key_meta["owner"] == str(interaction.user.id)
            is_shared = key_meta["owner"] == "shared"
            is_admin = interaction.user.guild_permissions.administrator
            
            if not (is_owner or is_shared or is_admin):
                await interaction.followup.send(
                    "❌ You don't have access to this key.",
                    ephemeral=True
                )
                return
            
            key_value = key_manager.get_key_value(key_alias)
            masked_key = key_manager.mask_key(key_value) if key_value else "Not found"
            
            embed = discord.Embed(
                title=f"Key: {key_alias}",
                color=discord.Color.blue()
            )
            embed.add_field(name="Owner", value=key_meta["owner"], inline=True)
            embed.add_field(name="Type", value=key_meta.get("key_type", "user"), inline=True)
            embed.add_field(name="Access Level", value=key_meta.get("access_level", "Unknown"), inline=True)
            embed.add_field(name="Key", value=masked_key, inline=False)
            embed.add_field(
                name="Permission categories",
                value=", ".join(list(key_meta.get("permissions", {}).keys())[:15]) or "None",
                inline=False
            )
            embed.add_field(
                name="Last Validated",
                value=key_meta.get("last_validated", "Never"),
                inline=False
            )
            
            await interaction.followup.send(embed=embed, ephemeral=True)
            
        except Exception as e:
            await interaction.followup.send(
                f"❌ Error: {str(e)}",
                ephemeral=True
            )
    
    @bot.tree.command(name="torn-key-validate", description="Validate and update a Torn API key's permissions")
    @discord.app_commands.describe(
        key_alias="The alias of the key to validate"
    )
    async def torn_key_validate(interaction: discord.Interaction, key_alias: str):
        """Validate a key and update its permissions."""
        await interaction.response.defer(ephemeral=True)
        
        try:
            key_meta = key_manager.get_key_metadata(key_alias)
            if not key_meta:
                await interaction.followup.send(
                    f"❌ Key alias '{key_alias}' not found.",
                    ephemeral=True
                )
                return
            
            # Check if user has access
            is_owner = key_meta["owner"] == str(interaction.user.id)
            is_shared = key_meta["owner"] == "shared"
            is_admin = interaction.user.guild_permissions.administrator
            
            if not (is_owner or is_shared or is_admin):
                await interaction.followup.send(
                    "❌ You don't have access to this key.",
                    ephemeral=True
                )
                return
            
            # Validate key
            validation = await key_manager.validate_key(key_alias)
            
            if validation.get("valid"):
                embed = discord.Embed(
                    title="✅ Key Validated Successfully",
                    color=discord.Color.green()
                )
                embed.add_field(name="Access Level", value=validation.get("access_level", "Unknown"), inline=True)
                embed.add_field(
                    name="Permission categories",
                    value=", ".join(list(key_meta.get("permissions", {}).keys())[:15]) or "None",
                    inline=False
                )
                embed.add_field(
                    name="Last Validated",
                    value=validation.get("last_validated", "Unknown"),
                    inline=False
                )
                
                await interaction.followup.send(embed=embed, ephemeral=True)
            else:
                await interaction.followup.send(
                    f"❌ Validation failed: {validation.get('error', 'Unknown error')}",
                    ephemeral=True
                )
                
        except Exception as e:
            await interaction.followup.send(
                f"❌ Error while validating key: {str(e)}",
                ephemeral=True
            )

    # API call commands to get game data
    
    @bot.tree.command(name="torn-user", description="Get Torn user information")
    @discord.app_commands.describe(
        user_id="Torn user ID (leave empty to use your own key's owner data)"
    )
    async def torn_user(interaction: discord.Interaction, user_id: Optional[int] = None):
        """Get user information from Torn API."""
        await interaction.response.defer(ephemeral=True)
        
        try:
            # Find appropriate key
            key_alias = key_manager.find_key_for_request(
                str(interaction.user.id),
                "user"
            )
            
            if not key_alias:
                await interaction.followup.send(
                    "❌ No API key found with permission to access user data. "
                    "Please register a key using `/torn-key-add`.",
                    ephemeral=True
                )
                return
            
            key_value = key_manager.get_key_value(key_alias)
            if not key_value:
                await interaction.followup.send(
                    f"❌ Key '{key_alias}' not found in environment variables.",
                    ephemeral=True
                )
                return
            
            # Make API request
            client = TornAPIClient()
            try:
                data = await client.get_user(key_value, user_id, selections=["basic", "profile"])
                
                # Format response
                if user_id:
                    user_data = data
                else:
                    user_data = data
                
                name = user_data.get("name", "Unknown")
                level = user_data.get("level", "?")
                status = user_data.get("status", {})
                status_desc = status.get("description", "Unknown")
                status_state = status.get("state", "Unknown")
                
                embed = discord.Embed(
                    title=f"Torn User: {name}",
                    color=discord.Color.blue()
                )
                embed.add_field(name="Level", value=str(level), inline=True)
                embed.add_field(name="Status", value=status_state, inline=True)
                embed.add_field(name="Description", value=status_desc[:100] or "None", inline=False)
                
                if user_id:
                    embed.add_field(name="User ID", value=str(user_id), inline=True)
                
                await interaction.followup.send(embed=embed, ephemeral=True)
                
            except TornAPIError as e:
                await interaction.followup.send(
                    f"❌ API Error: {str(e)}",
                    ephemeral=True
                )
            finally:
                await client.close()
                
        except Exception as e:
            await interaction.followup.send(
                f"❌ Error: {str(e)}",
                ephemeral=True
            )
    
    @bot.tree.command(name="torn-faction", description="Get Torn faction information")
    @discord.app_commands.describe(
        faction_id="Torn faction ID (leave empty to use your key's owner faction)"
    )
    async def torn_faction(interaction: discord.Interaction, faction_id: Optional[int] = None):
        """Get faction information from Torn API."""
        await interaction.response.defer(ephemeral=True)
        
        try:
            # Find appropriate key
            key_alias = key_manager.find_key_for_request(
                str(interaction.user.id),
                "faction"
            )
            
            if not key_alias:
                await interaction.followup.send(
                    "❌ No API key found with permission to access faction data. "
                    "Please register a key using `/torn-key-add`.",
                    ephemeral=True
                )
                return
            
            key_value = key_manager.get_key_value(key_alias)
            if not key_value:
                await interaction.followup.send(
                    f"❌ Key '{key_alias}' not found in environment variables.",
                    ephemeral=True
                )
                return
            
            # Make API request
            client = TornAPIClient()
            try:
                data = await client.get_faction(key_value, faction_id, selections=["basic"])
                
                # Format response
                faction_name = data.get("name", "Unknown")
                faction_id_display = data.get("ID", faction_id or "?")
                respect = data.get("respect", "?")
                age = data.get("age", "?")
                best_chain = data.get("best_chain", "?")
                
                embed = discord.Embed(
                    title=f"Faction: {faction_name}",
                    color=discord.Color.green()
                )
                embed.add_field(name="Faction ID", value=str(faction_id_display), inline=True)
                embed.add_field(name="Respect", value=str(respect), inline=True)
                embed.add_field(name="Age", value=f"{age} days", inline=True)
                embed.add_field(name="Best Chain", value=str(best_chain), inline=True)
                
                await interaction.followup.send(embed=embed, ephemeral=True)
                
            except TornAPIError as e:
                await interaction.followup.send(
                    f"❌ API Error: {str(e)}",
                    ephemeral=True
                )
            finally:
                await client.close()
                
        except Exception as e:
            await interaction.followup.send(
                f"❌ Error: {str(e)}",
                ephemeral=True
            )
