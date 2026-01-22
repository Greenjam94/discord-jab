"""Torn API commands for Discord bot."""

import discord
from discord.ext import commands
from torn_api import TornAPIClient, TornKeyManager
from torn_api.client import TornAPIError
from typing import Optional
from utils.permissions import check_command_permission, get_bot_database


def setup(bot: commands.Bot):
    """Setup function to register Torn API commands."""
    
    # Initialize key manager (shared instance)
    key_manager = TornKeyManager()

    # API Key management commands
    @bot.tree.command(name="torn-key-add", description="Register a new Torn API key (Admin only)")
    @discord.app_commands.describe(
        key_alias="A name/alias for this key",
        api_key="The Torn API key to store",
        owner="Discord user ID (leave empty for personal key, or 'shared' for shared key)"
    )
    async def torn_key_add(
        interaction: discord.Interaction,
        key_alias: str,
        api_key: str,
        owner: Optional[str] = None
    ):
        """Add a new Torn API key to the registry."""
        # Check database permissions first
        db = get_bot_database(bot)
        has_permission, error_msg = await check_command_permission(interaction, "torn-key-add", db)
        if not has_permission:
            await interaction.response.send_message(error_msg or "You don't have permission to use this command.", ephemeral=True)
            return
        
        # Legacy check: if no DB permissions set, fall back to Discord permissions
        if db and db.connection is not None:
            try:
                perms = await db.get_command_permissions(str(interaction.guild.id))
                if not perms.get("torn-key-add"):  # No custom permissions set, use Discord default
                    if not interaction.user.guild_permissions.administrator:
                        await interaction.response.send_message(
                            "❌ You need administrator permissions to add API keys.",
                            ephemeral=True
                        )
                        return
            except Exception as e:
                # If database query fails, fall back to Discord permissions
                print(f"⚠️  Error checking permissions: {e}")
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
            key_manager.add_key(key_alias, api_key, owner, key_type)
            
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
                    f"❌ Key '{key_alias}' not found.",
                    ephemeral=True
                )
                return
            
            # Get masked key for data source tracking
            masked_key = key_manager.mask_key(key_value) if key_value else None
            
            # Make API request
            client = TornAPIClient()
            try:
                data = await client.get_user(key_value, user_id, selections=["basic", "profile", "battlestats", "networth"])
                
                player_id = data.get("player_id") or user_id
                name = data.get("name", "Unknown")
                level = data.get("level", "Unknown")
                rank = data.get("rank", "Unknown")
                current_life = data.get("life", {}).get("current", -1)
                max_life = data.get("life", {}).get("maximum", -1)
                life = (
                    f"{round((current_life / max_life) * 100)}%"
                    if current_life >= 0 and max_life > 0
                    else "N/A"
                )
                status = data.get("status", {})
                status_desc = status.get("description", "Unknown")
                status_state = status.get("state", "Unknown")
                
                # Extract faction ID if available
                faction_id = data.get("faction", {}).get("faction_id") if isinstance(data.get("faction"), dict) else data.get("faction")
                
                # Save to database if available
                if hasattr(bot, 'database') and bot.database and player_id:
                    try:
                        # Upsert player current state
                        await bot.database.upsert_player(
                            player_id=player_id,
                            name=name,
                            level=level if isinstance(level, int) else None,
                            rank=rank if isinstance(rank, str) else None,
                            faction_id=faction_id if isinstance(faction_id, int) else None,
                            status_state=status_state if isinstance(status_state, str) else None,
                            status_description=status_desc if isinstance(status_desc, str) else None,
                            life_current=current_life if current_life >= 0 else None,
                            life_maximum=max_life if max_life > 0 else None
                        )
                        
                        # Extract stats for history
                        battlestats = data.get("battlestats", {})
                        networth_data = data.get("networth", {})
                        
                        # Append to history if we have stat data
                        if battlestats or networth_data or level:
                            await bot.database.append_player_stats(
                                player_id=player_id,
                                strength=battlestats.get("strength"),
                                defense=battlestats.get("defense"),
                                speed=battlestats.get("speed"),
                                dexterity=battlestats.get("dexterity"),
                                total_stats=battlestats.get("total"),
                                level=level if isinstance(level, int) else None,
                                life_maximum=max_life if max_life > 0 else None,
                                networth=networth_data.get("total") if isinstance(networth_data, dict) else (networth_data if isinstance(networth_data, (int, float)) else None),
                                data_source=masked_key
                            )
                    except Exception as db_error:
                        # Log detailed error but don't fail the command if database save fails
                        import traceback
                        error_details = traceback.format_exc()
                        print(f"Warning: Failed to save user data to database: {db_error}")
                        print(f"Error details: {error_details}")
                
                embed = discord.Embed(
                    title=f"User: {name} [{player_id}]",
                    color=discord.Color.blue()
                )
                embed.add_field(name="Level", value=str(level), inline=True)
                embed.add_field(name="Rank", value=rank, inline=True)
                embed.add_field(name="Status", value=status_state, inline=True)
                embed.add_field(name="Life", value=life, inline=True)
                if status_desc != status_state:
                    embed.add_field(name="Description", value=status_desc[:100] or "None", inline=False)
                
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
                    f"❌ Key '{key_alias}' not found.",
                    ephemeral=True
                )
                return
            
            # Get masked key for data source tracking
            masked_key = key_manager.mask_key(key_value) if key_value else None
            
            # Make API request
            client = TornAPIClient()
            try:
                data = await client.get_faction(key_value, faction_id, selections=["basic"])
                
                # Format response
                faction_id_display = data.get("ID") or faction_id
                faction_name = data.get("name", "Unknown")
                tag = data.get("tag", "?")
                respect = data.get("respect", "?")
                age = data.get("age", "?")
                best_chain = data.get("best_chain", "?")
                members = data.get("members", {})
                members_count = len(members) if isinstance(members, dict) else 0
                leader = data.get("leader", "?")
                coleader = data.get("co-leader", "?")
                
                # Save to database if available
                if hasattr(bot, 'database') and bot.database and faction_id_display:
                    try:
                        # Upsert faction current state
                        await bot.database.upsert_faction(
                            faction_id=faction_id_display,
                            name=faction_name,
                            tag=tag if isinstance(tag, str) else None,
                            leader_id=leader if isinstance(leader, int) else None,
                            co_leader_id=coleader if isinstance(coleader, int) else None,
                            respect=respect if isinstance(respect, int) else None,
                            age=age if isinstance(age, int) else None,
                            best_chain=best_chain if isinstance(best_chain, int) else None,
                            member_count=members_count
                        )
                        
                        # Append to history
                        await bot.database.append_faction_history(
                            faction_id=faction_id_display,
                            respect=respect if isinstance(respect, int) else None,
                            member_count=members_count,
                            best_chain=best_chain if isinstance(best_chain, int) else None,
                            data_source=masked_key
                        )
                    except Exception as db_error:
                        # Log but don't fail the command if database save fails
                        print(f"Warning: Failed to save faction data to database: {db_error}")
                
                embed = discord.Embed(
                    title=f"Faction: {faction_name} [{tag}]",
                    color=discord.Color.green()
                )
                embed.add_field(name="Faction ID", value=str(faction_id_display), inline=True)
                embed.add_field(
                    name="Leader",
                    value=f"[View Profile](https://www.torn.com/profiles.php?XID={leader})",
                    inline=True
                )

                embed.add_field(
                    name="Co-Leader",
                    value=f"[View Profile](https://www.torn.com/profiles.php?XID={coleader})",
                    inline=True
                )
                embed.add_field(name="Respect", value=str(respect), inline=True)
                embed.add_field(name="Age", value=f"{age} days", inline=True)
                embed.add_field(name="Best Chain", value=str(best_chain), inline=True)
                embed.add_field(name="Members", value=str(members_count), inline=True)
                
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
