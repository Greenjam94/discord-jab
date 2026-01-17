import discord
from discord.ext import commands
import re
import os
import asyncio
from datetime import datetime, timedelta

def parse_duration(duration_str: str) -> timedelta:
    """Parse duration string like '10m', '1h', '30s' into timedelta"""
    # Match patterns like "10m", "1h", "30s", "2d"
    match = re.match(r'^(\d+)([smhd])$', duration_str.lower())
    if not match:
        raise ValueError("Invalid duration format")
    
    amount = int(match.group(1))
    unit = match.group(2)
    
    if unit == 's':
        return timedelta(seconds=amount)
    elif unit == 'm':
        return timedelta(minutes=amount)
    elif unit == 'h':
        return timedelta(hours=amount)
    elif unit == 'd':
        return timedelta(days=amount)
    else:
        raise ValueError("Invalid duration unit")

def setup(bot: commands.Bot):
    """Setup function to register admin commands"""
    
    @bot.tree.command(name="ping", description="Check if the bot is responding")
    async def ping(interaction: discord.Interaction):
        """Ping command - checks bot responsiveness and latency"""
        latency = round(bot.latency * 1000)  # Convert to milliseconds
        await interaction.response.send_message(
            f"Pong! 🏓\nLatency: {latency}ms",
            ephemeral=True
        )
    
    @bot.tree.command(name="mute", description="Mute a user in the server")
    @discord.app_commands.describe(
        user="The user to mute",
        duration="Duration (e.g., 10m, 1h, 30s, 2d)",
        reason="Reason for muting"
    )
    async def mute(interaction: discord.Interaction, user: discord.Member, duration: str = "10m", reason: str = "No reason provided"):
        """Mute command - mutes a user using timeout"""
        # Check if user has permission
        if not interaction.user.guild_permissions.moderate_members:
            await interaction.response.send_message(
                "You don't have permission to mute members.",
                ephemeral=True
            )
            return
        
        # Check if bot has permission
        if not interaction.guild.me.guild_permissions.moderate_members:
            await interaction.response.send_message(
                "I don't have permission to mute members. Please give me the 'Timeout Members' permission.",
                ephemeral=True
            )
            return
        
        # Can't mute yourself
        if user.id == interaction.user.id:
            await interaction.response.send_message(
                "You can't mute yourself!",
                ephemeral=True
            )
            return
        
        # Can't mute the bot
        if user.id == bot.user.id:
            await interaction.response.send_message(
                "I can't mute myself!",
                ephemeral=True
            )
            return
        
        # Check if target user has higher role
        if interaction.user.top_role <= user.top_role and interaction.user.id != interaction.guild.owner_id:
            await interaction.response.send_message(
                "You can't mute someone with equal or higher permissions than you.",
                ephemeral=True
            )
            return
        
        try:
            # Parse duration
            duration_delta = parse_duration(duration)
            
            # Discord timeout limit is 28 days
            max_duration = timedelta(days=28)
            if duration_delta > max_duration:
                await interaction.response.send_message(
                    "Maximum mute duration is 28 days.",
                    ephemeral=True
                )
                return
            
            # Calculate timeout until time
            timeout_until = datetime.utcnow() + duration_delta
            
            # Apply timeout
            await user.timeout(timeout_until, reason=f"Muted by {interaction.user.display_name}: {reason}")
            
            # Format duration for display
            duration_parts = []
            total_seconds = int(duration_delta.total_seconds())
            if total_seconds >= 86400:
                days = total_seconds // 86400
                duration_parts.append(f"{days}d")
                total_seconds %= 86400
            if total_seconds >= 3600:
                hours = total_seconds // 3600
                duration_parts.append(f"{hours}h")
                total_seconds %= 3600
            if total_seconds >= 60:
                minutes = total_seconds // 60
                duration_parts.append(f"{minutes}m")
                total_seconds %= 60
            if total_seconds > 0:
                duration_parts.append(f"{total_seconds}s")
            
            duration_display = " ".join(duration_parts) if duration_parts else "0s"
            
            await interaction.response.send_message(
                f"🔇 Muted {user.mention} for {duration_display}\nReason: {reason}",
                ephemeral=False
            )
            
            # Try to DM the user
            try:
                await user.send(f"You have been muted in {interaction.guild.name} for {duration_display}.\nReason: {reason}")
            except discord.Forbidden:
                pass  # User has DMs disabled, that's okay
            
        except ValueError as e:
            await interaction.response.send_message(
                f"Invalid duration format: {str(e)}\nUse format like: 10m, 1h, 30s, 2d",
                ephemeral=True
            )
        except discord.Forbidden:
            await interaction.response.send_message(
                "I don't have permission to mute this user. They may have a higher role than me.",
                ephemeral=True
            )
        except Exception as e:
            await interaction.response.send_message(
                f"An error occurred while muting: {str(e)}",
                ephemeral=True
            )
    
    @bot.tree.command(name="warn", description="Warn a user in the server")
    @discord.app_commands.describe(
        user="The user to warn",
        reason="Reason for the warning"
    )
    async def warn(interaction: discord.Interaction, user: discord.Member, reason: str = "No reason provided"):
        """Warn command - warns a user"""
        # Check if user has permission
        if not interaction.user.guild_permissions.moderate_members:
            await interaction.response.send_message(
                "You don't have permission to warn members.",
                ephemeral=True
            )
            return
        
        # Can't warn yourself
        if user.id == interaction.user.id:
            await interaction.response.send_message(
                "You can't warn yourself!",
                ephemeral=True
            )
            return
        
        # Can't warn the bot
        if user.id == bot.user.id:
            await interaction.response.send_message(
                "I can't warn myself!",
                ephemeral=True
            )
            return
        
        # Check if target user has higher role
        if interaction.user.top_role <= user.top_role and interaction.user.id != interaction.guild.owner_id:
            await interaction.response.send_message(
                "You can't warn someone with equal or higher permissions than you.",
                ephemeral=True
            )
            return
        
        # Send warning message
        await interaction.response.send_message(
            f"⚠️ {user.mention} has been warned by {interaction.user.mention}\nReason: {reason}",
            ephemeral=False
        )
        
        # Try to DM the user
        try:
            await user.send(f"You have received a warning in {interaction.guild.name}.\nReason: {reason}")
        except discord.Forbidden:
            pass  # User has DMs disabled, that's okay
    
    async def perform_sync_validation(bot: commands.Bot, force_guild_sync: bool = False):
        """Shared function to perform command sync and validation with comprehensive diagnostics"""
        diagnostic_parts = []
        
        # Get registered commands
        registered_commands = {cmd.name: cmd for cmd in bot.tree.get_commands()}
        registered_names = set(registered_commands.keys())
        
        if not registered_names:
            return {
                "success": False,
                "message": "⚠️ No commands registered in the bot!"
            }
        
        # Determine sync scope
        guild_id = os.getenv('DISCORD_GUILD_ID')
        guild_object = None
        actual_guild = None
        sync_scope = "guild"
        
        # If force_guild_sync is True, try to get guild from interaction context
        if force_guild_sync and hasattr(bot, '_last_interaction_guild'):
            guild_id_int = bot._last_interaction_guild.id
            guild_object = discord.Object(id=guild_id_int)
            actual_guild = bot._last_interaction_guild
            diagnostic_parts.append(f"🔄 Force syncing to guild: {actual_guild.name} (ID: {guild_id_int})")
        elif guild_id:
            try:
                guild_id_int = int(guild_id)
                guild_object = discord.Object(id=guild_id_int)
                
                # Try to get the actual guild object
                actual_guild = bot.get_guild(guild_id_int)
                if actual_guild:
                    diagnostic_parts.append(f"✅ Bot is in guild: {actual_guild.name} (ID: {guild_id})")
                    
                    # Check bot's permissions
                    bot_member = actual_guild.get_member(bot.user.id)
                    if bot_member:
                        perms = bot_member.guild_permissions
                        if perms.administrator:
                            diagnostic_parts.append("✅ Bot has Administrator permission")
                        else:
                            missing_perms = []
                            if not perms.use_application_commands:
                                missing_perms.append("Use Application Commands")
                            if not perms.send_messages:
                                missing_perms.append("Send Messages")
                            if missing_perms:
                                diagnostic_parts.append(f"⚠️ Bot missing permissions: {', '.join(missing_perms)}")
                            else:
                                diagnostic_parts.append("✅ Bot has basic permissions")
                    else:
                        diagnostic_parts.append("⚠️ Bot member not found in guild (may need to rejoin)")
                else:
                    diagnostic_parts.append(f"⚠️ Bot is NOT in guild {guild_id} - commands won't work!")
                    diagnostic_parts.append(f"   Invite bot with: https://discord.com/api/oauth2/authorize?client_id={bot.user.id}&permissions=0&scope=bot%20applications.commands")
            except ValueError:
                guild_object = None
                sync_scope = "global"
                diagnostic_parts.append("⚠️ Invalid DISCORD_GUILD_ID format")
        else:
            sync_scope = "global"
            diagnostic_parts.append("ℹ️ Syncing globally (no guild ID set)")
            diagnostic_parts.append("⚠️ Global syncs can take up to 1 hour to appear!")
            diagnostic_parts.append("   Set DISCORD_GUILD_ID in .env for instant guild commands")
        
        # Check bot application info
        try:
            app_info = await bot.application_info()
            diagnostic_parts.append(f"ℹ️ Bot Application: {app_info.name} (ID: {app_info.id})")
            if app_info.bot_public:
                diagnostic_parts.append("ℹ️ Bot is public")
            else:
                diagnostic_parts.append("ℹ️ Bot is private (only works in authorized servers)")
        except Exception as e:
            diagnostic_parts.append(f"⚠️ Could not fetch application info: {e}")
        
        # Perform sync
        try:
            # If syncing to guild, optionally clear existing commands first to avoid conflicts
            if guild_object and actual_guild:
                try:
                    # Fetch existing guild commands to see what's there
                    existing_commands = await bot.tree.fetch_commands(guild=guild_object)
                    if existing_commands:
                        diagnostic_parts.append(f"ℹ️ Found {len(existing_commands)} existing guild command(s)")
                except:
                    pass  # Ignore errors when fetching
            
            if guild_object:
                synced = await bot.tree.sync(guild=guild_object)
                diagnostic_parts.append(f"✅ Guild sync completed: {len(synced)} command(s) synced to guild")
                diagnostic_parts.append("   Guild commands appear immediately!")
            else:
                synced = await bot.tree.sync()
                diagnostic_parts.append(f"✅ Global sync completed: {len(synced)} command(s) synced globally")
                diagnostic_parts.append("   ⚠️ Global commands can take up to 1 hour to appear!")
            
            synced_names = {cmd.name for cmd in synced}
            
            # Wait for Discord to process
            await asyncio.sleep(2)
            
            # Validate by fetching
            try:
                if guild_object:
                    discord_commands = await bot.tree.fetch_commands(guild=guild_object)
                else:
                    discord_commands = await bot.tree.fetch_commands()
                
                discord_names = {cmd.name for cmd in discord_commands}
                
                # Try to get a specific command to verify accessibility
                if discord_commands:
                    test_cmd = discord_commands[0]
                    diagnostic_parts.append(f"✅ Successfully fetched commands from Discord")
                    diagnostic_parts.append(f"   Test command: {test_cmd.name} (ID: {test_cmd.id})")
                
            except discord.HTTPException as fetch_error:
                diagnostic_parts.append(f"❌ Could not fetch commands: {fetch_error}")
                if fetch_error.status == 404:
                    diagnostic_parts.append("   This usually means commands weren't synced or bot isn't in guild")
                elif fetch_error.status == 403:
                    diagnostic_parts.append("   Bot lacks permission to read commands (check OAuth2 scopes)")
                discord_names = set()
            except Exception as fetch_error:
                diagnostic_parts.append(f"⚠️ Error fetching commands: {fetch_error}")
                discord_names = set()
            
            # Build response
            missing = registered_names - discord_names
            extra = discord_names - registered_names
            
            response_parts = [
                f"**Sync Results ({sync_scope}):**",
                f"✅ Synced {len(synced)} command(s)",
                f"📋 Registered: {len(registered_names)}",
                f"📥 Found in Discord: {len(discord_names)}",
            ]
            
            if missing:
                response_parts.append(f"\n⚠️ Missing: {', '.join(sorted(missing))}")
            if extra:
                response_parts.append(f"\nℹ️ Extra: {', '.join(sorted(extra))}")
            
            # Add diagnostics
            response_parts.append("\n**Diagnostics:**")
            response_parts.extend(diagnostic_parts)
            
            # Add troubleshooting tips if commands aren't working
            if not missing and len(discord_names) > 0:
                response_parts.append("\n✅ All commands validated successfully!")
                if actual_guild is None and guild_id:
                    response_parts.append("\n⚠️ **TROUBLESHOOTING:**")
                    response_parts.append("Commands are synced but bot may not be in the guild.")
                    response_parts.append(f"Re-invite bot: https://discord.com/api/oauth2/authorize?client_id={bot.user.id}&permissions=0&scope=bot%20applications.commands")
            elif missing:
                response_parts.append("\n❌ Some commands are missing. Sync may have failed.")
                response_parts.append("\n**Troubleshooting:**")
                response_parts.append("1. Ensure bot was invited with 'applications.commands' scope")
                response_parts.append("2. For guild commands, bot must be in the guild")
                response_parts.append("3. Wait a few seconds and try again (Discord may be processing)")
                if guild_id:
                    response_parts.append(f"4. Re-invite: https://discord.com/api/oauth2/authorize?client_id={bot.user.id}&permissions=0&scope=bot%20applications.commands")
            else:
                response_parts.append("\n✅ Commands synced, but verify bot is in guild and has permissions.")
            
            return {
                "success": True,
                "message": "\n".join(response_parts)
            }
            
        except discord.HTTPException as e:
            error_msg = f"❌ Sync failed: {e}"
            if e.status == 429:
                error_msg += "\n⚠️ Rate limited! Wait before retrying."
            elif e.status == 403:
                error_msg += "\n⚠️ Check bot permissions (applications.commands scope)."
                error_msg += f"\nRe-invite bot: https://discord.com/api/oauth2/authorize?client_id={bot.user.id}&permissions=0&scope=bot%20applications.commands"
            elif e.status == 404:
                error_msg += "\n⚠️ Guild not found. Check DISCORD_GUILD_ID is correct."
            
            error_msg += "\n\n**Diagnostics:**"
            error_msg += "\n" + "\n".join(diagnostic_parts)
            
            return {
                "success": False,
                "message": error_msg
            }
        except Exception as e:
            error_msg = f"❌ Error during sync: {str(e)}"
            error_msg += "\n\n**Diagnostics:**"
            error_msg += "\n" + "\n".join(diagnostic_parts)
            return {
                "success": False,
                "message": error_msg
            }
    
    @bot.tree.command(name="sync-commands", description="Manually sync and validate bot commands (Admin only)")
    @discord.app_commands.describe(
        force_guild="Force sync to this guild (recommended for immediate availability)"
    )
    async def sync_commands_slash(interaction: discord.Interaction, force_guild: bool = True):
        """Sync commands slash command - manually triggers command sync and validation"""
        # Check if user has permission (administrator or manage guild)
        if not (interaction.user.guild_permissions.administrator or interaction.user.guild_permissions.manage_guild):
            await interaction.response.send_message(
                "You don't have permission to sync commands. Administrator or Manage Server permission required.",
                ephemeral=True
            )
            return
        
        await interaction.response.defer(ephemeral=True)
        
        try:
            # Store guild for force sync
            if force_guild and interaction.guild:
                bot._last_interaction_guild = interaction.guild
            
            result = await perform_sync_validation(bot, force_guild_sync=force_guild and interaction.guild is not None)
            await interaction.followup.send(result["message"], ephemeral=True)
        except Exception as e:
            await interaction.followup.send(
                f"❌ Failed to sync commands: {str(e)}",
                ephemeral=True
            )
    
    @bot.command(name="sync-commands")
    async def sync_commands_prefix(ctx: commands.Context):
        """Sync commands prefix command - manually triggers command sync and validation"""
        # Check if user has permission (administrator or manage guild)
        if not ctx.guild:
            await ctx.send("❌ This command can only be used in a server.")
            return
        
        if not (ctx.author.guild_permissions.administrator or ctx.author.guild_permissions.manage_guild):
            await ctx.send("❌ You don't have permission to sync commands. Administrator or Manage Server permission required.")
            return
        
        # Send initial message
        msg = await ctx.send("🔄 Syncing commands to this guild...")
        
        try:
            # Force guild sync when using prefix command
            bot._last_interaction_guild = ctx.guild
            result = await perform_sync_validation(bot, force_guild_sync=True)
            await msg.edit(content=result["message"])
        except Exception as e:
            await msg.edit(content=f"❌ Failed to sync commands: {str(e)}")
