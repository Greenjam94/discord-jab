import discord
from discord.ext import commands
import re
import os
import asyncio
from datetime import datetime, timedelta
from typing import Optional
from utils.permissions import check_command_permission, get_bot_database

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
    
    @bot.tree.command(name="warn", description="Warn a user in the server")
    @discord.app_commands.describe(
        user="The user to warn",
        reason="Reason for the warning"
    )
    async def warn(interaction: discord.Interaction, user: discord.Member, reason: str = "No reason provided"):
        """Warn command - warns a user"""
        # Check database permissions first
        db = get_bot_database(bot)
        has_permission, error_msg = await check_command_permission(interaction, "warn", db)
        if not has_permission:
            await interaction.response.send_message(error_msg or "You don't have permission to use this command.", ephemeral=True)
            return
        
        # Legacy check: if no DB permissions set, fall back to Discord permissions
        if db:
            perms = await db.get_command_permissions(str(interaction.guild.id))
            if not perms.get("warn"):  # No custom permissions set, use Discord default
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
        # Check database permissions first
        db = get_bot_database(bot)
        has_permission, error_msg = await check_command_permission(interaction, "sync-commands", db)
        if not has_permission:
            await interaction.response.send_message(error_msg or "You don't have permission to use this command.", ephemeral=True)
            return
        
        # Legacy check: if no DB permissions set, fall back to Discord permissions
        if db:
            perms = await db.get_command_permissions(str(interaction.guild.id))
            if not perms.get("sync-commands"):  # No custom permissions set, use Discord default
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
        """Sync commands via ! prefix - manually triggers command sync and validation"""
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
    
    @bot.tree.command(name="db-health", description="View database health metrics (Admin only)")
    async def db_health(interaction: discord.Interaction):
        """Display database health metrics."""
        # Check database permissions first
        db = get_bot_database(bot)
        has_permission, error_msg = await check_command_permission(interaction, "db-health", db)
        if not has_permission:
            await interaction.response.send_message(error_msg or "You don't have permission to use this command.", ephemeral=True)
            return
        
        # Legacy check: if no DB permissions set, fall back to Discord permissions
        if db:
            perms = await db.get_command_permissions(str(interaction.guild.id))
            if not perms.get("db-health"):  # No custom permissions set, use Discord default
                if not interaction.user.guild_permissions.administrator:
                    await interaction.response.send_message(
                        "You don't have permission to view database metrics.",
                        ephemeral=True
                    )
                    return
        
        await interaction.response.defer(ephemeral=True)
        
        try:
            # Get database instance from bot
            if not hasattr(bot, 'database') or bot.database is None:
                await interaction.followup.send(
                    "❌ Database not initialized. Please check bot configuration.",
                    ephemeral=True
                )
                return
            
            metrics = await bot.database.get_health_metrics()
            
            # Build embed
            embed = discord.Embed(
                title="📊 Database Health Metrics",
                color=discord.Color.blue(),
                timestamp=datetime.utcnow()
            )
            
            # Database info
            embed.add_field(
                name="Database Info",
                value=f"**Size:** {metrics.get('database_size_mb', 0)} MB\n"
                      f"**Schema Version:** {metrics.get('schema_version', 'Unknown')}",
                inline=False
            )
            
            # Current state tables
            embed.add_field(
                name="Current State Tables",
                value=f"**Players:** {metrics.get('players_count', 0):,}\n"
                      f"**Factions:** {metrics.get('factions_count', 0):,}",
                inline=True
            )
            
            # Append-only tables
            history_counts = (
                f"**Player Stats:** {metrics.get('player_stats_history_count', 0):,}\n"
                f"**Faction History:** {metrics.get('faction_history_count', 0):,}\n"
                f"**War Status:** {metrics.get('war_status_history_count', 0):,}\n"
                f"**Territory Ownership:** {metrics.get('territory_ownership_history_count', 0):,}"
            )
            embed.add_field(
                name="Historical Records",
                value=history_counts,
                inline=True
            )
            
            # Summary tables
            summary_counts = (
                f"**Player Stats:** {metrics.get('player_stats_summary_count', 0):,}\n"
                f"**Faction:** {metrics.get('faction_summary_count', 0):,}\n"
                f"**Wars:** {metrics.get('war_summary_count', 0):,}\n"
                f"**Territories:** {metrics.get('territory_ownership_summary_count', 0):,}"
            )
            embed.add_field(
                name="Monthly Summaries",
                value=summary_counts,
                inline=True
            )
            
            # Old records (should be pruned)
            old_records = (
                f"**Player Stats:** {metrics.get('player_stats_history_old_records', 0):,}\n"
                f"**Faction History:** {metrics.get('faction_history_old_records', 0):,}\n"
                f"**War Status:** {metrics.get('war_status_history_old_records', 0):,}\n"
                f"**Territory:** {metrics.get('territory_ownership_history_old_records', 0):,}"
            )
            
            total_old = (
                metrics.get('player_stats_history_old_records', 0) +
                metrics.get('faction_history_old_records', 0) +
                metrics.get('war_status_history_old_records', 0) +
                metrics.get('territory_ownership_history_old_records', 0)
            )
            
            if total_old > 0:
                embed.add_field(
                    name="⚠️ Records > 2 Months Old",
                    value=old_records,
                    inline=False
                )
                embed.color = discord.Color.orange()
            else:
                embed.add_field(
                    name="✅ Pruning Status",
                    value="No records older than 2 months found.",
                    inline=False
                )
            
            # Data range info
            if metrics.get('player_stats_history_oldest'):
                embed.add_field(
                    name="Data Range",
                    value=f"**Oldest:** {metrics.get('player_stats_history_oldest', 'N/A')}\n"
                          f"**Newest:** {metrics.get('player_stats_history_newest', 'N/A')}",
                    inline=False
                )
            
            await interaction.followup.send(embed=embed, ephemeral=True)
            
        except Exception as e:
            await interaction.followup.send(
                f"❌ Error retrieving database metrics: {str(e)}",
                ephemeral=True
            )
    
    @bot.tree.command(name="db-query", description="Query database tables with pagination (Admin only)")
    @discord.app_commands.describe(
        table="Table name to query",
        page="Page number (starts at 1)",
        limit="Number of rows per page (1-50, default: 20)",
        order_by="Column to order by (default: primary key or first column)",
        filter="Optional WHERE clause filter (e.g., 'player_id = 12345')"
    )
    async def db_query(
        interaction: discord.Interaction,
        table: str,
        page: int = 1,
        limit: int = 20,
        order_by: Optional[str] = None,
        filter: Optional[str] = None
    ):
        """Query database tables with pagination."""
        # Check database permissions first
        db = get_bot_database(bot)
        has_permission, error_msg = await check_command_permission(interaction, "db-query", db)
        if not has_permission:
            await interaction.response.send_message(error_msg or "You don't have permission to use this command.", ephemeral=True)
            return
        
        # Legacy check: if no DB permissions set, fall back to Discord permissions
        if db:
            perms = await db.get_command_permissions(str(interaction.guild.id))
            if not perms.get("db-query"):  # No custom permissions set, use Discord default
                if not interaction.user.guild_permissions.administrator:
                    await interaction.response.send_message(
                "You don't have permission to query the database.",
                ephemeral=True
            )
            return
        
        # Validate limit
        limit = max(1, min(50, limit))
        
        # Validate page
        if page < 1:
            page = 1
        
        await interaction.response.defer(ephemeral=True)
        
        try:
            if not hasattr(bot, 'database') or bot.database is None:
                await interaction.followup.send(
                    "❌ Database not initialized.",
                    ephemeral=True
                )
                return
            
            # Get table info to validate
            table_info = await bot.database.get_table_info()
            
            if table not in table_info:
                available = ', '.join(sorted(table_info.keys()))
                await interaction.followup.send(
                    f"❌ Table '{table}' not found.\n\n**Available tables:**\n{available}",
                    ephemeral=True
                )
                return
            
            # Calculate offset
            offset = (page - 1) * limit
            
            # Parse filter into where clause and params (simple parsing)
            where_clause = None
            where_params = None
            
            if filter:
                # Basic filter parsing - only allow simple comparisons
                # This is a simple implementation - for production, use a proper SQL parser or builder
                where_clause = filter
            
            # Query the table
            rows, total_count = await bot.database.query_table(
                table_name=table,
                limit=limit,
                offset=offset,
                order_by=order_by,
                where_clause=where_clause,
                where_params=where_params
            )
            
            if not rows:
                await interaction.followup.send(
                    f"📭 No rows found in `{table}`" + (f" with filter `{filter}`" if filter else ""),
                    ephemeral=True
                )
                return
            
            # Calculate pagination info
            total_pages = (total_count + limit - 1) // limit if total_count > 0 else 1
            
            # Format rows for display
            columns = list(rows[0].keys())
            
            # Truncate long values for Discord
            def truncate_value(value, max_len=50):
                if value is None:
                    return "NULL"
                str_val = str(value)
                if len(str_val) > max_len:
                    return str_val[:max_len-3] + "..."
                return str_val
            
            # Build display text
            display_rows = []
            for row in rows:
                row_str = " | ".join([f"**{col}**: {truncate_value(row[col])}" for col in columns[:5]])  # Show first 5 columns
                display_rows.append(row_str)
            
            embed = discord.Embed(
                title=f"📊 Database Query: `{table}`",
                description=f"**Page {page}/{total_pages}** ({total_count:,} total rows)\n" +
                           (f"**Filter:** `{filter}`\n" if filter else "") +
                           (f"**Order by:** `{order_by}`\n" if order_by else ""),
                color=discord.Color.blue(),
                timestamp=datetime.utcnow()
            )
            
            # Add rows (Discord embed limit is ~6000 chars, so be conservative)
            rows_text = "\n".join(display_rows[:10])  # Show max 10 rows per page
            if len(display_rows) > 10:
                rows_text += f"\n... and {len(display_rows) - 10} more (showing first 10)"
            
            embed.add_field(
                name=f"Rows {offset + 1}-{min(offset + limit, total_count)}",
                value=rows_text[:1024],  # Discord field limit
                inline=False
            )
            
            embed.set_footer(text=f"Columns: {', '.join(columns)}")
            
            # Create pagination buttons
            view = DatabaseQueryView(
                bot=bot,
                table=table,
                page=page,
                limit=limit,
                total_pages=total_pages,
                total_count=total_count,
                order_by=order_by,
                filter=filter
            )
            
            await interaction.followup.send(embed=embed, view=view, ephemeral=True)
            
        except ValueError as e:
            await interaction.followup.send(
                f"❌ Invalid query: {str(e)}",
                ephemeral=True
            )
        except Exception as e:
            await interaction.followup.send(
                f"❌ Error querying database: {str(e)}",
                ephemeral=True
            )
    
    @bot.tree.command(name="db-tables", description="List all available database tables (Admin only)")
    async def db_tables(interaction: discord.Interaction):
        """List all available database tables."""
        # Check database permissions first
        db = get_bot_database(bot)
        has_permission, error_msg = await check_command_permission(interaction, "db-tables", db)
        if not has_permission:
            await interaction.response.send_message(error_msg or "You don't have permission to use this command.", ephemeral=True)
            return
        
        # Legacy check: if no DB permissions set, fall back to Discord permissions
        if db:
            perms = await db.get_command_permissions(str(interaction.guild.id))
            if not perms.get("db-tables"):  # No custom permissions set, use Discord default
                if not interaction.user.guild_permissions.administrator:
                    await interaction.response.send_message(
                        "You don't have permission to view database tables.",
                        ephemeral=True
                    )
                    return
        
        await interaction.response.defer(ephemeral=True)
        
        try:
            if not hasattr(bot, 'database') or bot.database is None:
                await interaction.followup.send(
                    "❌ Database not initialized.",
                    ephemeral=True
                )
                return
            
            table_info = await bot.database.get_table_info()
            
            embed = discord.Embed(
                title="📋 Available Database Tables",
                color=discord.Color.blue(),
                timestamp=datetime.utcnow()
            )
            
            # Group tables by category
            current_state = ['players', 'factions']
            history = ['player_stats_history', 'faction_history', 'war_status_history', 'territory_ownership_history']
            summaries = ['player_stats_summary', 'faction_summary', 'war_summary', 'territory_ownership_summary']
            competitions = ['competitions', 'competition_participants', 'competition_teams', 'competition_start_stats', 'competition_stats']
            
            def format_table_list(tables):
                return "\n".join([f"• `{t}` ({len(table_info.get(t, []))} columns)" for t in tables if t in table_info])
            
            if any(t in table_info for t in current_state):
                embed.add_field(
                    name="Current State Tables",
                    value=format_table_list(current_state),
                    inline=False
                )
            
            if any(t in table_info for t in history):
                embed.add_field(
                    name="Historical Tables",
                    value=format_table_list(history),
                    inline=False
                )
            
            if any(t in table_info for t in summaries):
                embed.add_field(
                    name="Summary Tables",
                    value=format_table_list(summaries),
                    inline=False
                )
            
            if any(t in table_info for t in competitions):
                embed.add_field(
                    name="Competition Tables",
                    value=format_table_list(competitions),
                    inline=False
                )
            
            await interaction.followup.send(embed=embed, ephemeral=True)
            
        except Exception as e:
            await interaction.followup.send(
                f"❌ Error listing tables: {str(e)}",
                ephemeral=True
            )


class DatabaseQueryView(discord.ui.View):
    """View with buttons for paginating database query results."""
    
    def __init__(
        self,
        bot: commands.Bot,
        table: str,
        page: int,
        limit: int,
        total_pages: int,
        total_count: int,
        order_by: Optional[str],
        filter: Optional[str]
    ):
        super().__init__(timeout=300)  # 5 minute timeout
        self.bot = bot
        self.table = table
        self.page = page
        self.limit = limit
        self.total_pages = total_pages
        self.total_count = total_count
        self.order_by = order_by
        self.filter = filter
    
    @discord.ui.button(label="◀️ First", style=discord.ButtonStyle.secondary, disabled=True)
    async def first_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.page == 1:
            return
        self.page = 1
        await self.update_view(interaction)
    
    @discord.ui.button(label="◀️ Prev", style=discord.ButtonStyle.primary, disabled=True)
    async def prev_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.page > 1:
            self.page -= 1
            await self.update_view(interaction)
    
    @discord.ui.button(label="Next ▶️", style=discord.ButtonStyle.primary)
    async def next_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.page < self.total_pages:
            self.page += 1
            await self.update_view(interaction)
    
    @discord.ui.button(label="Last ▶️▶️", style=discord.ButtonStyle.secondary)
    async def last_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.page == self.total_pages:
            return
        self.page = self.total_pages
        await self.update_view(interaction)
    
    async def update_view(self, interaction: discord.Interaction):
        """Update the view with new page."""
        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message(
                "You don't have permission to query the database.",
                ephemeral=True
            )
            return
        
        await interaction.response.defer()
        
        try:
            offset = (self.page - 1) * self.limit
            
            rows, total_count = await self.bot.database.query_table(
                table_name=self.table,
                limit=self.limit,
                offset=offset,
                order_by=self.order_by,
                where_clause=self.filter,
                where_params=None
            )
            
            if not rows:
                await interaction.followup.send(
                    f"📭 No rows found.",
                    ephemeral=True
                )
                return
            
            columns = list(rows[0].keys())
            
            def truncate_value(value, max_len=50):
                if value is None:
                    return "NULL"
                str_val = str(value)
                if len(str_val) > max_len:
                    return str_val[:max_len-3] + "..."
                return str_val
            
            display_rows = []
            for row in rows:
                row_str = " | ".join([f"**{col}**: {truncate_value(row[col])}" for col in columns[:5]])
                display_rows.append(row_str)
            
            embed = discord.Embed(
                title=f"📊 Database Query: `{self.table}`",
                description=f"**Page {self.page}/{self.total_pages}** ({total_count:,} total rows)\n" +
                           (f"**Filter:** `{self.filter}`\n" if self.filter else "") +
                           (f"**Order by:** `{self.order_by}`\n" if self.order_by else ""),
                color=discord.Color.blue(),
                timestamp=datetime.utcnow()
            )
            
            rows_text = "\n".join(display_rows[:10])
            if len(display_rows) > 10:
                rows_text += f"\n... and {len(display_rows) - 10} more (showing first 10)"
            
            embed.add_field(
                name=f"Rows {offset + 1}-{min(offset + self.limit, total_count)}",
                value=rows_text[:1024],
                inline=False
            )
            
            embed.set_footer(text=f"Columns: {', '.join(columns)}")
            
            # Update button states
            self.first_page.disabled = self.page == 1
            self.prev_page.disabled = self.page == 1
            self.next_page.disabled = self.page >= self.total_pages
            self.last_page.disabled = self.page >= self.total_pages
            
            await interaction.followup.edit_message(interaction.message.id, embed=embed, view=self)
            
        except Exception as e:
            await interaction.followup.send(
                f"❌ Error: {str(e)}",
                ephemeral=True
            )
