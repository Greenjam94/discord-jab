import discord
from discord.ext import commands, tasks
import os
import asyncio
from dotenv import load_dotenv
from datetime import datetime, timedelta

# Load environment variables from .env file
load_dotenv()

# Bot setup
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix='!', intents=intents)

async def sync_commands_with_validation(bot: commands.Bot):
    """
    Sync commands to Discord and validate that the sync succeeded.
    Fetches commands from Discord after syncing to verify they're actually registered.
    """
    try:
        # Get all registered commands from the bot
        registered_commands = {cmd.name: cmd for cmd in bot.tree.get_commands()}
        registered_names = set(registered_commands.keys())
        
        print(f'📋 Registered commands in bot: {sorted(registered_names)}')
        
        if not registered_names:
            print('⚠️  No commands registered! Check your command setup functions.')
            return
        
        # Try guild-specific sync first, then fall back to global
        guild_id = os.getenv('DISCORD_GUILD_ID')
        guild_object = None
        actual_guild = None
        sync_scope = "guild"
        
        if guild_id:
            try:
                guild_id_int = int(guild_id)
                guild_object = discord.Object(id=guild_id_int)
                
                # Check if bot is actually in the guild
                actual_guild = bot.get_guild(guild_id_int)
                if actual_guild:
                    print(f'✅ Bot is in guild: {actual_guild.name} (ID: {guild_id})')
                    
                    # Check bot's permissions
                    bot_member = actual_guild.get_member(bot.user.id)
                    if bot_member:
                        perms = bot_member.guild_permissions
                        if perms.administrator:
                            print('✅ Bot has Administrator permission')
                        else:
                            if not perms.use_application_commands:
                                print('⚠️  Bot missing permission: Use Application Commands')
                            if not perms.send_messages:
                                print('⚠️  Bot missing permission: Send Messages')
                            if perms.use_application_commands and perms.send_messages:
                                print('✅ Bot has basic permissions')
                    else:
                        print('⚠️  Bot member not found in guild (may need to rejoin)')
                else:
                    print(f'⚠️  WARNING: Bot is NOT in guild {guild_id}!')
                    print(f'   Commands will not work until bot is invited to the guild.')
                    print(f'   Invite URL: https://discord.com/api/oauth2/authorize?client_id={bot.user.id}&permissions=0&scope=bot%20applications.commands')
                
                print(f'🔄 Syncing commands to guild {guild_id}...')
            except ValueError:
                print(f'⚠️  Invalid DISCORD_GUILD_ID format: {guild_id}')
                guild_object = None
                sync_scope = "global"
        else:
            print('🔄 Syncing commands globally (may take up to 1 hour to appear)...')
            sync_scope = "global"
        
        # Check bot application info
        try:
            app_info = await bot.application_info()
            print(f'ℹ️  Bot Application: {app_info.name} (ID: {app_info.id})')
        except Exception as e:
            print(f'⚠️  Could not fetch application info: {e}')
        
        # Perform the sync
        try:
            if guild_object:
                synced = await bot.tree.sync(guild=guild_object)
            else:
                synced = await bot.tree.sync()
            
            print(f'✅ Sync completed. Discord returned {len(synced)} command(s):')
            
        except discord.HTTPException as e:
            print(f'❌ HTTP error during sync: {e}')
            if e.status == 429:  # Rate limit
                print('⚠️  Rate limited! Commands may not have synced. Wait before retrying.')
            elif e.status == 403:
                print('⚠️  Forbidden! Check bot permissions and ensure it has "applications.commands" scope.')
            return
        except Exception as sync_error:
            print(f'❌ Sync failed: {sync_error}')
            import traceback
            traceback.print_exc()
            return
        
        # Wait a moment for Discord to process the sync
        await asyncio.sleep(2)
        
        # Validate by fetching commands from Discord
        print(f'\n🔍 Validating sync by fetching commands from Discord ({sync_scope})...')
        try:
            if guild_object:
                # Fetch guild commands
                discord_commands = await bot.tree.fetch_commands(guild=guild_object)
            else:
                # Fetch global commands
                discord_commands = await bot.tree.fetch_commands()
            
            discord_names = {cmd.name for cmd in discord_commands}
            print(f'📥 Commands found in Discord: {sorted(discord_names)}')
            
            # Compare registered vs synced
            missing_in_discord = registered_names - discord_names
            extra_in_discord = discord_names - registered_names
            
            if missing_in_discord:
                print(f'⚠️  WARNING: {len(missing_in_discord)} command(s) registered but NOT found in Discord:')
                for cmd_name in sorted(missing_in_discord):
                    print(f'   - {cmd_name}')
                print('   This may indicate a sync failure. Try syncing again.')
            
            if extra_in_discord:
                print(f'ℹ️  {len(extra_in_discord)} command(s) in Discord but not in current bot code:')
                for cmd_name in sorted(extra_in_discord):
                    print(f'   - {cmd_name}')
                print('   These may be old commands that should be removed.')
            
            if not missing_in_discord and not extra_in_discord:
                print('✅ Validation successful! All registered commands are present in Discord.')
                if actual_guild is None and guild_id:
                    print('\n⚠️  TROUBLESHOOTING: Commands are synced but bot may not be in the guild.')
                    print(f'   Re-invite bot: https://discord.com/api/oauth2/authorize?client_id={bot.user.id}&permissions=0&scope=bot%20applications.commands')
            elif not missing_in_discord:
                print('✅ All registered commands are present in Discord (some extra commands exist).')
            else:
                print('❌ Validation failed! Some commands are missing. The sync may not have completed successfully.')
                print('\nTroubleshooting:')
                print('1. Ensure bot was invited with \'applications.commands\' scope')
                print('2. For guild commands, bot must be in the guild')
                print('3. Wait a few seconds and try again (Discord may be processing)')
                if guild_id:
                    print(f'4. Re-invite: https://discord.com/api/oauth2/authorize?client_id={bot.user.id}&permissions=0&scope=bot%20applications.commands')
                
        except discord.HTTPException as e:
            print(f'⚠️  Could not fetch commands for validation: {e}')
            if e.status == 404:
                print('   This might mean the bot is not in the guild or commands were not synced.')
                if guild_id:
                    print(f'   Re-invite bot: https://discord.com/api/oauth2/authorize?client_id={bot.user.id}&permissions=0&scope=bot%20applications.commands')
            elif e.status == 403:
                print('   Check bot permissions - it needs to be able to read application commands.')
                print('   Bot must be invited with \'applications.commands\' scope in OAuth2.')
        except Exception as validation_error:
            print(f'⚠️  Error during validation: {validation_error}')
            import traceback
            traceback.print_exc()
            
    except Exception as e:
        print(f'❌ Failed to sync commands: {e}')
        import traceback
        traceback.print_exc()

@tasks.loop(hours=24)  # Check daily
async def monthly_summarization_task():
    """Background task to run monthly summarization on the 1st of each month."""
    if not hasattr(bot, 'database') or not bot.database:
        return
    
    now = datetime.utcnow()
    
    # Only run on the 1st of the month
    if now.day != 1:
        return
    
    # Calculate previous month
    if now.month == 1:
        prev_year = now.year - 1
        prev_month = 12
    else:
        prev_year = now.year
        prev_month = now.month - 1
    
    print(f'📊 Starting monthly summarization for {prev_year}-{prev_month:02d}...')
    
    try:
        # Create backup before summarization
        try:
            backup_path = await bot.database.backup()
            print(f'✅ Backup created: {backup_path}')
        except Exception as backup_error:
            print(f'⚠️  Backup failed: {backup_error}')
        
        # Summarize player stats
        try:
            summaries = await bot.database.summarize_player_stats_monthly(prev_year, prev_month)
            print(f'✅ Created {summaries} player stats summaries for {prev_year}-{prev_month:02d}')
        except Exception as e:
            print(f'⚠️  Player stats summarization failed: {e}')
        
        # Summarize faction history
        try:
            summaries = await bot.database.summarize_faction_history_monthly(prev_year, prev_month)
            print(f'✅ Created {summaries} faction history summaries for {prev_year}-{prev_month:02d}')
        except Exception as e:
            print(f'⚠️  Faction history summarization failed: {e}')
        
        # Summarize territory ownership
        try:
            summaries = await bot.database.summarize_territory_ownership_monthly(prev_year, prev_month)
            print(f'✅ Created {summaries} territory ownership summaries for {prev_year}-{prev_month:02d}')
        except Exception as e:
            print(f'⚠️  Territory ownership summarization failed: {e}')
        
        # TODO: Add war status summarization when method is implemented
        # await bot.database.summarize_war_status_monthly(prev_year, prev_month)
        
        # Prune old records (older than 2 months)
        try:
            pruned = await bot.database.prune_player_stats_history(older_than_days=60)
            print(f'✅ Pruned {pruned} old player stats records')
        except Exception as e:
            print(f'⚠️  Player stats pruning failed: {e}')
        
        try:
            pruned = await bot.database.prune_faction_history(older_than_days=60)
            print(f'✅ Pruned {pruned} old faction history records')
        except Exception as e:
            print(f'⚠️  Faction history pruning failed: {e}')
        
        try:
            pruned = await bot.database.prune_territory_ownership_history(older_than_days=60)
            print(f'✅ Pruned {pruned} old territory ownership records')
        except Exception as e:
            print(f'⚠️  Territory ownership pruning failed: {e}')
        
        # TODO: Add pruning for war status when method is implemented
        
        print(f'✅ Monthly summarization completed for {prev_year}-{prev_month:02d}')
        
    except Exception as e:
        print(f'❌ Monthly summarization error: {e}')
        import traceback
        traceback.print_exc()

@monthly_summarization_task.before_loop
async def before_monthly_task():
    """Wait until bot is ready before starting the task."""
    await bot.wait_until_ready()
    # Wait until next hour to avoid running immediately on startup
    await asyncio.sleep(3600)  # Wait 1 hour

@tasks.loop(hours=24)  # Run daily
async def daily_competition_stats_update_task():
    """Background task to update stats for all active competitions daily."""
    if not hasattr(bot, 'database') or not bot.database:
        return
    
    print('📊 Starting daily competition stats update...')
    
    try:
        from commands.competitions import _update_competition_stats_helper
        result = await _update_competition_stats_helper(bot)
        
        if result.get("error"):
            print(f'❌ Daily competition stats update error: {result.get("error")}')
        else:
            print(f'✅ Daily competition stats update completed: {result.get("message")}')
    except Exception as e:
        print(f'❌ Daily competition stats update error: {e}')
        import traceback
        traceback.print_exc()

@daily_competition_stats_update_task.before_loop
async def before_daily_competition_task():
    """Wait until bot is ready before starting the task."""
    await bot.wait_until_ready()
    # Wait until next hour to avoid running immediately on startup
    await asyncio.sleep(3600)  # Wait 1 hour

@bot.event
async def on_ready():
    print(f'{bot.user} has logged in!')
    
    # Initialize database
    try:
        from database import TornDatabase
        bot.database = TornDatabase()
        await bot.database.connect()
        print('✅ Database initialized and connected')
    except ImportError as e:
        print(f'❌ Failed to import database module: {e}')
        print('   Make sure aiosqlite is installed: pip install aiosqlite>=0.19.0')
        bot.database = None
    except Exception as e:
        print(f'⚠️  Failed to initialize database: {e}')
        import traceback
        traceback.print_exc()
        bot.database = None
    
    # Load command modules
    try:
        from commands import admin, games, torn, competitions
        admin.setup(bot)
        games.setup(bot)
        torn.setup(bot)
        competitions.setup(bot)
        print('Loaded command modules: admin, games, torn, competitions')
    except Exception as e:
        print(f'Failed to load command modules: {e}')
    
    # Load message handlers
    try:
        import message
        message.setup(bot)
        print('Loaded message handlers')
    except Exception as e:
        print(f'Failed to load message handlers: {e}')
    
    # Wait a moment for commands to be fully registered
    await asyncio.sleep(1)
    
    # Sync commands with validation
    await sync_commands_with_validation(bot)
    
    # Start monthly summarization task
    if bot.database:
        monthly_summarization_task.start()
        print('✅ Monthly summarization task started')
        
        # Start daily competition stats update task
        daily_competition_stats_update_task.start()
        print('✅ Daily competition stats update task started')

@bot.event
async def on_disconnect():
    """Cleanup on bot disconnect."""
    if hasattr(bot, 'database') and bot.database:
        await bot.database.close()
        print('Database connection closed')

# Run the bot
if __name__ == "__main__":
    token = os.getenv('DISCORD_BOT_TOKEN')
    if not token:
        raise ValueError("DISCORD_BOT_TOKEN environment variable is required")
    try:
        bot.run(token)
    finally:
        # Ensure database is closed on exit
        if hasattr(bot, 'database') and bot.database:
            import asyncio
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    asyncio.create_task(bot.database.close())
                else:
                    loop.run_until_complete(bot.database.close())
            except:
                pass
