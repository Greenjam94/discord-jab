import discord
from discord.ext import commands
import os
import asyncio
from dotenv import load_dotenv

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

@bot.event
async def on_ready():
    print(f'{bot.user} has logged in!')
    
    # Load command modules
    try:
        from commands import admin, games, torn
        admin.setup(bot)
        games.setup(bot)
        torn.setup(bot)
        print('Loaded command modules: admin, games, torn')
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

# Run the bot
if __name__ == "__main__":
    token = os.getenv('DISCORD_BOT_TOKEN')
    if not token:
        raise ValueError("DISCORD_BOT_TOKEN environment variable is required")
    bot.run(token)
