import discord
from discord.ext import commands

def setup(bot: commands.Bot):
    """Setup function to register message handlers"""
    
    @bot.event
    async def on_message(message: discord.Message):
        # Ignore messages from the bot itself
        if message.author == bot.user:
            return
        
        # Check if message contains "hey jab" (case-insensitive)
        if "hey jab" in message.content.lower():
            # Get the user's display name (server nickname if available, otherwise username)
            user_name = message.author.display_name
            await message.channel.send(f"Hey {user_name}")
        
        # Process commands (required for bot commands to work)
        await bot.process_commands(message)
