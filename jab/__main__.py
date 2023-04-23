import logging
import asyncio
import os
import discord

from discord.ext import commands

from jab import constants

log = logging.getLogger(__name__)

intents = discord.Intents.all()
intents.members = True
bot = commands.Bot(command_prefix=constants.BotConstants.prefix, intents=intents, help_command=None)

async def load():
    for filename in os.listdir('./jab/cogs'):
        if filename.endswith('.py'):
            extension = f"jab.cogs.{filename[:-3]}"
            print(f'Try to load {filename} as {extension}')
            await bot.load_extension(extension)

async def main():
    await load()
    await bot.start(constants.BotConstants.token)

asyncio.run(main())