import discord

from bot import constants

intents = discord.Intents.default()
intents.messages = True

client = discord.Client(intents=intents)

@client.event
async def on_ready():
    print(f'We have logged in as {constants.Client.name}')

@client.event
async def on_message(message):
    if message.author == constants.Client.name:
        return

    if message.content.startswith(constants.Client.prefix+'hello'):
        await message.channel.send('Hello!')
