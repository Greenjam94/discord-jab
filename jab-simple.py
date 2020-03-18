import discord

import settings as s

class MyClient(discord.Client):
    async def on_ready(self):
        print('Logged on as {0}!'.format(self.user))

    async def on_message(self, message):
        #Display every event in python output
        print('Message from {0.author}: {0.content}'.format(message))

        if message.author == client.user:
            return

        if message.content.startswith('!hello'):
            msg = 'Hello {0.author.mention}'.format(message)
            await message.channel.send(msg)

client = MyClient()
client.run(s.TOKEN)
