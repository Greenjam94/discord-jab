import discord
from discord.ext import commands

class misc(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        # self._last_member = None

    @commands.Cog.listener()
    async def on_ready(self):
        print('Jab is online')

    @commands.command()
    async def ping(self, ctx):
        await ctx.send("Pong")

    # @commands.command()
    # async def whois(self, ctx, *, member: discord.Member = None):
    #     """Says hello"""
    #     member = member or ctx.author
    #     if self._last_member is None or self._last_member.id != member.id:
    #         await ctx.send(f'SYN-ACK {member.name}~')
    #     else:
    #         await ctx.send(f'ACK {member.name}. This feels familiar.')
    #     self._last_member = member

    # @commands.command()
    # async def ping(self, ctx, *, member: discord.Member = None):
    #     """Says hello"""
    #     member = member or ctx.author
    #     if self._last_member.id == member.id:
    #         await ctx.send(f'PONG {self._last_member.name}~')
    #     else:
    #         await ctx.send(f'PING {self._last_member.name}~')

async def setup(bot):
    await bot.add_cog(misc(bot))