import discord
from discord.ext import commands

import settings as s

bot = commands.Bot(command_prefix=s.PREFIX)

@bot.event
async def on_ready():
    print('[+] Logged in as ' + bot.user.name)
    print('------ READY -----')

@bot.command()
async def test(ctx, arg1, arg2):
    await ctx.send('You passed {} and {}'.format(arg1, arg2))

@test.error
async def test_error(ctx, error):
    await ctx.send('please pass two arguements')

@bot.command()
async def add(ctx, a: int, b: int):
    await ctx.send(a + b)

def to_upper(argument):
    return argument.upper()

@bot.command()
async def scream(ctx, *, content: to_upper):
    await ctx.send(content)

@bot.command()
async def slap(ctx, members: commands.Greedy[discord.Member], *, reason='no reason'):
    slapped = ", ".join(x.name for x in members)
    await ctx.send('{} just got slapped for {}'.format(slapped, reason))

bot.run(s.TOKEN)
