import discord
from discord.ext import commands
import random

def setup(bot: commands.Bot):
    """Setup function to register game commands"""
    
    @bot.tree.command(name="diceroll", description="Roll various dice and generate random dice rolls")
    async def diceroll(interaction: discord.Interaction, dice: str = "1d6"):
        """
        Roll dice in standard notation (e.g., 2d6, 1d20, 3d8)
        Examples: 1d6, 2d20, 4d10, 1d100
        """
        try:
            # Parse dice notation (e.g., "2d6" = 2 dice with 6 sides each)
            if 'd' not in dice.lower():
                await interaction.response.send_message(
                    "Invalid dice format. Use format like: 1d6, 2d20, 3d8, etc.",
                    ephemeral=True
                )
                return
            
            parts = dice.lower().split('d')
            if len(parts) != 2:
                await interaction.response.send_message(
                    "Invalid dice format. Use format like: 1d6, 2d20, 3d8, etc.",
                    ephemeral=True
                )
                return
            
            num_dice = int(parts[0]) if parts[0] else 1
            num_sides = int(parts[1])
            
            # Validate inputs
            if num_dice < 1 or num_dice > 100:
                await interaction.response.send_message(
                    "Number of dice must be between 1 and 100.",
                    ephemeral=True
                )
                return
            
            if num_sides < 2 or num_sides > 1000:
                await interaction.response.send_message(
                    "Number of sides must be between 2 and 1000.",
                    ephemeral=True
                )
                return
            
            # Roll the dice
            rolls = [random.randint(1, num_sides) for _ in range(num_dice)]
            total = sum(rolls)
            
            # Format response
            if num_dice == 1:
                result = f"🎲 Rolled {dice}: **{rolls[0]}**"
            else:
                rolls_str = ", ".join(map(str, rolls))
                result = f"🎲 Rolled {dice}: {rolls_str}\n**Total: {total}**"
            
            await interaction.response.send_message(result)
            
        except ValueError:
            await interaction.response.send_message(
                "Invalid dice format. Use format like: 1d6, 2d20, 3d8, etc.",
                ephemeral=True
            )
        except Exception as e:
            await interaction.response.send_message(
                f"An error occurred: {str(e)}",
                ephemeral=True
            )
    
    @bot.tree.command(name="parrot", description="Repeat anything you say")
    async def parrot(interaction: discord.Interaction, text: str):
        """Parrot command - repeats the provided text or squaks"""
        if random.randint(1, 1000) % 3 == 0:
            text = 'Squak!'
        await interaction.response.send_message(text)
