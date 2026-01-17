# Discord Moderation Bot

A Discord bot built with Python and Discord.py that provides moderation commands for server administrators.

## Features

The bot includes commands organized by purpose:

### Administration Commands (mocked):
- `/ping` - Check if the bot is responding
- `/mute` - Mute a user in the server
- `/warn` - Warn a user in the server

### Gaming Commands:
- `/diceroll` - Roll various dice using standard notation (e.g., 1d6, 2d20, 3d8)

All admin commands currently respond with "not now" as a placeholder.

## Setup

### Prerequisites

- Docker installed on your system (for Docker deployment)
- Python 3.11+ installed (for local development)
- A Discord bot token (get one from [Discord Developer Portal](https://discord.com/developers/applications))

### Running with Docker

1. Create a `.env` file in the project root (or set environment variable):
   ```
   DISCORD_BOT_TOKEN=your_bot_token_here
   ```

2. Build the Docker image:
   ```bash
   docker build -t discord-bot .
   ```

3. Run the container:
   ```bash
   docker run -d --env-file .env discord-bot
   ```

   Or with environment variable directly:
   ```bash
   docker run -e DISCORD_BOT_TOKEN=your_bot_token_here discord-bot
   ```

### Local Development

1. Create a virtual environment (recommended):
   ```bash
   python3 -m venv venv
   ```

2. Activate the virtual environment:
   - On macOS/Linux:
     ```bash
     source venv/bin/activate
     ```
   - On Windows:
     ```bash
     venv\Scripts\activate
     ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Set the environment variable:
   ```bash
   export DISCORD_BOT_TOKEN=your_bot_token_here
   ```
   
   Or create a `.env` file:
   ```
   DISCORD_BOT_TOKEN=your_bot_token_here
   ```

5. Run the bot:
   ```bash
   python bot.py
   ```

6. When done, deactivate the virtual environment:
   ```bash
   deactivate
   ```

## Bot Setup on Discord

1. Go to [Discord Developer Portal](https://discord.com/developers/applications)
2. Create a new application
3. Go to the "Bot" section and create a bot
4. Copy the bot token
5. Enable the following intents in the Bot section:
   - Message Content Intent
6. Invite the bot to your server with the following permissions:
   - Use Slash Commands
   - Send Messages
   - Read Message History

## Notes

- All commands are currently mocked and will respond with "not now"
- The bot uses Discord's slash commands (application commands)
- Commands are ephemeral by default (only visible to the user who ran them)
