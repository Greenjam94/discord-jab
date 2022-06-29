# Dev setup

Install required dependencies

```
python3 -m pip install -U discord.py
python3 -m pip install -U python-dotenv
```

Alternative:
```
sudo apt install python3-pip
pip3 install python-dotenv
pip3 install discord.py
```

Go to discord developer portal and get a token from the bot page
https:// discord. com/developers/applications/your-app-id-here/bot

Create a .env file setting with a `BOT_TOKEN` env variable
```
BOT_TOKEN=replace_me1234567890
```

# Running the Bot
`python3 jab-commands.py`

# Functions

- `!test arg1 arg2` Tells you the first two parameters, errors if fewer are given
- `!add int1 int2` Adds two numbers together
- `!scream *` Converts to ALL CAPS
- `!slap user reason` says "$user just got slapped for $reason"

Two prompts can be used, ! or ?
