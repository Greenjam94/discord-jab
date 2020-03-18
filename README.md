# Dev setup
python3 -m pip install -U discord.py
python3 -m pip install -U python-dotenv

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
