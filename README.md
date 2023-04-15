# Dev setup

## Windows specific

1. Install visual stuio code
2. Install Docker Desktop
3. Setup Git 
4. Git clone this repo to a folder
5. Follow https://code.visualstudio.com/docs/python/python-tutorial to set up python environment on windows

# Project Setup

1. Set up virtual env

```
python3 -m venv .venv
source .venv/bin/activate
```

Windows alterative
```
py -3 -m venv .venv
.venv\scripts\activate
```

2. Install required dependencies

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

3. Go to discord developer portal and get a token from the bot page
https:// discord. com/developers/applications/your-app-id-here/bot

4. Create a .env file setting with a `BOT_TOKEN` env variable
```
BOT_TOKEN=replace_me1234567890
```

## Troubleshooting

poetry is a package management tool, if pyproject.toml is changed, make sure the .lock files is updated by running `poetry lock` if the command is not recognized then run `pip install --upgrade poetry`

Packages have locked versions... they may need to be updated

# Running the Bot on Docker
`docker-compose up -d --build`

