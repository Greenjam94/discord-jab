import os
import dataclasses
import enum
import logging
from datetime import datetime
from os import environ
from typing import NamedTuple

__all__ = (
    "BotConstants",
    "Logging",
    "ERROR_REPLIES",
    "NEGATIVE_REPLIES",
    "POSITIVE_REPLIES"
)

log = logging.getLogger(__name__)

class BotConstants(NamedTuple):
    name = "Jab"
    prefix = os.getenv("PREFIX", "!")
    token = os.getenv("BOT_TOKEN")
    debug = os.getenv("BOT_DEBUG", "true").lower() == "true"
    in_ci = os.getenv("IN_CI", "false").lower() == "true"
    github_bot_repo = "https://github.com/Greenjam94/discord-jab"

class Logging(NamedTuple):
    debug = BotConstants.debug
    file_logs = os.getenv("FILE_LOGS", "false").lower() == "true"
    trace_loggers = os.getenv("BOT_TRACE_LOGGERS")

ERROR_REPLIES = [
    "Please don't do that.",
    "You have to stop.",
    "Do you mind?",
    "In the future, don't do that.",
    "That was a mistake.",
    "You blew it.",
    "You're bad at computers.",
    "Are you trying to kill me?",
    "Noooooo!!",
    "I can't believe you've done this",
]

NEGATIVE_REPLIES = [
    "Noooooo!!",
    "Nope.",
    "I'm sorry Dave, I'm afraid I can't do that.",
    "I don't think so.",
    "Not gonna happen.",
    "Out of the question.",
    "Huh? No.",
    "Nah.",
    "Naw.",
    "Not likely.",
    "No way, José.",
    "Not in a million years.",
    "Fat chance.",
    "Certainly not.",
    "NEGATORY.",
    "Nuh-uh.",
    "Not in my house!",
]

POSITIVE_REPLIES = [
    "Yep.",
    "Absolutely!",
    "Can do!",
    "Affirmative!",
    "Yeah okay.",
    "Sure.",
    "Sure thing!",
    "You're the boss!",
    "Okay.",
    "No problem.",
    "I got you.",
    "Alright.",
    "You got it!",
    "ROGER THAT",
    "Of course!",
    "Aye aye, cap'n!",
    "I'll allow it.",
]
