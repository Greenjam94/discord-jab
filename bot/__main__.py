import logging
from bot import constants

from bot.client import client

log = logging.getLogger(__name__)

client.run(constants.Client.token)
