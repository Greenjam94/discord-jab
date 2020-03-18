from dotenv import load_dotenv
import os

load_dotenv(verbose=True)

TOKEN = os.getenv("BOT_TOKEN")
PREFIX = ("?", "!")

print("[+] settings loaded")
