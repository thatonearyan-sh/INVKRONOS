import sys
import os
from pathlib import Path

# Add project root to sys.path so modules can be imported directly on Vercel
ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

# Signal to server that we are in Vercel Serverless environment
os.environ["VERCEL"] = "1"

from server import app
