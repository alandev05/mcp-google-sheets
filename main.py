#!/usr/bin/env python3
"""
Entry point for Google Sheets MCP Server.

This file is required by Dedalus for deployment.
It imports and runs the main server from src/main.py.
"""

from src.main import main

if __name__ == "__main__":
    main()
