#!/usr/bin/env python3
"""仓库根入口，便于 `python3 -I run.py ...` 运行（-I 会剥离 cwd，故显式加入路径）。"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from market.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
