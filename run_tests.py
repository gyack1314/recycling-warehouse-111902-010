#!/usr/bin/env python3
"""在 `python3 -I`（隔离模式）下运行全部 unittest。

-I 会剥离 cwd，因此显式把仓库根加入 sys.path；产物与原件隔离。
用法：python3 -I run_tests.py
"""
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

if __name__ == "__main__":
    loader = unittest.TestLoader()
    suite = loader.discover(str(ROOT / "tests"), pattern="test_*.py")
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    raise SystemExit(0 if result.wasSuccessful() else 1)
