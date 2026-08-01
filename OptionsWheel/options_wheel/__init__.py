"""Compatibility package so `python -m options_wheel...` works from repo root."""

import os


_SRC_PACKAGE_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "src", "options_wheel")
)

__path__ = [_SRC_PACKAGE_DIR]
