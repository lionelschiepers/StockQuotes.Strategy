"""Backward-compatible entrypoint forcing --type call."""
import os
import sys

src_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "src")
sys.path.insert(0, src_dir)

from options_wheel.analysis import main  # noqa: E402


if __name__ == "__main__":
    if "--type" not in sys.argv:
        sys.argv.extend(["--type", "call"])
    main()
