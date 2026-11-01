"""Sync scan archives, IV history and outputs with a data-branch checkout.

Scan archives and the IV history only become useful once they accumulate over
months, but they are too noisy to commit to ``main``. CI therefore keeps them
on a dedicated ``ow-data`` branch with this layout::

    history/iv/YYYY-MM-DD.json             # {symbol|type: atm_iv} per scan date
    history/scans/{put,call}/YYYY-MM-DD.json
    output/*.json            # latest results and graded outcomes

``pull`` brings that data into ``data/`` without overwriting local files (the
IV history is merged by date), ``push`` copies local data into the checkout.
A pre-split ``history/iv_history.json`` is read too, and the destination copy
is converted to per-date files and deleted. The checkout must sit inside the
folder that contains this repository (e.g. ``../ow-data`` next to it).

Usage::

    git worktree add ../ow-data ow-data
    python -m options_wheel.datasync pull ../ow-data
"""

from __future__ import annotations

import argparse
import glob
import os
import shutil

from .iv_history import IV_HISTORY_DIRNAME, LEGACY_FILENAME, IVHistoryStore

MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(MODULE_DIR, "..", ".."))
LOCAL_HISTORY_DIR = os.path.join(PROJECT_ROOT, "data", "history")
LOCAL_OUTPUT_DIR = os.path.join(PROJECT_ROOT, "data", "output")
# The data-branch checkout lives next to the repository, e.g. ``../ow-data``.
WORKSPACE_ROOT = os.path.dirname(os.path.dirname(PROJECT_ROOT))


def resolve_data_dir(path, root=WORKSPACE_ROOT):
    """Canonical ``path``; refused unless it lies inside ``root``.

    Sync deletes and overwrites files below the data directory, so a path that
    escapes the workspace is rejected instead of trusted.
    """
    root = os.path.realpath(root)
    resolved = os.path.realpath(path)
    try:
        inside = os.path.commonpath([root, resolved]) == root
    except ValueError:  # different drives on Windows
        inside = False
    if not inside or resolved == root:
        raise ValueError(f"Data directory must be inside {root}: {path}")
    return resolved


def merge_iv_history(source_history, target_history):
    """Union the IV history of ``source_history`` into ``target_history``.

    Returns the number of observations added from the source.
    """
    target = IVHistoryStore(os.path.join(target_history, IV_HISTORY_DIRNAME)).load()
    target_legacy = os.path.join(target_history, LEGACY_FILENAME)
    target.merge(IVHistoryStore.from_legacy_file(target_legacy))
    added = target.merge(
        IVHistoryStore(os.path.join(source_history, IV_HISTORY_DIRNAME)).load()
    )
    added += target.merge(
        IVHistoryStore.from_legacy_file(os.path.join(source_history, LEGACY_FILENAME))
    )
    target.save()
    if os.path.exists(target_legacy):
        os.remove(target_legacy)
    return added


def copy_files(source_dir, target_dir, pattern="*.json", overwrite=False):
    """Copy matching files under ``source_dir``, keeping relative paths."""
    copied = 0
    for source in glob.glob(os.path.join(source_dir, "**", pattern), recursive=True):
        target = os.path.join(target_dir, os.path.relpath(source, source_dir))
        if not overwrite and os.path.exists(target):
            continue
        os.makedirs(os.path.dirname(target), exist_ok=True)
        shutil.copy2(source, target)
        copied += 1
    return copied


def sync(direction, data_dir, history_dir=LOCAL_HISTORY_DIR, output_dir=LOCAL_OUTPUT_DIR):
    """Copy between the local ``data/`` folders and a data-branch checkout."""
    remote_history = os.path.join(data_dir, "history")
    remote_output = os.path.join(data_dir, "output")
    if direction == "pull":
        src_history, dst_history = remote_history, history_dir
        src_output, dst_output = remote_output, output_dir
        overwrite = False
    elif direction == "push":
        src_history, dst_history = history_dir, remote_history
        src_output, dst_output = output_dir, remote_output
        overwrite = True
    else:
        raise ValueError(f"Unknown direction: {direction}")

    stats = {
        "iv_observations": merge_iv_history(src_history, dst_history),
        "scans": copy_files(
            os.path.join(src_history, "scans"),
            os.path.join(dst_history, "scans"),
            overwrite=overwrite,
        ),
        "outputs": copy_files(src_output, dst_output, overwrite=overwrite),
    }
    return stats


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("direction", choices=["pull", "push"])
    parser.add_argument(
        "data_dir",
        help="Checkout of the data branch (e.g. a git worktree) next to the repository.",
    )
    args = parser.parse_args()
    try:
        data_dir = resolve_data_dir(args.data_dir)
    except ValueError as e:
        parser.error(str(e))
    stats = sync(args.direction, data_dir)
    print(
        f"{args.direction}: {stats['iv_observations']} IV observations merged, "
        f"{stats['scans']} scans and {stats['outputs']} outputs copied."
    )


if __name__ == "__main__":
    main()
