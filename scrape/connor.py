"""Run unmodified Connor Python scrapers with the repository's data layout."""

import subprocess
import sys
from pathlib import Path
from tempfile import TemporaryDirectory


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = REPO_ROOT / "scrape" / "legacy" / "connor"
CHAMBERS = {"upper": "Senate", "lower": "House"}


def select_scripts(state: str, chamber: str = None):
    """Select primary scrapers; Missouri has a separate script per chamber."""
    state = state.upper()
    if chamber is not None and chamber not in CHAMBERS:
        raise ValueError("Chamber must be upper or lower.")
    if state == "CA":
        raise ValueError(
            "Connor's California file is an R data-cleaning script requiring "
            "downloaded Pubinfo files and RStudio; scrape-connor runs Python scrapers."
        )
    if chamber is not None and state != "MO":
        raise ValueError(
            "--chamber is supported only for MO's separate upper/lower scripts; "
            "other Connor scrapers handle their own chambers."
        )
    if state == "MO":
        chambers = [chamber] if chamber else ["lower", "upper"]
        names = [f"MO_State_Leg_Scrape_{CHAMBERS[c]}_Auto_CHP.py" for c in chambers]
    else:
        if len(state) != 2 or not state.isalpha():
            raise ValueError("State must be a two-letter postal code.")
        names = [f"{state}_State_Leg_Scrape_Auto_CHP.py"]
        if not (SCRIPT_DIR / names[0]).is_file():
            names = [f"{state}_State_Leg_Scrape_Auto.py"]
    scripts = [SCRIPT_DIR / name for name in names]
    if any(not path.is_file() for path in scripts):
        raise ValueError(f"No primary Connor Python scraper found for {state}.")
    return scripts


def run_script(script: Path, output_dir: Path):
    """Supply ../States/XX through a temporary symlink, in a child process.

    Relative reads, existence checks, and writes all see the real bill directory.
    The child uses the CLI's Python environment and the original script filename.
    """
    script = script.resolve()
    output_dir = output_dir.resolve()
    state = script.name[:2]
    output_dir.mkdir(parents=True, exist_ok=True)
    if state == "MO":
        for chamber in CHAMBERS.values():
            (output_dir / chamber).mkdir(exist_ok=True)
    with TemporaryDirectory(prefix="sles-connor-") as temporary:
        layout = Path(temporary)
        working_dir = layout / "Scrapers"
        working_dir.mkdir()
        (layout / "States").mkdir()
        (layout / "States" / state).symlink_to(output_dir, target_is_directory=True)
        result = subprocess.run([sys.executable, str(script)], cwd=working_dir)
    return result.returncode


def scrape_connor(state: str, chamber: str = None, dry_run: bool = False):
    state = state.upper()
    scripts = select_scripts(state, chamber)
    output_dir = REPO_ROOT / ".data" / state / "bill"
    print(f"Connor data directory: {output_dir}", flush=True)
    print("Sessions are selected by the original scripts.", flush=True)
    for script in scripts:
        print(f"Script: {script.name}", flush=True)
        if not dry_run:
            returncode = run_script(script, output_dir)
            if returncode:
                return returncode
    return 0
