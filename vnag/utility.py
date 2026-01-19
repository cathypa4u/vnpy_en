import json
import sys

from pathlib import Path


def _get_agent_dir(temp_name: str) -> tuple[Path, Path]:
    """Get runtime directory"""
    cwd: Path = Path.cwd()
    temp_path: Path = cwd.joinpath(temp_name)

    #If the .vnag directory exists, use it as the runtime directory
    if temp_path.exists():
        return cwd, temp_path

    #Otherwise use the system user directory
    home_path: Path = Path.home()
    temp_path = home_path.joinpath(temp_name)

    #Create the .vnag directory if it does not exist
    if not temp_path.exists():
        temp_path.mkdir()

    return home_path, temp_path


#Get the running directory
WORKING_DIR, TEMP_DIR = _get_agent_dir(".vnag")

#Add to path
sys.path.append(str(WORKING_DIR))


def get_file_path(filename: str) -> Path:
    """Get temporary file path"""
    return TEMP_DIR.joinpath(filename)


def get_folder_path(folder_name: str) -> Path:
    """Get temporary folder path"""
    folder_path: Path = TEMP_DIR.joinpath(folder_name)
    if not folder_path.exists():
        folder_path.mkdir()
    return folder_path


def load_json(filename: str) -> dict:
    """Load JSON file"""
    filepath: Path = get_file_path(filename)

    if filepath.exists():
        with open(filepath, encoding="UTF-8") as f:
            data: dict = json.load(f)
        return data
    else:
        return {}


def save_json(filename: str, data: dict | list) -> None:
    """Save JSON file"""
    filepath: Path = get_file_path(filename)

    with open(filepath, mode="w+", encoding="UTF-8") as f:
        json.dump(
            data,
            f,
            indent=4,
            ensure_ascii=False
        )


def read_text_file(path: str | Path) -> str:
    """Read text files, using UTF-8 encoding"""
    p: Path = Path(path)
    text: str = p.read_text(encoding="utf-8")
    return text


def write_text_file(path: str | Path, content: str) -> None:
    """Write to a text file, using UTF-8 encoding (overwriting)"""
    p: Path = Path(path)
    p.write_text(content, encoding="utf-8")


PROFILE_DIR: Path = TEMP_DIR.joinpath("profile")
PROFILE_DIR.mkdir(parents=True, exist_ok=True)

SESSION_DIR: Path = TEMP_DIR.joinpath("session")
SESSION_DIR.mkdir(parents=True, exist_ok=True)
