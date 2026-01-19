import ctypes
import platform
import sys
from datetime import datetime
from pathlib import Path
from typing import TextIO

import qdarkstyle
from PySide6 import QtGui, QtWidgets, QtCore, QtWebEngineWidgets, QtWebEngineCore

from ..utility import TEMP_DIR
from .setting import load_font_family, load_font_size


#Redirect stdout/stderr to solve pythonw.exe startup problem
if sys.executable.endswith("pythonw.exe"):
    pythonw_log_folder: Path = TEMP_DIR.joinpath("pythonw_log")
    pythonw_log_folder.mkdir(parents=True, exist_ok=True)

    file_name: str = datetime.now().strftime("%Y%m%d_%H%M%S.log")
    file_path: Path = pythonw_log_folder.joinpath(file_name)

    f: TextIO = open(file_path, "w", buffering=1)  #Line buffering for real-time viewing
    sys.stdout = f
    sys.stderr = f


def create_qapp() -> QtWidgets.QApplication:
    """Create Qt application"""
    #Set style
    qapp: QtWidgets.QApplication = QtWidgets.QApplication(sys.argv)
    qapp.setStyleSheet(qdarkstyle.load_stylesheet(qt_api="pyside6"))

    #Set font
    font_family: str = load_font_family()
    font_size: int = load_font_size()
    font: QtGui.QFont = QtGui.QFont(font_family, font_size)
    qapp.setFont(font)

    #Settings icon
    icon_path: Path = Path(__file__).parent / "logo.ico"
    icon: QtGui.QIcon = QtGui.QIcon(str(icon_path))
    qapp.setWindowIcon(icon)

    #Set process ID
    if "Windows" in platform.uname():
        ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID("vnag")

    return qapp


__all__ = [
    "create_qapp",
    "QtCore",
    "QtGui",
    "QtWidgets",
    "QtWebEngineWidgets",
    "QtWebEngineCore",
]
