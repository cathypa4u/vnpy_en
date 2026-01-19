import csv
from functools import partial
from datetime import datetime

from vnpy.event import EventEngine, Event
from vnpy.trader.engine import MainEngine, LogData
from vnpy.trader.ui import QtWidgets, QtCore

from ..engine import (
    AlgoEngine,
    AlgoTemplate,
    APP_NAME,
    EVENT_ALGO_LOG,
    EVENT_ALGO_UPDATE,
    AlgoStatus,
    Direction,
    Offset
)
from .display import NAME_DISPLAY_MAP


class AlgoWidget(QtWidgets.QWidget):
    """Algorithm Start Widget"""

    def __init__(
        self,
        algo_engine: AlgoEngine,
        algo_template: type[AlgoTemplate]
    ) -> None:
        """Constructor"""
        super().__init__()

        self.algo_engine: AlgoEngine = algo_engine
        self.template_name: str = algo_template.__name__

        self.default_setting: dict = {
            "vt_symbol": "",
            "direction": [
                Direction.LONG.value,
                Direction.SHORT.value
            ],
            "offset": [
                Offset.NONE.value,
                Offset.OPEN.value,
                Offset.CLOSE.value,
                Offset.CLOSETODAY.value,
                Offset.CLOSEYESTERDAY.value
            ],
            "price": 0.0,
            "volume": 0,
        }
        self.default_setting.update(algo_template.default_setting)

        self.widgets: dict[str, QtWidgets.QWidget] = {}

        self.init_ui()

    def init_ui(self) -> None:
        """Initialize input boxes and form layout using default configuration"""
        self.setMaximumWidth(400)

        form: QtWidgets.QFormLayout = QtWidgets.QFormLayout()

        for field_name, field_value in self.default_setting.items():
            field_type: object = type(field_value)

            if field_type is list:
                widget: QtWidgets.QComboBox | QtWidgets.QLineEdit = QtWidgets.QComboBox()
                widget.addItems(field_value)
            else:
                widget = QtWidgets.QLineEdit()

            display_name: str = NAME_DISPLAY_MAP.get(field_name, field_name)

            form.addRow(display_name, widget)
            self.widgets[field_name] = (widget, field_type)

        start_algo_button: QtWidgets.QPushButton = QtWidgets.QPushButton("Start Algorithm")
        start_algo_button.clicked.connect(self.start_algo)
        form.addRow(start_algo_button)

        load_csv_button: QtWidgets.QPushButton = QtWidgets.QPushButton("Start from CSV")
        load_csv_button.clicked.connect(self.load_csv)
        form.addRow(load_csv_button)

        for button in [
            start_algo_button,
            load_csv_button
        ]:
            button.setFixedHeight(button.sizeHint().height() * 2)

        self.setLayout(form)

    def load_csv(self) -> None:
        """Load algorithm configuration from CSV file"""
        # Get csv path from dialog
        path, type_ = QtWidgets.QFileDialog.getOpenFileName(
            self,
            "Load Algorithm Configuration",
            "",
            "CSV(*.csv)"
        )

        if not path:
            return

        # Create csv dictReader
        with open(path) as f:
            buf: list = [line for line in f]
            reader: csv.DictReader = csv.DictReader(buf)

        if not reader.fieldnames:
            QtWidgets.QMessageBox.warning(
                self,
                "CSV File Format Error",
                "CSV file is empty or format is incorrect, column names cannot be recognized. Please check if the file contains a header row and ensure the format is correct."
            )
            return

        # Check for missing fields in csv file
        for field_name in self.widgets.keys():
            if field_name not in reader.fieldnames:
                QtWidgets.QMessageBox.warning(
                    self,
                    "Missing Field",
                    f"CSV file is missing required field {field_name} for algorithm {self.template_name}"
                )
                return

        settings: list = []

        for d in reader:
            # Initialize algorithm configuration with template name
            setting: dict = {}

            # Read content of each field in the csv file
            for field_name, tp in self.widgets.items():
                _widget, field_type = tp
                field_text: str = d[field_name]

                if field_type is list:
                    field_value = field_text
                else:
                    try:
                        field_value = field_type(field_text)
                    except ValueError:
                        display_name: str = NAME_DISPLAY_MAP.get(field_name, field_name)
                        QtWidgets.QMessageBox.warning(
                            self,
                            "Parameter Error",
                            f"Parameter type for {display_name} should be {field_type}, please check!"
                        )
                        return

                setting[field_name] = field_value

            # Add setting to settings
            settings.append(setting)

        # Start algorithm when no error occurs
        for setting in settings:
            self.algo_engine.start_algo(
                template_name=self.template_name,
                vt_symbol=setting.pop("vt_symbol"),
                direction=Direction(setting.pop("direction")),
                offset=Offset(setting.pop("offset")),
                price=setting.pop("price"),
                volume=setting.pop("volume"),
                setting=setting
            )

    def get_setting(self) -> dict:
        """Get current configuration"""
        setting: dict = {}

        for field_name, tp in self.widgets.items():
            widget, field_type = tp
            if field_type is list:
                field_value: str = str(widget.currentText())
            else:
                try:
                    field_value = field_type(widget.text())
                except ValueError:
                    display_name: str = NAME_DISPLAY_MAP.get(field_name, field_name)
                    QtWidgets.QMessageBox.warning(
                        self,
                        "Parameter Error",
                        f"{display_name} parameter type should be {field_type}, please check!"
                    )
                    return {}

            setting[field_name] = field_value

        return setting

    def start_algo(self) -> None:
        """Start trading algorithm"""
        setting: dict = self.get_setting()
        if not setting:
            return

        self.algo_engine.start_algo(
            template_name=self.template_name,
            vt_symbol=setting.pop("vt_symbol"),
            direction=Direction(setting.pop("direction")),
            offset=Offset(setting.pop("offset")),
            price=setting.pop("price"),
            volume=setting.pop("volume"),
            setting=setting
        )


class AlgoMonitor(QtWidgets.QTableWidget):
    """Algorithm Monitoring Component"""

    algo_signal: QtCore.Signal = QtCore.Signal(Event)

    def __init__(
        self,
        algo_engine: AlgoEngine,
        event_engine: EventEngine,
        mode_active: bool
    ):
        """Constructor"""
        super().__init__()

        self.algo_engine: AlgoEngine = algo_engine
        self.event_engine: EventEngine = event_engine
        self.mode_active: bool = mode_active

        self.algo_cells: dict = {}

        self.init_ui()
        self.register_event()

    def init_ui(self) -> None:
        """Initialize interface"""
        labels: list = [
            "",
            "",
            "Algo",
            "Local Code",
            "Direction",
            "Offset",
            "Price",
            "Total Volume",
            "Traded Volume",
            "Remaining Volume",
            "Average Traded Price",
            "Status",
            "Parameters",
            "Variables"
        ]
        self.setColumnCount(len(labels))
        self.setHorizontalHeaderLabels(labels)
        self.verticalHeader().setVisible(False)
        self.setEditTriggers(self.EditTrigger.NoEditTriggers)

        self.verticalHeader().setSectionResizeMode(
            QtWidgets.QHeaderView.ResizeMode.ResizeToContents
        )

        for column in range(12, 14):
            self.horizontalHeader().setSectionResizeMode(
                column,
                QtWidgets.QHeaderView.ResizeMode.Stretch
            )
        self.setWordWrap(True)

        if not self.mode_active:
            self.hideColumn(0)
            self.hideColumn(1)

    def register_event(self) -> None:
        """Register event listeners"""
        self.algo_signal.connect(self.process_algo_event)
        self.event_engine.register(EVENT_ALGO_UPDATE, self.algo_signal.emit)

    def process_algo_event(self, event: Event) -> None:
        """Process algorithm update event"""
        data: dict = event.data

        # Read algorithm standard parameters and get content cell dictionary
        algo_name: str = data["algo_name"]
        vt_symbol: str = data["vt_symbol"]
        direction: Direction = data["direction"]
        offset: Offset = data["offset"]
        price: float = data["price"]
        volume: float = data["volume"]

        cells: dict = self.get_algo_cells(algo_name, vt_symbol, direction, offset, price, volume)

        # Read algorithm standard variables and update to content cells
        traded_price: float = data["traded_price"]
        traded: float = data["traded"]
        left: float = data["left"]
        status: AlgoStatus = data["status"]

        cells["status"].setText(status.value)
        cells["traded_price"].setText(str(traded_price))
        cells["traded"].setText(str(traded))
        cells["left"].setText(str(left))

        # Read algorithm custom parameters and variables, and display in cells
        parameters: dict = data["parameters"]
        cells["parameters"].setText(to_text(parameters))

        variables: dict = data["variables"]
        cells["variables"].setText(to_text(variables))

        # Decide whether to hide based on display mode
        row: int = self.row(cells["variables"])
        active: bool = status not in [AlgoStatus.STOPPED, AlgoStatus.FINISHED]

        if self.mode_active:
            if active:
                self.showRow(row)
            else:
                self.hideRow(row)
        else:
            if active:
                self.hideRow(row)
            else:
                self.showRow(row)

    def stop_algo(self, algo_name: str) -> None:
        """Stop algorithm"""
        self.algo_engine.stop_algo(algo_name)

    def switch(self, algo_name: str) -> None:
        """Algorithm switch adjustment (Pause/Resume)"""
        button: QtWidgets.QPushButton = self.algo_cells[algo_name]["button"]

        if button.text() == "Pause":
            self.algo_engine.pause_algo(algo_name)
            button.setText("Resume")
        else:
            self.algo_engine.resume_algo(algo_name)
            button.setText("Pause")

        self.algo_cells[algo_name]["button"] = button

    def get_algo_cells(
        self,
        algo_name: str,
        vt_symbol: str,
        direction: Direction,
        offset: Offset,
        price: float,
        volume: float
    ) -> dict[str, QtWidgets.QTableWidgetItem]:
        """Get cell dictionary corresponding to the algorithm"""
        cells: dict | None = self.algo_cells.get(algo_name, None)

        if not cells:
            stop_func = partial(self.stop_algo, algo_name=algo_name)
            stop_button: QtWidgets.QPushButton = QtWidgets.QPushButton("Stop")
            stop_button.clicked.connect(stop_func)

            # Initialize with Pause button
            switch_func = partial(self.switch, algo_name=algo_name)
            switch_button: QtWidgets.QPushButton = QtWidgets.QPushButton("Pause")
            switch_button.clicked.connect(switch_func)

            parameters_cell: QtWidgets.QTableWidgetItem = QtWidgets.QTableWidgetItem()
            variables_cell: QtWidgets.QTableWidgetItem = QtWidgets.QTableWidgetItem()

            self.insertRow(0)
            self.setCellWidget(0, 0, stop_button)
            self.setCellWidget(0, 1, switch_button)
            self.setItem(0, 12, parameters_cell)
            self.setItem(0, 13, variables_cell)

            cells = {
                "parameters": parameters_cell,
                "variables": variables_cell,
                "button": switch_button        # Cache button corresponding to algo_name to update button status
            }

            items: list[tuple[int, str, str]] = [
                (2, "name", algo_name),
                (3, "vt_symbol", vt_symbol),
                (4, "direction", direction.value),
                (5, "offset", offset.value),
                (6, "price", str(price)),
                (7, "volume", str(volume)),
                (8, "traded", ""),
                (9, "left", ""),
                (10, "traded_price", ""),
                (11, "status", ""),
            ]

            for column, name, content in items:
                cell: QtWidgets.QTableWidgetItem = QtWidgets.QTableWidgetItem(content)
                cell.setTextAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)

                self.setItem(0, column, cell)
                cells[name] = cell

            self.algo_cells[algo_name] = cells

        return cells


class ActiveAlgoMonitor(AlgoMonitor):
    """Active Algorithm Monitoring Component"""

    def __init__(self, algo_engine: AlgoEngine, event_engine: EventEngine) -> None:
        """"""
        super().__init__(algo_engine, event_engine, True)


class InactiveAlgoMonitor(AlgoMonitor):
    """Finished Algorithm Monitoring Component"""

    def __init__(self, algo_engine: AlgoEngine, event_engine: EventEngine) -> None:
        """"""
        super().__init__(algo_engine, event_engine, False)


class LogMonitor(QtWidgets.QTableWidget):
    """Log Component"""

    signal: QtCore.Signal = QtCore.Signal(Event)

    def __init__(self, event_engine: EventEngine) -> None:
        """Constructor"""
        super().__init__()

        self.event_engine: EventEngine = event_engine

        self.init_ui()
        self.register_event()

    def init_ui(self) -> None:
        """Initialize interface"""
        labels: list = [
            "Time",
            "Information"
        ]
        self.setColumnCount(len(labels))
        self.setHorizontalHeaderLabels(labels)
        self.setEditTriggers(self.EditTrigger.NoEditTriggers)
        self.verticalHeader().setVisible(False)
        self.verticalHeader().setSectionResizeMode(QtWidgets.QHeaderView.ResizeMode.ResizeToContents)
        self.horizontalHeader().setSectionResizeMode(1, QtWidgets.QHeaderView.ResizeMode.Stretch)
        self.setWordWrap(True)

    def register_event(self) -> None:
        """Register event listeners"""
        self.signal.connect(self.process_log_event)

        self.event_engine.register(EVENT_ALGO_LOG, self.signal.emit)

    def process_log_event(self, event: Event) -> None:
        """Process log event"""
        log: LogData = event.data
        msg: str = log.msg
        timestamp: str = datetime.now().strftime("%H:%M:%S")

        timestamp_cell: QtWidgets.QTableWidgetItem = QtWidgets.QTableWidgetItem(timestamp)
        msg_cell: QtWidgets.QTableWidgetItem = QtWidgets.QTableWidgetItem(msg)

        self.insertRow(0)
        self.setItem(0, 0, timestamp_cell)
        self.setItem(0, 1, msg_cell)


class AlgoManager(QtWidgets.QWidget):
    """Algorithm Trading Management Widget"""

    def __init__(self, main_engine: MainEngine, event_engine: EventEngine) -> None:
        """"""
        super().__init__()

        self.main_engine: MainEngine = main_engine
        self.event_engine: EventEngine = event_engine
        self.algo_engine: AlgoEngine = main_engine.get_engine(APP_NAME)

        self.algo_widgets: dict[str, AlgoWidget] = {}

        self.init_ui()
        self.algo_engine.init_engine()

    def init_ui(self) -> None:
        """"""
        self.setWindowTitle("Algorithm Trading")

        # Left control widget
        self.template_combo: QtWidgets.QComboBox = QtWidgets.QComboBox()
        self.template_combo.currentIndexChanged.connect(self.show_algo_widget)

        form: QtWidgets.QFormLayout = QtWidgets.QFormLayout()
        form.addRow("Algorithm", self.template_combo)
        widget: QtWidgets.QWidget = QtWidgets.QWidget()
        widget.setLayout(form)

        vbox: QtWidgets.QVBoxLayout = QtWidgets.QVBoxLayout()
        vbox.addWidget(widget)

        algo_templates: dict = self.algo_engine.get_algo_template()
        for algo_template in algo_templates.values():
            widget = AlgoWidget(self.algo_engine, algo_template)
            vbox.addWidget(widget)

            template_name: str = algo_template.__name__
            display_name: str = algo_template.display_name

            self.algo_widgets[template_name] = widget
            self.template_combo.addItem(display_name, template_name)

        vbox.addStretch()

        stop_all_button: QtWidgets.QPushButton = QtWidgets.QPushButton("Stop All")
        stop_all_button.setFixedHeight(stop_all_button.sizeHint().height() * 2)
        stop_all_button.clicked.connect(self.algo_engine.stop_all)

        vbox.addWidget(stop_all_button)

        # Right monitoring widget
        active_algo_monitor: ActiveAlgoMonitor = ActiveAlgoMonitor(
            self.algo_engine, self.event_engine
        )
        inactive_algo_monitor: InactiveAlgoMonitor = InactiveAlgoMonitor(
            self.algo_engine, self.event_engine
        )
        tab1: QtWidgets.QTabWidget = QtWidgets.QTabWidget()
        tab1.addTab(active_algo_monitor, "Running")
        tab1.addTab(inactive_algo_monitor, "Finished")

        log_monitor: LogMonitor = LogMonitor(self.event_engine)
        tab2: QtWidgets.QTabWidget = QtWidgets.QTabWidget()
        tab2.addTab(log_monitor, "Log")

        vbox2: QtWidgets.QVBoxLayout = QtWidgets.QVBoxLayout()
        vbox2.addWidget(tab1)
        vbox2.addWidget(tab2)

        hbox2: QtWidgets.QHBoxLayout = QtWidgets.QHBoxLayout()
        hbox2.addLayout(vbox)
        hbox2.addLayout(vbox2)
        self.setLayout(hbox2)

        self.show_algo_widget()

    def show_algo_widget(self) -> None:
        """"""
        ix: int = self.template_combo.currentIndex()
        current_name: object = self.template_combo.itemData(ix)

        for template_name, widget in self.algo_widgets.items():
            if template_name == current_name:
                widget.show()
            else:
                widget.hide()

    def show(self) -> None:
        """"""
        self.showMaximized()


def to_text(data: dict) -> str:
    """Convert dictionary data to string data"""
    buf: list = []
    for key, value in data.items():
        key = NAME_DISPLAY_MAP.get(key, key)
        buf.append(f"{key}:{value}")
    text: str = ";".join(buf)
    return text