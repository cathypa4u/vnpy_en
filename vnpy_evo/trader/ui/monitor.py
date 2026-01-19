"""
Basic widgets for UI.
"""

import csv
from datetime import datetime
from enum import Enum
from tzlocal import get_localzone_name

from qfluentwidgets import TableWidget, PushButton, LineEdit

from vnpy.trader.locale import _
from .qt import QtCore, QtGui, QtWidgets
from ..constant import Direction
from ..engine import MainEngine, Event, EventEngine
from ..event import (
    EVENT_QUOTE,
    EVENT_TICK,
    EVENT_TRADE,
    EVENT_ORDER,
    EVENT_POSITION,
    EVENT_ACCOUNT,
    EVENT_LOG
)
from ..object import (
    CancelRequest,
    ContractData,
    OrderData,
    QuoteData,
)
from ..utility import ZoneInfo


COLOR_LONG = QtGui.QColor("red")
COLOR_SHORT = QtGui.QColor("green")
COLOR_BID = QtGui.QColor("red")
COLOR_ASK = QtGui.QColor("green")
COLOR_BLACK = QtGui.QColor("black")


class BaseCell(QtWidgets.QTableWidgetItem):
    """
    General cell used in tablewidgets.
    """

    def __init__(self, content: object, data: object) -> None:
        """"""
        super().__init__()
        self.setTextAlignment(QtCore.Qt.AlignCenter)
        self.set_content(content, data)

    def set_content(self, content: object, data: object) -> None:
        """
        Set text content.
        """
        self.setText(str(content))
        self._data = data

    def get_data(self) -> object:
        """
        Get data object.
        """
        return self._data


class EnumCell(BaseCell):
    """
    Cell used for showing enum data.
    """

    def __init__(self, content: str, data: object) -> None:
        """"""
        super().__init__(content, data)

    def set_content(self, content: object, data: object) -> None:
        """
        Set text using enum.constant.value.
        """
        if content:
            super().set_content(content.value, data)


class DirectionCell(EnumCell):
    """
    Cell used for showing direction data.
    """

    def __init__(self, content: str, data: object) -> None:
        """"""
        super().__init__(content, data)

    def set_content(self, content: object, data: object) -> None:
        """
        Cell color is set according to direction.
        """
        super().set_content(content, data)

        if content is Direction.SHORT:
            self.setForeground(COLOR_SHORT)
        else:
            self.setForeground(COLOR_LONG)


class BidCell(BaseCell):
    """
    Cell used for showing bid price and volume.
    """

    def __init__(self, content: object, data: object) -> None:
        """"""
        super().__init__(content, data)

        self.setForeground(COLOR_BID)


class AskCell(BaseCell):
    """
    Cell used for showing ask price and volume.
    """

    def __init__(self, content: object, data: object) -> None:
        """"""
        super().__init__(content, data)

        self.setForeground(COLOR_ASK)


class PnlCell(BaseCell):
    """
    Cell used for showing pnl data.
    """

    def __init__(self, content: object, data: object) -> None:
        """"""
        super().__init__(content, data)

    def set_content(self, content: object, data: object) -> None:
        """
        Cell color is set based on whether pnl is
        positive or negative.
        """
        super().set_content(content, data)

        if str(content).startswith("-"):
            self.setForeground(COLOR_SHORT)
        else:
            self.setForeground(COLOR_LONG)


class TimeCell(BaseCell):
    """
    Cell used for showing time string from datetime object.
    """

    local_tz = ZoneInfo(get_localzone_name())

    def __init__(self, content: object, data: object) -> None:
        """"""
        super().__init__(content, data)

    def set_content(self, content: object, data: object) -> None:
        """"""
        if content is None:
            return

        content: datetime = content.astimezone(self.local_tz)
        timestamp: str = content.strftime("%H:%M:%S")

        millisecond: int = int(content.microsecond / 1000)
        if millisecond:
            timestamp = f"{timestamp}.{millisecond}"
        else:
            timestamp = f"{timestamp}.000"

        self.setText(timestamp)
        self._data = data


class DateCell(BaseCell):
    """
    Cell used for showing date string from datetime object.
    """

    def __init__(self, content: object, data: object) -> None:
        """"""
        super().__init__(content, data)

    def set_content(self, content: object, data: object) -> None:
        """"""
        if content is None:
            return

        self.setText(content.strftime("%Y-%m-%d"))
        self._data = data


class MsgCell(BaseCell):
    """
    Cell used for showing msg data.
    """

    def __init__(self, content: str, data: object) -> None:
        """"""
        super().__init__(content, data)
        self.setTextAlignment(QtCore.Qt.AlignLeft | QtCore.Qt.AlignVCenter)


class BaseMonitor(TableWidget):
    """
    Monitor data update.
    """

    event_type: str = ""
    data_key: str = ""
    sorting: bool = False
    headers: dict = {}

    signal: QtCore.Signal = QtCore.Signal(Event)

    def __init__(self, main_engine: MainEngine, event_engine: EventEngine) -> None:
        """"""
        super().__init__()

        self.main_engine: MainEngine = main_engine
        self.event_engine: EventEngine = event_engine
        self.cells: dict[str, dict] = {}

        self.init_ui()
        self.load_setting()
        self.register_event()

    def init_ui(self) -> None:
        """"""
        self.init_table()
        self.init_menu()

    def init_table(self) -> None:
        """
        Initialize table.
        """
        self.setColumnCount(len(self.headers))

        labels: list = [d["display"] for d in self.headers.values()]
        self.setHorizontalHeaderLabels(labels)

        self.verticalHeader().setVisible(False)
        self.setEditTriggers(self.EditTrigger.NoEditTriggers)
        self.setAlternatingRowColors(True)
        self.setSortingEnabled(self.sorting)

    def init_menu(self) -> None:
        """
        Create right click menu.
        """
        self.menu: QtWidgets.QMenu = QtWidgets.QMenu(self)

        resize_action: QtGui.QAction = QtWidgets.QAction("Resize columns", self)
        resize_action.triggered.connect(self.resize_columns)
        self.menu.addAction(resize_action)

        save_action: QtGui.QAction = QtWidgets.QAction("Save data", self)
        save_action.triggered.connect(self.save_csv)
        self.menu.addAction(save_action)

    def register_event(self) -> None:
        """
        Register event handler into event engine.
        """
        if self.event_type:
            self.signal.connect(self.process_event)
            self.event_engine.register(self.event_type, self.signal.emit)

    def process_event(self, event: Event) -> None:
        """
        Process new data from event and update into table.
        """
        # Disable sorting to prevent unwanted error.
        if self.sorting:
            self.setSortingEnabled(False)

        # Update data into table.
        data = event.data

        if not self.data_key:
            self.insert_new_row(data)
        else:
            key: str = data.__getattribute__(self.data_key)

            if key in self.cells:
                self.update_old_row(data)
            else:
                self.insert_new_row(data)

        # Enable sorting
        if self.sorting:
            self.setSortingEnabled(True)

    def insert_new_row(self, data: object) -> None:
        """
        Insert a new row at the top of table.
        """
        self.insertRow(0)

        row_cells: dict = {}
        for column, header in enumerate(self.headers.keys()):
            setting: dict = self.headers[header]

            content = data.__getattribute__(header)
            cell: QtWidgets.QTableWidgetItem = setting["cell"](content, data)
            self.setItem(0, column, cell)

            if setting["update"]:
                row_cells[header] = cell

        if self.data_key:
            key: str = data.__getattribute__(self.data_key)
            self.cells[key] = row_cells

    def update_old_row(self, data: object) -> None:
        """
        Update an old row in table.
        """
        key: str = data.__getattribute__(self.data_key)
        row_cells = self.cells[key]

        for header, cell in row_cells.items():
            content = data.__getattribute__(header)
            cell.set_content(content, data)

    def resize_columns(self) -> None:
        """
        Resize all columns according to contents.
        """
        self.horizontalHeader().resizeSections(QtWidgets.QHeaderView.ResizeToContents)

    def save_csv(self) -> None:
        """
        Save table data into a csv file
        """
        path, __ = QtWidgets.QFileDialog.getSaveFileName(
            self, "Save data", "", "CSV(*.csv)")

        if not path:
            return

        with open(path, "w") as f:
            writer = csv.writer(f, lineterminator="\n")

            headers: list = [d["display"] for d in self.headers.values()]
            writer.writerow(headers)

            for row in range(self.rowCount()):
                if self.isRowHidden(row):
                    continue

                row_data: list = []
                for column in range(self.columnCount()):
                    item: QtWidgets.QTableWidgetItem = self.item(row, column)
                    if item:
                        row_data.append(str(item.text()))
                    else:
                        row_data.append("")
                writer.writerow(row_data)

    def contextMenuEvent(self, event: QtGui.QContextMenuEvent) -> None:
        """
        Show menu with right click.
        """
        self.menu.popup(QtGui.QCursor.pos())

    def save_setting(self) -> None:
        """"""
        settings: QtCore.QSettings = QtCore.QSettings(self.__class__.__name__, "custom")
        settings.setValue("column_state", self.horizontalHeader().saveState())

    def load_setting(self) -> None:
        """"""
        settings: QtCore.QSettings = QtCore.QSettings(self.__class__.__name__, "custom")
        column_state = settings.value("column_state")

        if isinstance(column_state, QtCore.QByteArray):
            self.horizontalHeader().restoreState(column_state)
            self.horizontalHeader().setSortIndicator(-1, QtCore.Qt.AscendingOrder)


class TickMonitor(BaseMonitor):
    """
    Monitor for tick data.
    """

    event_type: str = EVENT_TICK
    data_key: str = "vt_symbol"
    sorting: bool = True

    headers: dict = {
        "symbol": {"display": _("Code"), "cell": BaseCell, "update": False},
        "exchange": {"display": _("Exchange"), "cell": EnumCell, "update": False},
        "name": {"display": _("Name"), "cell": BaseCell, "update": True},
        "last_price": {"display": _("Latest price"), "cell": BaseCell, "update": True},
        "volume": {"display": _("Trading volume"), "cell": BaseCell, "update": True},
        "open_price": {"display": _("Opening price"), "cell": BaseCell, "update": True},
        "high_price": {"display": _("Highest price"), "cell": BaseCell, "update": True},
        "low_price": {"display": _("Lowest price"), "cell": BaseCell, "update": True},
        "bid_price_1": {"display": _("Buy 1 price"), "cell": BidCell, "update": True},
        "bid_volume_1": {"display": _("Buy 1 quantity"), "cell": BidCell, "update": True},
        "ask_price_1": {"display": _("Sell ​​for 1 price"), "cell": AskCell, "update": True},
        "ask_volume_1": {"display": _("Sell ​​1 quantity"), "cell": AskCell, "update": True},
        "datetime": {"display": _("Time"), "cell": TimeCell, "update": True},
        "gateway_name": {"display": _("Interface"), "cell": BaseCell, "update": False},
    }


class LogMonitor(BaseMonitor):
    """
    Monitor for log data.
    """

    event_type: str = EVENT_LOG
    data_key: str = ""
    sorting: bool = False

    headers: dict = {
        "time": {"display": _("Time"), "cell": TimeCell, "update": False},
        "msg": {"display": _("Information"), "cell": MsgCell, "update": False},
        "gateway_name": {"display": _("Interface"), "cell": BaseCell, "update": False},
    }


class TradeMonitor(BaseMonitor):
    """
    Monitor for trade data.
    """

    event_type: str = EVENT_TRADE
    data_key: str = ""
    sorting: bool = True

    headers: dict = {
        "tradeid": {"display": _("Transaction number"), "cell": BaseCell, "update": False},
        "orderid": {"display": _("Entrustment number"), "cell": BaseCell, "update": False},
        "symbol": {"display": _("Code"), "cell": BaseCell, "update": False},
        "exchange": {"display": _("Exchange"), "cell": EnumCell, "update": False},
        "direction": {"display": _("Direction"), "cell": DirectionCell, "update": False},
        "offset": {"display": _("Open flat"), "cell": EnumCell, "update": False},
        "price": {"display": _("Price"), "cell": BaseCell, "update": False},
        "volume": {"display": _("Quantity"), "cell": BaseCell, "update": False},
        "datetime": {"display": _("Time"), "cell": TimeCell, "update": False},
        "gateway_name": {"display": _("Interface"), "cell": BaseCell, "update": False},
    }


class OrderMonitor(BaseMonitor):
    """
    Monitor for order data.
    """

    event_type: str = EVENT_ORDER
    data_key: str = "vt_orderid"
    sorting: bool = True

    headers: dict = {
        "orderid": {"display": _("Entrustment number"), "cell": BaseCell, "update": False},
        "reference": {"display": _("Source"), "cell": BaseCell, "update": False},
        "symbol": {"display": _("Code"), "cell": BaseCell, "update": False},
        "exchange": {"display": _("Exchange"), "cell": EnumCell, "update": False},
        "type": {"display": _("Type"), "cell": EnumCell, "update": False},
        "direction": {"display": _("Direction"), "cell": DirectionCell, "update": False},
        "offset": {"display": _("Open flat"), "cell": EnumCell, "update": False},
        "price": {"display": _("Price"), "cell": BaseCell, "update": False},
        "volume": {"display": _("Total quantity"), "cell": BaseCell, "update": True},
        "traded": {"display": _("Delivered"), "cell": BaseCell, "update": True},
        "status": {"display": _("State"), "cell": EnumCell, "update": True},
        "datetime": {"display": _("Time"), "cell": TimeCell, "update": True},
        "gateway_name": {"display": _("Interface"), "cell": BaseCell, "update": False},
    }

    def init_ui(self) -> None:
        """
        Connect signal.
        """
        super().init_ui()

        self.setToolTip(_("Double-click the cell to withdraw orders"))
        self.itemDoubleClicked.connect(self.cancel_order)

    def cancel_order(self, cell: BaseCell) -> None:
        """
        Cancel order if cell double clicked.
        """
        order: OrderData = cell.get_data()
        req: CancelRequest = order.create_cancel_request()
        self.main_engine.cancel_order(req, order.gateway_name)


class PositionMonitor(BaseMonitor):
    """
    Monitor for position data.
    """

    event_type: str = EVENT_POSITION
    data_key: str = "vt_positionid"
    sorting: bool = True

    headers: dict = {
        "symbol": {"display": _("Code"), "cell": BaseCell, "update": False},
        "exchange": {"display": _("Exchange"), "cell": EnumCell, "update": False},
        "direction": {"display": _("Direction"), "cell": DirectionCell, "update": False},
        "volume": {"display": _("Quantity"), "cell": BaseCell, "update": True},
        "yd_volume": {"display": _("Yesterday's warehouse"), "cell": BaseCell, "update": True},
        "frozen": {"display": _("Freeze"), "cell": BaseCell, "update": True},
        "price": {"display": _("Average price"), "cell": BaseCell, "update": True},
        "pnl": {"display": _("Profit and loss"), "cell": PnlCell, "update": True},
        "gateway_name": {"display": _("Interface"), "cell": BaseCell, "update": False},
    }


class AccountMonitor(BaseMonitor):
    """
    Monitor for account data.
    """

    event_type: str = EVENT_ACCOUNT
    data_key: str = "vt_accountid"
    sorting: bool = True

    headers: dict = {
        "accountid": {"display": _("Account"), "cell": BaseCell, "update": False},
        "balance": {"display": _("Balance"), "cell": BaseCell, "update": True},
        "frozen": {"display": _("Freeze"), "cell": BaseCell, "update": True},
        "available": {"display": _("Available"), "cell": BaseCell, "update": True},
        "gateway_name": {"display": _("Interface"), "cell": BaseCell, "update": False},
    }


class QuoteMonitor(BaseMonitor):
    """
    Monitor for quote data.
    """

    event_type: str = EVENT_QUOTE
    data_key: str = "vt_quoteid"
    sorting: bool = True

    headers: dict = {
        "quoteid": {"display": _("Quote number"), "cell": BaseCell, "update": False},
        "reference": {"display": _("Source"), "cell": BaseCell, "update": False},
        "symbol": {"display": _("Code"), "cell": BaseCell, "update": False},
        "exchange": {"display": _("Exchange"), "cell": EnumCell, "update": False},
        "bid_offset": {"display": _("Buy open flat"), "cell": EnumCell, "update": False},
        "bid_volume": {"display": _("Buy quantity"), "cell": BidCell, "update": False},
        "bid_price": {"display": _("Purchase price"), "cell": BidCell, "update": False},
        "ask_price": {"display": _("Sell ​​price"), "cell": AskCell, "update": False},
        "ask_volume": {"display": _("Selling volume"), "cell": AskCell, "update": False},
        "ask_offset": {"display": _("Sell ​​open flat"), "cell": EnumCell, "update": False},
        "status": {"display": _("State"), "cell": EnumCell, "update": True},
        "datetime": {"display": _("Time"), "cell": TimeCell, "update": True},
        "gateway_name": {"display": _("Interface"), "cell": BaseCell, "update": False},
    }

    def init_ui(self):
        """
        Connect signal.
        """
        super().init_ui()

        self.setToolTip(_("Double-click the cell to cancel the quote"))
        self.itemDoubleClicked.connect(self.cancel_quote)

    def cancel_quote(self, cell: BaseCell) -> None:
        """
        Cancel quote if cell double clicked.
        """
        quote: QuoteData = cell.get_data()
        req: CancelRequest = quote.create_cancel_request()
        self.main_engine.cancel_quote(req, quote.gateway_name)


class ActiveOrderMonitor(OrderMonitor):
    """
    Monitor which shows active order only.
    """

    def process_event(self, event) -> None:
        """
        Hides the row if order is not active.
        """
        super().process_event(event)

        order: OrderData = event.data
        row_cells: dict = self.cells[order.vt_orderid]
        row: int = self.row(row_cells["volume"])

        if order.is_active():
            self.showRow(row)
        else:
            self.hideRow(row)


class ContractManager(QtWidgets.QWidget):
    """
    Query contract data available to trade in system.
    """

    headers: dict[str, str] = {
        "vt_symbol": _("Local code"),
        "symbol": _("Code"),
        "exchange": _("Exchange"),
        "name": _("Name"),
        "product": _("Contract classification"),
        "size": _("Contract multiplier"),
        "pricetick": _("Price jump"),
        "min_volume": _("Minimum entrustment amount"),
        "option_portfolio": _("Options products"),
        "option_expiry": _("Option expiration date"),
        "option_strike": _("Option exercise price"),
        "option_type": _("Option type"),
        "gateway_name": _("Transaction interface"),
    }

    def __init__(self, main_engine: MainEngine, event_engine: EventEngine) -> None:
        super().__init__()

        self.main_engine: MainEngine = main_engine
        self.event_engine: EventEngine = event_engine

        self.init_ui()

    def init_ui(self) -> None:
        """"""
        self.setWindowTitle(_("Contract query"))
        self.resize(1000, 600)

        self.filter_line: LineEdit = LineEdit()
        self.filter_line.setPlaceholderText(_("Enter the contract code or exchange, leave it blank to query all contracts"))

        self.button_show: PushButton = PushButton(_("Query"))
        self.button_show.clicked.connect(self.show_contracts)

        labels: list = []
        for name, display in self.headers.items():
            label: str = f"{display}"
            labels.append(label)

        self.contract_table: TableWidget = TableWidget()
        self.contract_table.setColumnCount(len(self.headers))
        self.contract_table.setHorizontalHeaderLabels(labels)
        self.contract_table.verticalHeader().setVisible(False)
        self.contract_table.setEditTriggers(self.contract_table.EditTrigger.NoEditTriggers)
        self.contract_table.setAlternatingRowColors(True)

        hbox: QtWidgets.QHBoxLayout = QtWidgets.QHBoxLayout()
        hbox.addWidget(self.filter_line)
        hbox.addWidget(self.button_show)

        vbox: QtWidgets.QVBoxLayout = QtWidgets.QVBoxLayout()
        vbox.addLayout(hbox)
        vbox.addWidget(self.contract_table)

        self.setLayout(vbox)

    def show_contracts(self) -> None:
        """
        Show contracts by symbol
        """
        flt: str = str(self.filter_line.text())

        all_contracts: list[ContractData] = self.main_engine.get_all_contracts()
        if flt:
            contracts: list[ContractData] = [
                contract for contract in all_contracts if flt in contract.vt_symbol
            ]
        else:
            contracts: list[ContractData] = all_contracts

        self.contract_table.clearContents()
        self.contract_table.setRowCount(len(contracts))

        for row, contract in enumerate(contracts):
            for column, name in enumerate(self.headers.keys()):
                value: object = getattr(contract, name)

                if value in {None, 0, 0.0}:
                    value = ""

                if isinstance(value, Enum):
                    cell: EnumCell = EnumCell(value, contract)
                elif isinstance(value, datetime):
                    cell: DateCell = DateCell(value, contract)
                else:
                    cell: BaseCell = BaseCell(value, contract)
                self.contract_table.setItem(row, column, cell)

        self.contract_table.resizeColumnsToContents()
