from pathlib import Path
from typing import cast

from typing import cast

from vnpy.event import EventEngine, Event
from vnpy.trader.engine import MainEngine
from vnpy.trader.ui import QtWidgets, QtCore, QtGui
from vnpy.trader.constant import Direction, Offset, OrderType
from vnpy.trader.object import OrderRequest, CancelRequest, ContractData, TickData
from vnpy.trader.event import EVENT_TICK
from vnpy.trader.utility import get_digits

from ..base import APP_NAME, EVENT_OPTION_NEW_PORTFOLIO, EVENT_OPTION_RISK_NOTICE, PortfolioData, UnderlyingData
from ..engine import OptionEngine, OptionHedgeEngine, PRICING_MODELS
from .monitor import (
    OptionMarketMonitor, OptionGreeksMonitor, OptionChainMonitor,
    MonitorCell
)
from .chart import OptionVolatilityChart, ScenarioAnalysisChart
from .manager import ElectronicEyeManager, PricingVolatilityManager


class OptionManager(QtWidgets.QWidget):
    """"""
    signal_new_portfolio: QtCore.Signal = QtCore.Signal(Event)

    def __init__(self, main_engine: MainEngine, event_engine: EventEngine) -> None:
        """"""
        super().__init__()

        self.main_engine: MainEngine = main_engine
        self.event_engine: EventEngine = event_engine
        self.option_engine: OptionEngine = cast(OptionEngine, main_engine.get_engine(APP_NAME))

        self.portfolio_name: str = ""

        self.market_monitor: OptionMarketMonitor
        self.greeks_monitor: OptionGreeksMonitor
        self.volatility_chart: OptionVolatilityChart
        self.chain_monitor: OptionChainMonitor
        self.manual_trader: OptionManualTrader
        self.hedge_widget: OptionHedgeWidget
        self.scenario_chart: ScenarioAnalysisChart
        self.eye_manager: ElectronicEyeManager
        self.pricing_manager: PricingVolatilityManager

        self.init_ui()
        self.register_event()

    def init_ui(self) -> None:
        """"""
        self.setWindowTitle("OptionMaster")

        self.portfolio_combo: QtWidgets.QComboBox = QtWidgets.QComboBox()
        self.portfolio_combo.setFixedWidth(150)
        self.update_portfolio_combo()

        self.portfolio_button = QtWidgets.QPushButton("Configuration")
        self.portfolio_button.clicked.connect(self.open_portfolio_dialog)

        self.market_button = QtWidgets.QPushButton("T-shaped quotation")
        self.greeks_button = QtWidgets.QPushButton("Greek position value")
        self.chain_button = QtWidgets.QPushButton("Monitoring of price rise and discount")
        self.manual_button = QtWidgets.QPushButton("Fast transaction")
        self.volatility_button = QtWidgets.QPushButton("Volatility curve")
        self.hedge_button = QtWidgets.QPushButton("Delta Hedge")
        self.scenario_button = QtWidgets.QPushButton("Scenario analysis")
        self.eye_button = QtWidgets.QPushButton("Electronic eye")
        self.pricing_button = QtWidgets.QPushButton("Volatility Management")
        self.risk_button = QtWidgets.QPushButton("Risk monitoring")

        for button in [
            self.market_button,
            self.greeks_button,
            self.chain_button,
            self.manual_button,
            self.volatility_button,
            self.hedge_button,
            self.scenario_button,
            self.eye_button,
            self.pricing_button,
            self.risk_button
        ]:
            button.setEnabled(False)

        hbox: QtWidgets.QHBoxLayout = QtWidgets.QHBoxLayout()
        hbox.addWidget(QtWidgets.QLabel("Options products"))
        hbox.addWidget(self.portfolio_combo)
        hbox.addWidget(self.portfolio_button)
        hbox.addWidget(self.market_button)
        hbox.addWidget(self.greeks_button)
        hbox.addWidget(self.manual_button)
        hbox.addWidget(self.chain_button)
        hbox.addWidget(self.volatility_button)
        hbox.addWidget(self.hedge_button)
        hbox.addWidget(self.scenario_button)
        hbox.addWidget(self.pricing_button)
        hbox.addWidget(self.eye_button)
        hbox.addWidget(self.risk_button)

        self.setLayout(hbox)

    def register_event(self) -> None:
        """"""
        self.signal_new_portfolio.connect(self.process_new_portfolio_event)

        self.event_engine.register(EVENT_OPTION_NEW_PORTFOLIO, self.signal_new_portfolio.emit)

    def process_new_portfolio_event(self, event: Event) -> None:
        """"""
        self.update_portfolio_combo()

    def update_portfolio_combo(self) -> None:
        """"""
        if not self.portfolio_combo.isEnabled():
            return

        self.portfolio_combo.clear()
        portfolio_names: list = self.option_engine.get_portfolio_names()
        self.portfolio_combo.addItems(portfolio_names)

    def open_portfolio_dialog(self) -> None:
        """"""
        portfolio_name: str = self.portfolio_combo.currentText()
        if not portfolio_name:
            return

        self.portfolio_name = portfolio_name

        dialog: PortfolioDialog = PortfolioDialog(self.option_engine, portfolio_name)
        result: int = dialog.exec_()

        if result == dialog.DialogCode.Accepted:
            self.portfolio_combo.setEnabled(False)
            self.portfolio_button.setEnabled(False)

            self.init_widgets()

    def init_widgets(self) -> None:
        """"""
        self.market_monitor = OptionMarketMonitor(self.option_engine, self.portfolio_name)
        self.greeks_monitor = OptionGreeksMonitor(self.option_engine, self.portfolio_name)
        self.volatility_chart = OptionVolatilityChart(self.option_engine, self.portfolio_name)
        self.chain_monitor = OptionChainMonitor(self.option_engine, self.portfolio_name)
        self.manual_trader = OptionManualTrader(self.option_engine, self.portfolio_name)
        self.hedge_widget = OptionHedgeWidget(self.option_engine, self.portfolio_name)
        self.scenario_chart = ScenarioAnalysisChart(self.option_engine, self.portfolio_name)
        self.eye_manager = ElectronicEyeManager(self.option_engine, self.portfolio_name)
        self.pricing_manager = PricingVolatilityManager(self.option_engine, self.portfolio_name)
        self.risk_widget = OptionRiskWidget(self.option_engine)

        self.market_monitor.itemDoubleClicked.connect(self.manual_trader.update_symbol)

        self.market_button.clicked.connect(self.market_monitor.show)
        self.greeks_button.clicked.connect(self.greeks_monitor.show)
        self.manual_button.clicked.connect(self.manual_trader.show)
        self.chain_button.clicked.connect(self.chain_monitor.show)
        self.volatility_button.clicked.connect(self.volatility_chart.show)
        self.scenario_button.clicked.connect(self.scenario_chart.show)
        self.hedge_button.clicked.connect(self.hedge_widget.show)
        self.eye_button.clicked.connect(self.eye_manager.show)
        self.pricing_button.clicked.connect(self.pricing_manager.show)
        self.risk_button.clicked.connect(self.risk_widget.show)

        for button in [
            self.market_button,
            self.greeks_button,
            self.chain_button,
            self.manual_button,
            self.volatility_button,
            self.scenario_button,
            self.hedge_button,
            self.eye_button,
            self.pricing_button,
            self.risk_button
        ]:
            button.setEnabled(True)

    def closeEvent(self, event: QtGui.QCloseEvent) -> None:
        """"""
        if hasattr(self, "market_monitor"):
            self.market_monitor.close()
            self.greeks_monitor.close()
            self.volatility_chart.close()
            self.chain_monitor.close()
            self.manual_trader.close()
            self.hedge_widget.close()
            self.scenario_chart.close()
            self.eye_manager.close()
            self.pricing_manager.close()
            self.risk_widget.close()

        event.accept()


class PortfolioDialog(QtWidgets.QDialog):
    """"""

    def __init__(self, option_engine: OptionEngine, portfolio_name: str):
        """"""
        super().__init__()

        self.option_engine: OptionEngine = option_engine
        self.portfolio_name: str = portfolio_name

        self.init_ui()

    def init_ui(self) -> None:
        """"""
        self.setWindowTitle(f"{self.portfolio_name}portfolio configuration")

        portfolio_setting: dict = self.option_engine.get_portfolio_setting(
            self.portfolio_name
        )

        form: QtWidgets.QFormLayout = QtWidgets.QFormLayout()

        # Model Combo
        self.model_name_combo: QtWidgets.QComboBox = QtWidgets.QComboBox()
        self.model_name_combo.addItems(list(PRICING_MODELS.keys()))

        model_name: str = portfolio_setting.get("model_name", "")
        if model_name:
            self.model_name_combo.setCurrentIndex(
                self.model_name_combo.findText(model_name)
            )

        form.addRow("Pricing model", self.model_name_combo)

        # Interest rate spin
        self.interest_rate_spin: QtWidgets.QDoubleSpinBox = QtWidgets.QDoubleSpinBox()
        self.interest_rate_spin.setMinimum(0)
        self.interest_rate_spin.setMaximum(20)
        self.interest_rate_spin.setDecimals(1)
        self.interest_rate_spin.setSuffix("%")

        interest_rate: float = portfolio_setting.get("interest_rate", 0.02)
        self.interest_rate_spin.setValue(interest_rate * 100)

        form.addRow("Annualized interest rate", self.interest_rate_spin)

        # Greeks decimals precision
        self.precision_spin: QtWidgets.QSpinBox = QtWidgets.QSpinBox()
        self.precision_spin.setMinimum(0)
        self.precision_spin.setMaximum(10)

        precision: int = portfolio_setting.get("precision", 0)
        self.precision_spin.setValue(precision)

        form.addRow("Greeks decimal places", self.precision_spin)

        # Underlying for each chain
        self.combos: dict[str, QtWidgets.QComboBox] = {}

        portfolio: PortfolioData = self.option_engine.get_portfolio(self.portfolio_name)
        underlying_symbols: list = self.option_engine.get_underlying_symbols(
            self.portfolio_name
        )

        chain_symbols: list = list(portfolio._chains.keys())
        chain_symbols.sort()

        chain_underlying_map: dict = portfolio_setting.get("chain_underlying_map", {})

        for chain_symbol in chain_symbols:
            combo: QtWidgets.QComboBox = QtWidgets.QComboBox()
            combo.addItem("")
            combo.addItems(underlying_symbols)

            symbol, _ = chain_symbol.split(".")
            synthetic_symbol = f"{symbol}.LOCAL"
            combo.addItem(synthetic_symbol)

            underlying_symbol: str = chain_underlying_map.get(chain_symbol, "")
            if underlying_symbol:
                combo.setCurrentIndex(combo.findText(underlying_symbol))

            form.addRow(chain_symbol, combo)
            self.combos[chain_symbol] = combo

        # Set layout
        button: QtWidgets.QPushButton = QtWidgets.QPushButton("Sure")
        button.clicked.connect(self.update_portfolio_setting)
        form.addRow(button)

        self.setLayout(form)

    def update_portfolio_setting(self) -> None:
        """"""
        model_name: str = self.model_name_combo.currentText()
        interest_rate: float = self.interest_rate_spin.value() / 100

        precision: int = self.precision_spin.value()

        chain_underlying_map: dict = {}
        for chain_symbol, combo in self.combos.items():
            underlying_symbol: str = combo.currentText()

            if underlying_symbol:
                chain_underlying_map[chain_symbol] = underlying_symbol

        self.option_engine.update_portfolio_setting(
            self.portfolio_name,
            model_name,
            interest_rate,
            chain_underlying_map,
            precision
        )

        result: bool = self.option_engine.init_portfolio(self.portfolio_name)

        if result:
            self.accept()
        else:
            self.close()


class OptionManualTrader(QtWidgets.QWidget):
    """"""
    signal_tick: QtCore.Signal = QtCore.Signal(TickData)

    def __init__(self, option_engine: OptionEngine, portfolio_name: str) -> None:
        """"""
        super().__init__()

        self.option_engine: OptionEngine = option_engine
        self.main_engine: MainEngine = option_engine.main_engine
        self.event_engine: EventEngine = option_engine.event_engine

        self.contracts: dict[str, ContractData] = {}
        self.vt_symbol: str = ""
        self.price_digits: int = 0

        self.init_ui()
        self.init_contracts()
        self.connect_signal()

    def init_ui(self) -> None:
        """"""
        self.setWindowTitle("Options trading")

        # Trading Area
        self.symbol_line: QtWidgets.QLineEdit = QtWidgets.QLineEdit()
        self.symbol_line.returnPressed.connect(self._update_symbol)

        float_validator: QtGui.QDoubleValidator = QtGui.QDoubleValidator()
        float_validator.setBottom(0)

        self.price_line: QtWidgets.QLineEdit = QtWidgets.QLineEdit()
        self.price_line.setValidator(float_validator)

        int_validator: QtGui.QIntValidator = QtGui.QIntValidator()
        int_validator.setBottom(0)

        self.volume_line: QtWidgets.QLineEdit = QtWidgets.QLineEdit()
        self.volume_line.setValidator(int_validator)

        self.direction_combo: QtWidgets.QComboBox = QtWidgets.QComboBox()
        self.direction_combo.addItems([
            Direction.LONG.value,
            Direction.SHORT.value
        ])

        self.offset_combo: QtWidgets.QComboBox = QtWidgets.QComboBox()
        self.offset_combo.addItems([
            Offset.OPEN.value,
            Offset.CLOSE.value
        ])

        order_button: QtWidgets.QPushButton = QtWidgets.QPushButton("Order")
        order_button.clicked.connect(self.send_order)

        cancel_button: QtWidgets.QPushButton = QtWidgets.QPushButton("Withdraw all")
        cancel_button.clicked.connect(self.cancel_all)

        form1: QtWidgets.QFormLayout = QtWidgets.QFormLayout()
        form1.addRow("Code", self.symbol_line)
        form1.addRow("Direction", self.direction_combo)
        form1.addRow("Kaiping", self.offset_combo)
        form1.addRow("Price", self.price_line)
        form1.addRow("Quantity", self.volume_line)
        form1.addRow(order_button)
        form1.addRow(cancel_button)

        # Depth Area
        bid_color: str = "rgb(255,174,201)"
        ask_color: str = "rgb(160,255,160)"

        self.bp1_label: QtWidgets.QLabel = self.create_label(bid_color)
        self.bp2_label: QtWidgets.QLabel = self.create_label(bid_color)
        self.bp3_label: QtWidgets.QLabel = self.create_label(bid_color)
        self.bp4_label: QtWidgets.QLabel = self.create_label(bid_color)
        self.bp5_label: QtWidgets.QLabel = self.create_label(bid_color)

        self.bv1_label: QtWidgets.QLabel = self.create_label(
            bid_color, alignment=QtCore.Qt.AlignmentFlag.AlignRight)
        self.bv2_label: QtWidgets.QLabel = self.create_label(
            bid_color, alignment=QtCore.Qt.AlignmentFlag.AlignRight)
        self.bv3_label: QtWidgets.QLabel = self.create_label(
            bid_color, alignment=QtCore.Qt.AlignmentFlag.AlignRight)
        self.bv4_label: QtWidgets.QLabel = self.create_label(
            bid_color, alignment=QtCore.Qt.AlignmentFlag.AlignRight)
        self.bv5_label: QtWidgets.QLabel = self.create_label(
            bid_color, alignment=QtCore.Qt.AlignmentFlag.AlignRight)

        self.ap1_label: QtWidgets.QLabel = self.create_label(ask_color)
        self.ap2_label: QtWidgets.QLabel = self.create_label(ask_color)
        self.ap3_label: QtWidgets.QLabel = self.create_label(ask_color)
        self.ap4_label: QtWidgets.QLabel = self.create_label(ask_color)
        self.ap5_label: QtWidgets.QLabel = self.create_label(ask_color)

        self.av1_label: QtWidgets.QLabel = self.create_label(
            ask_color, alignment=QtCore.Qt.AlignmentFlag.AlignRight)
        self.av2_label: QtWidgets.QLabel = self.create_label(
            ask_color, alignment=QtCore.Qt.AlignmentFlag.AlignRight)
        self.av3_label: QtWidgets.QLabel = self.create_label(
            ask_color, alignment=QtCore.Qt.AlignmentFlag.AlignRight)
        self.av4_label: QtWidgets.QLabel = self.create_label(
            ask_color, alignment=QtCore.Qt.AlignmentFlag.AlignRight)
        self.av5_label: QtWidgets.QLabel = self.create_label(
            ask_color, alignment=QtCore.Qt.AlignmentFlag.AlignRight)

        self.lp_label: QtWidgets.QLabel = self.create_label()
        self.return_label: QtWidgets.QLabel = self.create_label(alignment=QtCore.Qt.AlignmentFlag.AlignRight)

        min_width: int = 70
        self.lp_label.setMinimumWidth(min_width)
        self.return_label.setMinimumWidth(min_width)

        form2: QtWidgets.QFormLayout = QtWidgets.QFormLayout()
        form2.addRow(self.ap5_label, self.av5_label)
        form2.addRow(self.ap4_label, self.av4_label)
        form2.addRow(self.ap3_label, self.av3_label)
        form2.addRow(self.ap2_label, self.av2_label)
        form2.addRow(self.ap1_label, self.av1_label)
        form2.addRow(self.lp_label, self.return_label)
        form2.addRow(self.bp1_label, self.bv1_label)
        form2.addRow(self.bp2_label, self.bv2_label)
        form2.addRow(self.bp3_label, self.bv3_label)
        form2.addRow(self.bp4_label, self.bv4_label)
        form2.addRow(self.bp5_label, self.bv5_label)

        # Set layout
        hbox: QtWidgets.QHBoxLayout = QtWidgets.QHBoxLayout()
        hbox.addLayout(form1)
        hbox.addLayout(form2)
        self.setLayout(hbox)

    def init_contracts(self) -> None:
        """"""
        contracts: list[ContractData] = self.main_engine.get_all_contracts()
        for contract in contracts:
            self.contracts[contract.symbol] = contract

    def connect_signal(self) -> None:
        """"""
        self.signal_tick.connect(self.update_tick)

    def send_order(self) -> None:
        """"""
        symbol: str = self.symbol_line.text()
        contract: ContractData | None = self.contracts.get(symbol, None)
        if not contract:
            return

        price_text: str = self.price_line.text()
        volume_text: str = self.volume_line.text()

        if not price_text or not volume_text:
            return

        price: float = float(price_text)
        volume: int = int(volume_text)
        direction: Direction = Direction(self.direction_combo.currentText())
        offset: Offset = Offset(self.offset_combo.currentText())

        req: OrderRequest = OrderRequest(
            symbol=contract.symbol,
            exchange=contract.exchange,
            direction=direction,
            type=OrderType.LIMIT,
            offset=offset,
            volume=volume,
            price=price
        )
        self.main_engine.send_order(req, contract.gateway_name)

    def cancel_all(self) -> None:
        """"""
        for order in self.main_engine.get_all_active_orders():
            req: CancelRequest = order.create_cancel_request()
            self.main_engine.cancel_order(req, order.gateway_name)

    def update_symbol(self, cell: MonitorCell) -> None:
        """"""
        if not cell.vt_symbol:
            return

        symbol: str = cell.vt_symbol.split(".")[0]
        self.symbol_line.setText(symbol)
        self._update_symbol()

    def _update_symbol(self) -> None:
        """"""
        symbol: str = self.symbol_line.text()
        contract: ContractData | None = self.contracts.get(symbol, None)

        if contract and contract.vt_symbol == self.vt_symbol:
            return

        if self.vt_symbol:
            self.event_engine.unregister(EVENT_TICK + self.vt_symbol, self.process_tick_event)
            self.clear_data()
            self.vt_symbol = ""

        if not contract:
            return

        vt_symbol: str = contract.vt_symbol
        self.vt_symbol = vt_symbol
        self.price_digits = get_digits(contract.pricetick)

        tick: TickData | None = self.main_engine.get_tick(vt_symbol)
        if tick:
            self.update_tick(tick)

        self.event_engine.register(EVENT_TICK + vt_symbol, self.process_tick_event)

    def create_label(
        self,
        color: str = "",
        alignment: QtCore.Qt.AlignmentFlag = QtCore.Qt.AlignmentFlag.AlignLeft
    ) -> QtWidgets.QLabel:
        """
        Create label with certain font color.
        """
        label: QtWidgets.QLabel = QtWidgets.QLabel("-")
        if color:
            label.setStyleSheet(f"color:{color}")
        label.setAlignment(alignment)
        return label

    def process_tick_event(self, event: Event) -> None:
        """"""
        tick: TickData = event.data
        if tick.vt_symbol != self.vt_symbol:
            return
        self.signal_tick.emit(tick)

    def update_tick(self, tick: TickData) -> None:
        """"""
        price_digits: int = self.price_digits

        self.lp_label.setText(f"{tick.last_price:.{price_digits}f}")
        self.bp1_label.setText(f"{tick.bid_price_1:.{price_digits}f}")
        self.bv1_label.setText(str(tick.bid_volume_1))
        self.ap1_label.setText(f"{tick.ask_price_1:.{price_digits}f}")
        self.av1_label.setText(str(tick.ask_volume_1))

        if tick.pre_close:
            r: float = (tick.last_price / tick.pre_close - 1) * 100
            self.return_label.setText(f"{r:.2f}%")

        if tick.bid_price_2:
            self.bp2_label.setText(f"{tick.bid_price_2:.{price_digits}f}")
            self.bv2_label.setText(str(tick.bid_volume_2))
            self.ap2_label.setText(f"{tick.ask_price_2:.{price_digits}f}")
            self.av2_label.setText(str(tick.ask_volume_2))

            self.bp3_label.setText(f"{tick.bid_price_3:.{price_digits}f}")
            self.bv3_label.setText(str(tick.bid_volume_3))
            self.ap3_label.setText(f"{tick.ask_price_3:.{price_digits}f}")
            self.av3_label.setText(str(tick.ask_volume_3))

            self.bp4_label.setText(f"{tick.bid_price_4:.{price_digits}f}")
            self.bv4_label.setText(str(tick.bid_volume_4))
            self.ap4_label.setText(f"{tick.ask_price_4:.{price_digits}f}")
            self.av4_label.setText(str(tick.ask_volume_4))

            self.bp5_label.setText(f"{tick.bid_price_5:.{price_digits}f}")
            self.bv5_label.setText(str(tick.bid_volume_5))
            self.ap5_label.setText(f"{tick.ask_price_5:.{price_digits}f}")
            self.av5_label.setText(str(tick.ask_volume_5))

    def clear_data(self) -> None:
        """"""
        self.lp_label.setText("-")
        self.return_label.setText("-")
        self.bp1_label.setText("-")
        self.bv1_label.setText("-")
        self.ap1_label.setText("-")
        self.av1_label.setText("-")

        self.bp2_label.setText("-")
        self.bv2_label.setText("-")
        self.ap2_label.setText("-")
        self.av2_label.setText("-")

        self.bp3_label.setText("-")
        self.bv3_label.setText("-")
        self.ap3_label.setText("-")
        self.av3_label.setText("-")

        self.bp4_label.setText("-")
        self.bv4_label.setText("-")
        self.ap4_label.setText("-")
        self.av4_label.setText("-")

        self.bp5_label.setText("-")
        self.bv5_label.setText("-")
        self.ap5_label.setText("-")
        self.av5_label.setText("-")


class OptionHedgeWidget(QtWidgets.QWidget):
    """"""

    def __init__(self, option_engine: OptionEngine, portfolio_name: str) -> None:
        """"""
        super().__init__()

        self.option_engine: OptionEngine = option_engine
        self.portfolio_name: str = portfolio_name
        self.hedge_engine: OptionHedgeEngine = option_engine.hedge_engine

        self.symbol_map: dict[str, str] = {}

        self.init_ui()

    def init_ui(self) -> None:
        """"""
        self.setWindowTitle("Delta Hedge")

        portfolio: PortfolioData = self.option_engine.get_portfolio(self.portfolio_name)
        underlying_symbols: list = [vs for vs in portfolio.underlyings.keys() if "LOCAL" not in vs]
        underlying_symbols.sort()

        self.symbol_combo: QtWidgets.QComboBox = QtWidgets.QComboBox()
        self.symbol_combo.addItems(underlying_symbols)

        self.trigger_spin: QtWidgets.QSpinBox = QtWidgets.QSpinBox()
        self.trigger_spin.setSuffix("Second")
        self.trigger_spin.setMinimum(1)
        self.trigger_spin.setValue(5)

        self.target_spin: QtWidgets.QSpinBox = QtWidgets.QSpinBox()
        self.target_spin.setMaximum(99999999)
        self.target_spin.setMinimum(-99999999)
        self.target_spin.setValue(0)

        self.range_spin: QtWidgets.QSpinBox = QtWidgets.QSpinBox()
        self.range_spin.setMinimum(0)
        self.range_spin.setMaximum(9999999)
        self.range_spin.setValue(12000)

        self.payup_spin: QtWidgets.QSpinBox = QtWidgets.QSpinBox()
        self.payup_spin.setMinimum(0)
        self.payup_spin.setValue(3)

        self.start_button: QtWidgets.QPushButton = QtWidgets.QPushButton("Start")
        self.start_button.clicked.connect(self.start)

        self.stop_button: QtWidgets.QPushButton = QtWidgets.QPushButton("Stop")
        self.stop_button.clicked.connect(self.stop)
        self.stop_button.setEnabled(False)

        form: QtWidgets.QFormLayout = QtWidgets.QFormLayout()
        form.addRow("Hedging contract", self.symbol_combo)
        form.addRow("Execution frequency", self.trigger_spin)
        form.addRow("Delta target", self.target_spin)
        form.addRow("Hedging threshold", self.range_spin)
        form.addRow("Overpriced order", self.payup_spin)
        form.addRow(self.start_button)
        form.addRow(self.stop_button)

        self.setLayout(form)

    def start(self) -> None:
        """"""
        vt_symbol: str = self.symbol_combo.currentText()
        timer_trigger: int = self.trigger_spin.value()
        delta_target: int = self.target_spin.value()
        delta_range: int = self.range_spin.value()
        hedge_payup: int = self.payup_spin.value()

        # Check delta of underlying
        underlying: UnderlyingData = cast(UnderlyingData, self.option_engine.get_instrument(vt_symbol))
        min_range: int = int(underlying.theo_delta * 0.6)
        if delta_range < min_range:
            msg: str = f"Delta hedging threshold ({delta_range}) is lower than the hedging contract"\
                f"60% of the Delta value ({min_range}), which may lead to frequent hedging back and forth!"

            QtWidgets.QMessageBox.warning(
                self,
                "Unable to start automatic hedging",
                msg,
                QtWidgets.QMessageBox.StandardButton.Ok
            )
            return

        self.hedge_engine.start(
            self.portfolio_name,
            vt_symbol,
            timer_trigger,
            delta_target,
            delta_range,
            hedge_payup
        )

        self.update_widget_status(False)

    def stop(self) -> None:
        """"""
        self.hedge_engine.stop()

        self.update_widget_status(True)

    def update_widget_status(self, status: bool) -> None:
        """"""
        self.start_button.setEnabled(status)
        self.symbol_combo.setEnabled(status)
        self.target_spin.setEnabled(status)
        self.range_spin.setEnabled(status)
        self.payup_spin.setEnabled(status)
        self.trigger_spin.setEnabled(status)
        self.stop_button.setEnabled(not status)


class OptionRiskWidget(QtWidgets.QWidget):
    """Option Risk Monitoring Component"""

    signal: QtCore.Signal = QtCore.Signal(Event)

    def __init__(self, option_engine: OptionEngine) -> None:
        """"""
        super().__init__()

        self.event_engine: EventEngine = option_engine.event_engine

        self.cancel_order_limit: float = 0.9
        self.trade_position_limit: float = 99999

        self.tray_icon: QtWidgets.QSystemTrayIcon | None = None

        self.init_ui()
        self.register_event()

    def init_ui(self) -> None:
        """"""
        self.setWindowTitle("Risk monitoring")
        self.resize(400, 200)

        self.trade_volume_label: QtWidgets.QLabel = QtWidgets.QLabel("0")
        self.net_pos_label: QtWidgets.QLabel = QtWidgets.QLabel("0")
        self.order_count_label: QtWidgets.QLabel = QtWidgets.QLabel("0")
        self.cancel_count_label: QtWidgets.QLabel = QtWidgets.QLabel("0")
        self.trade_position_ratio_label: QtWidgets.QLabel = QtWidgets.QLabel("0")
        self.cancel_order_ratio_label: QtWidgets.QLabel = QtWidgets.QLabel("0")

        self.trade_position_limit_spin: QtWidgets.QDoubleSpinBox = QtWidgets.QDoubleSpinBox()
        self.trade_position_limit_spin.setDecimals(1)
        self.trade_position_limit_spin.setRange(0, 100000)
        self.trade_position_limit_spin.setValue(self.trade_position_limit)
        self.trade_position_limit_spin.valueChanged.connect(self.set_trade_position_limit)

        self.cancel_order_ratio_spin: QtWidgets.QDoubleSpinBox = QtWidgets.QDoubleSpinBox()
        self.cancel_order_ratio_spin.setDecimals(1)
        self.cancel_order_ratio_spin.setRange(0, 1)
        self.cancel_order_ratio_spin.setValue(self.cancel_order_limit)
        self.cancel_order_ratio_spin.valueChanged.connect(self.set_cancel_order_limit)

        form: QtWidgets.QFormLayout = QtWidgets.QFormLayout()
        form.addRow("Transaction and position limits", self.trade_position_limit_spin)
        form.addRow("Order cancellation restrictions", self.cancel_order_ratio_spin)
        form.addRow(QtWidgets.QLabel(" "))
        form.addRow("Total trading volume", self.trade_volume_label)
        form.addRow("Net position", self.net_pos_label)
        form.addRow("Transaction position ratio", self.trade_position_ratio_label)
        form.addRow("Number of orders", self.order_count_label)
        form.addRow("Number of canceled orders", self.cancel_count_label)
        form.addRow("Cancellation order ratio", self.cancel_order_ratio_label)

        self.setLayout(form)

        icon_path: Path = Path(__file__).parent.joinpath("option.ico")
        icon: QtGui.QIcon = QtGui.QIcon(str(icon_path))
        self.tray_icon = QtWidgets.QSystemTrayIcon()
        self.tray_icon.setIcon(icon)
        self.tray_icon.setVisible(True)

    def register_event(self) -> None:
        """"""
        self.signal.connect(self.process_event)
        self.event_engine.register(EVENT_OPTION_RISK_NOTICE, self.signal.emit)

    def process_event(self, event: Event) -> None:
        """"""
        data = event.data
        self.trade_volume_label.setText(str(data["trade_volume"]))
        self.net_pos_label.setText(str(data["net_pos"]))
        self.order_count_label.setText(str(data["order_count"]))
        self.cancel_count_label.setText(str(data["cancel_count"]))
        self.trade_position_ratio_label.setText(f"{data['trade_position_ratio']:.2f}")
        self.cancel_order_ratio_label.setText(f"{data['cancel_order_ratio']:.2f}")

        texts: list = []
        if data["trade_position_ratio"] >= self.trade_position_limit:
            ratio = data["trade_position_ratio"]
            texts.append(f"The current trading position ratio {ratio} exceeds the limit {self.trade_position_limit}!")

        if data["cancel_order_ratio"] >= self.cancel_order_limit:
            ratio = data["cancel_order_ratio"]
            texts.append(f"The current order cancellation ratio {ratio} exceeds the limit {self.cancel_order_limit}!")

        if texts:
            msg: str = "\n\n".join(texts)
            self.show_warning(msg)

    def set_cancel_order_limit(self, limit: float) -> None:
        """Set order cancellation limit"""
        self.cancel_order_limit = limit

    def set_trade_position_limit(self, limit: float) -> None:
        """Set transaction and position ratio limits"""
        self.trade_position_limit = limit

    def show_warning(self, msg: str) -> None:
        """Show prompt information"""
        if self.tray_icon:
            self.tray_icon.showMessage("Risk warning", msg)
