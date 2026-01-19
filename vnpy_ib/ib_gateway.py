"""
IB Symbol Rules

SPY-USD-STK   SMART
EUR-USD-CASH  IDEALPRO
XAUUSD-USD-CMDTY  SMART
ES-202002-USD-FUT  GLOBEX
SI-202006-1000-USD-FUT  NYMEX
ES-2020006-C-2430-50-USD-FOP  GLOBEX

ConId is also supported for symbol.
"""


from copy import copy
from datetime import datetime, timedelta
from threading import Thread, Condition
from decimal import Decimal
import shelve
from tzlocal import get_localzone_name

from vnpy.event import EventEngine
from ibapi.client import EClient
from ibapi.common import OrderId, TickAttrib, TickerId
from ibapi.contract import Contract, ContractDetails
from ibapi.execution import Execution
from ibapi.order import Order
from ibapi.order_state import OrderState
from ibapi.ticktype import TickType, TickTypeEnum
from ibapi.wrapper import EWrapper
from ibapi.common import BarData as IbBarData
from ibapi.order_cancel import OrderCancel

from vnpy.trader.gateway import BaseGateway
from vnpy.trader.object import (
    TickData,
    OrderData,
    TradeData,
    PositionData,
    AccountData,
    ContractData,
    BarData,
    OrderRequest,
    CancelRequest,
    SubscribeRequest,
    HistoryRequest
)
from vnpy.trader.constant import (
    Product,
    OrderType,
    Direction,
    Exchange,
    Currency,
    Status,
    OptionType,
    Interval
)
from vnpy.trader.utility import get_file_path, ZoneInfo
from vnpy.trader.event import EVENT_TIMER
from vnpy.event import Event

#Delegate state mapping
STATUS_IB2VT: dict[str, Status] = {
    "ApiPending": Status.SUBMITTING,
    "PendingSubmit": Status.SUBMITTING,
    "PreSubmitted": Status.NOTTRADED,
    "Submitted": Status.NOTTRADED,
    "ApiCancelled": Status.CANCELLED,
    "Cancelled": Status.CANCELLED,
    "Filled": Status.ALLTRADED,
    "Inactive": Status.REJECTED,
}

#Long and short direction mapping
DIRECTION_VT2IB: dict[Direction, str] = {Direction.LONG: "BUY", Direction.SHORT: "SELL"}
DIRECTION_IB2VT: dict[str, Direction] = {v: k for k, v in DIRECTION_VT2IB.items()}
DIRECTION_IB2VT["BOT"] = Direction.LONG
DIRECTION_IB2VT["SLD"] = Direction.SHORT

#Delegate type mapping
ORDERTYPE_VT2IB: dict[OrderType, str] = {
    OrderType.LIMIT: "LMT",
    OrderType.MARKET: "MKT",
    OrderType.STOP: "STP"
}
ORDERTYPE_IB2VT: dict[str, OrderType] = {v: k for k, v in ORDERTYPE_VT2IB.items()}

#Exchange mapping
EXCHANGE_VT2IB: dict[Exchange, str] = {
    Exchange.SMART: "SMART",
    Exchange.NYMEX: "NYMEX",
    Exchange.COMEX: "COMEX",
    Exchange.GLOBEX: "GLOBEX",
    Exchange.IDEALPRO: "IDEALPRO",
    Exchange.CME: "CME",
    Exchange.CBOT: "CBOT",
    Exchange.CBOE: "CBOE",
    Exchange.ICE: "ICE",
    Exchange.SEHK: "SEHK",
    Exchange.SSE: "SEHKNTL",
    Exchange.SZSE: "SEHKSZSE",
    Exchange.HKFE: "HKFE",
    Exchange.CFE: "CFE",
    Exchange.TSE: "TSE",
    Exchange.NYSE: "NYSE",
    Exchange.NASDAQ: "NASDAQ",
    Exchange.AMEX: "AMEX",
    Exchange.ARCA: "ARCA",
    Exchange.EDGEA: "EDGEA",
    Exchange.ISLAND: "ISLAND",
    Exchange.BATS: "BATS",
    Exchange.IEX: "IEX",
    Exchange.IBKRATS: "IBKRATS",
    Exchange.OTC: "PINK",
    Exchange.SGX: "SGX",
    Exchange.EUREX: "EUREX",
    Exchange.LME: "LMEOTC"
}
EXCHANGE_IB2VT: dict[str, Exchange] = {v: k for k, v in EXCHANGE_VT2IB.items()}

#Product type mapping
PRODUCT_IB2VT: dict[str, Product] = {
    "STK": Product.EQUITY,
    "CASH": Product.FOREX,
    "CMDTY": Product.SPOT,
    "FUT": Product.FUTURES,
    "OPT": Product.OPTION,
    "FOP": Product.OPTION,
    "CONTFUT": Product.FUTURES,
    "IND": Product.INDEX,
    "CFD": Product.CFD
}

#Option type mapping
OPTION_IB2VT: dict[str, OptionType] = {
    "C": OptionType.CALL,
    "CALL": OptionType.CALL,
    "P": OptionType.PUT,
    "PUT": OptionType.PUT
}

#Currency type mapping
CURRENCY_VT2IB: dict[Currency, str] = {
    Currency.USD: "USD",
    Currency.CAD: "CAD",
    Currency.CNY: "CNY",
    Currency.HKD: "HKD",
}

#Slice data field mapping
TICKFIELD_IB2VT: dict[int, str] = {
    0: "bid_volume_1",
    1: "bid_price_1",
    2: "ask_price_1",
    3: "ask_volume_1",
    4: "last_price",
    5: "last_volume",
    6: "high_price",
    7: "low_price",
    8: "volume",
    9: "pre_close",
    10: "bid",
    11: "ask",
    12: "last",
    13: "model",
    14: "open_price",
    86: "open_interest"
}

#Account type mapping
ACCOUNTFIELD_IB2VT: dict[str, str] = {
    "NetLiquidationByCurrency": "balance",
    "NetLiquidation": "balance",
    "UnrealizedPnL": "positionProfit",
    "AvailableFunds": "available",
    "MaintMarginReq": "margin",
}

#Data frequency mapping
INTERVAL_VT2IB: dict[Interval, str] = {
    Interval.MINUTE: "1 min",
    Interval.HOUR: "1 hour",
    Interval.DAILY: "1 day",
}

#Other constants
LOCAL_TZ = ZoneInfo(get_localzone_name())
JOIN_SYMBOL: str = "-"


class IbGateway(BaseGateway):
    """
    VeighNa用于对接IB的交易接口。
    """

    default_name: str = "IB"

    default_setting: dict = {
        "TWS address": "127.0.0.1",
        "TWS port": 7497,
        "Customer number": 1,
        "Trading account": ""
    }

    exchanges: list[str] = list(EXCHANGE_VT2IB.keys())

    def __init__(self, event_engine: EventEngine, gateway_name: str) -> None:
        """Constructor"""
        super().__init__(event_engine, gateway_name)

        self.api: IbApi = IbApi(self)
        self.count: int = 0

    def connect(self, setting: dict) -> None:
        """Connect to transaction interface"""
        host: str = setting["TWS address"]
        port: int = setting["TWS port"]
        clientid: int = setting["Customer number"]
        account: str = setting["Trading account"]

        self.api.connect(host, port, clientid, account)

        self.event_engine.register(EVENT_TIMER, self.process_timer_event)

    def close(self) -> None:
        """Close interface"""
        self.api.close()

    def subscribe(self, req: SubscribeRequest) -> None:
        """Subscribe to Quotes"""
        self.api.subscribe(req)

    def send_order(self, req: OrderRequest) -> str:
        """Entrust an order"""
        return self.api.send_order(req)

    def cancel_order(self, req: CancelRequest) -> None:
        """Order cancellation"""
        self.api.cancel_order(req)

    def query_account(self) -> None:
        """Inquire about funds"""
        pass

    def query_position(self) -> None:
        """Query positions"""
        pass

    def query_history(self, req: HistoryRequest) -> list[BarData]:
        """Query historical data"""
        return self.api.query_history(req)

    def process_timer_event(self, event: Event) -> None:
        """Timing event processing"""
        self.count += 1
        if self.count < 10:
            return
        self.count = 0

        self.api.check_connection()


class IbApi(EWrapper):
    """IB API interface"""

    data_filename: str = "ib_contract_data.db"
    data_filepath: str = str(get_file_path(data_filename))

    def __init__(self, gateway: IbGateway) -> None:
        """Constructor"""
        super().__init__()

        self.gateway: IbGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.status: bool = False

        self.reqid: int = 0
        self.orderid: int = 0
        self.clientid: int = 0
        self.history_reqid: int = 0
        self.account: str = ""

        self.ticks: dict[int, TickData] = {}
        self.orders: dict[str, OrderData] = {}
        self.accounts: dict[str, AccountData] = {}
        self.contracts: dict[str, ContractData] = {}

        self.subscribed: dict[str, SubscribeRequest] = {}
        self.data_ready: bool = False

        self.history_req: HistoryRequest = None
        self.history_condition: Condition = Condition()
        self.history_buf: list[BarData] = []

        self.reqid_symbol_map: dict[int, str] = {}              # reqid: subscribe tick symbol
        self.reqid_underlying_map: dict[int, Contract] = {}     # reqid: query option underlying

        self.client: EClient = EClient(self)

        self.ib_contracts: dict[str, Contract] = {}

    def connectAck(self) -> None:
        """Connection success report"""
        self.status = True
        self.gateway.write_log("IB TWS connection successful")

        self.load_contract_data()

        self.data_ready = False

    def connectionClosed(self) -> None:
        """Disconnection report"""
        self.status = False
        self.gateway.write_log("IB TWS connection disconnected")

    def nextValidId(self, orderId: int) -> None:
        """Next valid order number report"""
        super().nextValidId(orderId)

        if not self.orderid:
            self.orderid = orderId

    def currentTime(self, time: int) -> None:
        """IB current server time report"""
        super().currentTime(time)

        dt: datetime = datetime.fromtimestamp(time)
        time_string: str = dt.strftime("%Y-%m-%d %H:%M:%S.%f")

        msg: str = f"Server time: {time_string}"
        self.gateway.write_log(msg)

    def error(
        self,
        reqId: TickerId,
        errorCode: int,
        errorString: str,
        advancedOrderRejectJson: str = ""
    ) -> None:
        """Specific error request report"""
        super().error(reqId, errorCode, errorString)

        #2000-2999 information notifications are not error messages
        if reqId == self.history_reqid and errorCode not in range(2000, 3000):
            self.history_condition.acquire()
            self.history_condition.notify()
            self.history_condition.release()

        msg: str = f"Information notification, code: {errorCode}, content: {errorString}"
        self.gateway.write_log(msg)

        #Quotes server is connected
        if errorCode == 2104 and not self.data_ready:
            self.data_ready = True

            self.client.reqCurrentTime()

            reqs: list = list(self.subscribed.values())
            self.subscribed.clear()
            for req in reqs:
                self.subscribe(req)

    def tickPrice(self, reqId: TickerId, tickType: TickType, price: float, attrib: TickAttrib) -> None:
        """Tick price update return"""
        super().tickPrice(reqId, tickType, price, attrib)

        if tickType not in TICKFIELD_IB2VT:
            return

        tick: TickData = self.ticks.get(reqId, None)
        if not tick:
            self.gateway.write_log(f"The tickPrice function receives an unsubscribed push, reqId: {reqId}")
            return

        name: str = TICKFIELD_IB2VT[tickType]
        setattr(tick, name, price)

        #Update tick data name field
        contract: ContractData = self.contracts.get(tick.vt_symbol, None)
        if contract:
            tick.name = contract.name

        #Local calculation of tick times and latest prices for Forex of IDEALPRO and Spot Commodity
        if tick.exchange == Exchange.IDEALPRO or "CMDTY" in tick.symbol:
            if not tick.bid_price_1 or not tick.ask_price_1 or tick.low_price == -1:
                return
            tick.last_price = (tick.bid_price_1 + tick.ask_price_1) / 2
            tick.datetime = datetime.now(LOCAL_TZ)

        self.gateway.on_tick(copy(tick))

    def tickSize(self, reqId: TickerId, tickType: TickType, size: Decimal) -> None:
        """Tick number update report"""
        super().tickSize(reqId, tickType, size)

        if tickType not in TICKFIELD_IB2VT:
            return

        tick: TickData = self.ticks.get(reqId, None)
        if not tick:
            self.gateway.write_log(f"The tickSize function receives an unsubscribed push, reqId: {reqId}")
            return

        name: str = TICKFIELD_IB2VT[tickType]
        setattr(tick, name, float(size))

        self.gateway.on_tick(copy(tick))

    def tickString(self, reqId: TickerId, tickType: TickType, value: str) -> None:
        """Tick string update report"""
        super().tickString(reqId, tickType, value)

        if tickType != TickTypeEnum.LAST_TIMESTAMP:
            return

        tick: TickData = self.ticks.get(reqId, None)
        if not tick:
            self.gateway.write_log(f"The tickString function receives an unsubscribed push, reqId: {reqId}")
            return

        dt: datetime = datetime.fromtimestamp(int(value))
        tick.datetime = dt.replace(tzinfo=LOCAL_TZ)

        self.gateway.on_tick(copy(tick))

    def tickOptionComputation(
        self,
        reqId: TickerId,
        tickType: TickType,
        tickAttrib: int,
        impliedVol: float,
        delta: float,
        optPrice: float,
        pvDividend: float,
        gamma: float,
        vega: float,
        theta: float,
        undPrice: float
    ) -> None:
        """Tick ​​option data push"""
        super().tickOptionComputation(
            reqId,
            tickType,
            tickAttrib,
            impliedVol,
            delta,
            optPrice,
            pvDividend,
            gamma,
            vega,
            theta,
            undPrice,
        )

        tick: TickData = self.ticks.get(reqId, None)
        if not tick:
            self.gateway.write_log(f"The tickOptionComputation function received an unsubscribed push, reqId: {reqId}")
            return

        prefix: str = TICKFIELD_IB2VT[tickType]

        tick.extra["underlying_price"] = undPrice

        if optPrice:
            tick.extra[f"{prefix}_price"] = optPrice
            tick.extra[f"{prefix}_impv"] = impliedVol
            tick.extra[f"{prefix}_delta"] = delta
            tick.extra[f"{prefix}_gamma"] = gamma
            tick.extra[f"{prefix}_theta"] = theta
            tick.extra[f"{prefix}_vega"] = vega
        else:
            tick.extra[f"{prefix}_price"] = 0
            tick.extra[f"{prefix}_impv"] = 0
            tick.extra[f"{prefix}_delta"] = 0
            tick.extra[f"{prefix}_gamma"] = 0
            tick.extra[f"{prefix}_theta"] = 0
            tick.extra[f"{prefix}_vega"] = 0

    def tickSnapshotEnd(self, reqId: int) -> None:
        """Quotation slice query return completed"""
        super().tickSnapshotEnd(reqId)

        tick: TickData = self.ticks.get(reqId, None)
        if not tick:
            self.gateway.write_log(f"The tickSnapshotEnd function receives an unsubscribed push, reqId: {reqId}")
            return

        self.gateway.write_log(f"{tick.vt_symbol} Quotation slice query successful")

    def orderStatus(
        self,
        orderId: OrderId,
        status: str,
        filled: Decimal,
        remaining: Decimal,
        avgFillPrice: float,
        permId: int,
        parentId: int,
        lastFillPrice: float,
        clientId: int,
        whyHeld: str,
        mktCapPrice: float,
    ) -> None:
        """Order status update report"""
        super().orderStatus(
            orderId,
            status,
            filled,
            remaining,
            avgFillPrice,
            permId,
            parentId,
            lastFillPrice,
            clientId,
            whyHeld,
            mktCapPrice,
        )

        orderid: str = str(orderId)
        order: OrderData = self.orders.get(orderid, None)
        if not order:
            return

        order.traded = float(filled)

        #Filter order cancellation status
        order_status: Status = STATUS_IB2VT.get(status, None)
        if order_status:
            order.status = order_status

        self.gateway.on_order(copy(order))

    def openOrder(
        self,
        orderId: OrderId,
        ib_contract: Contract,
        ib_order: Order,
        orderState: OrderState,
    ) -> None:
        """New order report"""
        super().openOrder(orderId, ib_contract, ib_order, orderState)

        orderid: str = str(orderId)

        if ib_order.orderRef:
            dt: datetime = datetime.strptime(ib_order.orderRef, "%Y-%m-%d %H:%M:%S")
        else:
            dt = datetime.now()

        #Prioritize the use of locally cached commission records to solve the problem that when an exchange transmits SMART, the exchange that returns the data may change
        order: OrderData = self.orders.get(orderid, None)
        if not order:
            order = OrderData(
                symbol=self.generate_symbol(ib_contract),
                exchange=EXCHANGE_IB2VT.get(ib_contract.exchange, Exchange.SMART),
                type=ORDERTYPE_IB2VT[ib_order.orderType],
                orderid=orderid,
                direction=DIRECTION_IB2VT[ib_order.action],
                volume=ib_order.totalQuantity,
                datetime=dt,
                gateway_name=self.gateway_name,
            )

        if order.type == OrderType.LIMIT:
            order.price = ib_order.lmtPrice
        elif order.type == OrderType.STOP:
            order.price = ib_order.auxPrice

        self.orders[orderid] = order
        self.gateway.on_order(copy(order))

    def updateAccountValue(self, key: str, val: str, currency: str, accountName: str) -> None:
        """Account update report"""
        super().updateAccountValue(key, val, currency, accountName)

        if not currency or key not in ACCOUNTFIELD_IB2VT:
            return

        accountid: str = f"{accountName}.{currency}"
        account: AccountData = self.accounts.get(accountid, None)
        if not account:
            account = AccountData(
                accountid=accountid,
                gateway_name=self.gateway_name
            )
            self.accounts[accountid] = account

        name: str = ACCOUNTFIELD_IB2VT[key]
        setattr(account, name, float(val))

    def updatePortfolio(
        self,
        contract: Contract,
        position: Decimal,
        marketPrice: float,
        marketValue: float,
        averageCost: float,
        unrealizedPNL: float,
        realizedPNL: float,
        accountName: str,
    ) -> None:
        """Position update return"""
        super().updatePortfolio(
            contract,
            position,
            marketPrice,
            marketValue,
            averageCost,
            unrealizedPNL,
            realizedPNL,
            accountName,
        )

        if contract.exchange:
            exchange: Exchange = EXCHANGE_IB2VT.get(contract.exchange, None)
        elif contract.primaryExchange:
            exchange = EXCHANGE_IB2VT.get(contract.primaryExchange, None)
        else:
            exchange = Exchange.SMART   # Use smart routing for default

        if not exchange:
            msg: str = f"There is an unsupported exchange position: {self.generate_symbol(contract)} {contract.exchange} {contract.primaryExchange}"
            self.gateway.write_log(msg)
            return

        try:
            ib_size: int = int(contract.multiplier)
        except ValueError:
            ib_size = 1
        price = averageCost / ib_size

        pos: PositionData = PositionData(
            symbol=self.generate_symbol(contract),
            exchange=exchange,
            direction=Direction.NET,
            volume=float(position),
            price=price,
            pnl=unrealizedPNL,
            gateway_name=self.gateway_name,
        )
        self.gateway.on_position(pos)

    def updateAccountTime(self, timeStamp: str) -> None:
        """Account update time report"""
        super().updateAccountTime(timeStamp)
        for account in self.accounts.values():
            self.gateway.on_account(copy(account))

    def contractDetails(self, reqId: int, contractDetails: ContractDetails) -> None:
        """Contract data update report"""
        super().contractDetails(reqId, contractDetails)

        #Extract contract information
        ib_contract: Contract = contractDetails.contract

        #Handling the case where the contract multiplier is 0
        if not ib_contract.multiplier:
            ib_contract.multiplier = 1

        #String style code needs to be obtained from cache
        if reqId in self.reqid_symbol_map:
            symbol: str = self.reqid_symbol_map[reqId]
        #Otherwise the default is to use numeric style codes
        else:
            symbol = str(ib_contract.conId)

        #Filter unsupported types
        product: Product = PRODUCT_IB2VT.get(ib_contract.secType, None)
        if not product:
            return

        #Generate contract
        contract: ContractData = ContractData(
            symbol=symbol,
            exchange=EXCHANGE_IB2VT[ib_contract.exchange],
            name=contractDetails.longName,
            product=PRODUCT_IB2VT[ib_contract.secType],
            size=float(ib_contract.multiplier),
            pricetick=contractDetails.minTick,
            min_volume=contractDetails.minSize,
            net_position=True,
            history_data=True,
            stop_supported=True,
            gateway_name=self.gateway_name,
        )

        if contract.product == Product.OPTION:
            underlying_symbol: str = str(contractDetails.underConId)

            contract.option_portfolio = underlying_symbol + "_O"
            contract.option_type = OPTION_IB2VT.get(ib_contract.right, None)
            contract.option_strike = ib_contract.strike
            contract.option_index = str(ib_contract.strike)
            contract.option_expiry = datetime.strptime(ib_contract.lastTradeDateOrContractMonth, "%Y%m%d")
            contract.option_underlying = underlying_symbol + "_" + ib_contract.lastTradeDateOrContractMonth

        if contract.vt_symbol not in self.contracts:
            self.gateway.on_contract(contract)

            self.contracts[contract.vt_symbol] = contract
            self.ib_contracts[contract.vt_symbol] = ib_contract

    def contractDetailsEnd(self, reqId: int) -> None:
        """Contract data update end report"""
        super().contractDetailsEnd(reqId)

        #Only option queries need to be processed
        underlying: Contract = self.reqid_underlying_map.get(reqId, None)
        if not underlying:
            return

        #Output log information
        symbol: str = self.generate_symbol(underlying)
        exchange: Exchange = EXCHANGE_IB2VT.get(underlying.exchange, Exchange.SMART)
        vt_symbol: str = f"{symbol}.{exchange.value}"

        self.gateway.write_log(f"{vt_symbol}option chain query successful")

        #Save option contracts to file
        self.save_contract_data()

    def execDetails(self, reqId: int, contract: Contract, execution: Execution) -> None:
        """Transaction data update report"""
        super().execDetails(reqId, contract, execution)

        #Analyze transaction time
        time_str: str = execution.time
        time_split: list = time_str.split(" ")
        words_count: int = 3

        if len(time_split) == words_count:
            timezone = time_split[-1]
            time_str = time_str.replace(f" {timezone}", "")
            tz = ZoneInfo(timezone)
        elif len(time_split) == (words_count - 1):
            tz = LOCAL_TZ
        else:
            self.gateway.write_log(f"Unsupported time format received: {time_str}")
            return

        dt: datetime = datetime.strptime(time_str, "%Y%m%d %H:%M:%S")
        dt = dt.replace(tzinfo=tz)

        if tz != LOCAL_TZ:
            dt = dt.astimezone(LOCAL_TZ)

        #Prioritize the use of locally cached commission records to solve the problem that when an exchange transmits SMART, the exchange that returns the data may change
        orderid: str = str(execution.orderId)
        order: OrderData = self.orders.get(orderid, None)

        if order:
            symbol: str = order.symbol
            exchange: Exchange = order.exchange
        else:
            symbol = self.generate_symbol(contract)
            exchange = EXCHANGE_IB2VT.get(contract.exchange, Exchange.SMART)

        #Push transaction data
        trade: TradeData = TradeData(
            symbol=symbol,
            exchange=exchange,
            orderid=orderid,
            tradeid=str(execution.execId),
            direction=DIRECTION_IB2VT[execution.side],
            price=execution.price,
            volume=float(execution.shares),
            datetime=dt,
            gateway_name=self.gateway_name,
        )

        self.gateway.on_trade(trade)

    def managedAccounts(self, accountsList: str) -> None:
        """All sub-account returns"""
        super().managedAccounts(accountsList)

        if not self.account:
            for account_code in accountsList.split(","):
                if account_code:
                    self.account = account_code

        self.gateway.write_log(f"The currently used trading account is {self.account}")
        self.client.reqAccountUpdates(True, self.account)

    def historicalData(self, reqId: int, ib_bar: IbBarData) -> None:
        """Historical data update report"""
        #The data format of daily level data and weekly level date data is %Y%m%d
        time_str: str = ib_bar.date
        time_split: list = time_str.split(" ")
        words_count: int = 3

        if ":" not in time_str:
            words_count -= 1

        if len(time_split) == words_count:
            timezone = time_split[-1]
            time_str = time_str.replace(f" {timezone}", "")
            tz = ZoneInfo(timezone)
        elif len(time_split) == (words_count - 1):
            tz = LOCAL_TZ
        else:
            self.gateway.write_log(f"Unsupported time format received: {time_str}")
            return

        if ":" in time_str:
            dt: datetime = datetime.strptime(time_str, "%Y%m%d %H:%M:%S")
        else:
            dt = datetime.strptime(time_str, "%Y%m%d")
        dt = dt.replace(tzinfo=tz)

        if tz != LOCAL_TZ:
            dt = dt.astimezone(LOCAL_TZ)

        bar: BarData = BarData(
            symbol=self.history_req.symbol,
            exchange=self.history_req.exchange,
            datetime=dt,
            interval=self.history_req.interval,
            volume=float(ib_bar.volume),
            open_price=ib_bar.open,
            high_price=ib_bar.high,
            low_price=ib_bar.low,
            close_price=ib_bar.close,
            gateway_name=self.gateway_name
        )
        if bar.volume < 0:
            bar.volume = 0

        self.history_buf.append(bar)

    def historicalDataEnd(self, reqId: int, start: str, end: str) -> None:
        """Report after historical data query is completed"""
        self.history_condition.acquire()
        self.history_condition.notify()
        self.history_condition.release()

    def connect(
        self,
        host: str,
        port: int,
        clientid: int,
        account: str
    ) -> None:
        """Connect to TWS"""
        if self.status:
            return

        self.host = host
        self.port = port
        self.clientid = clientid
        self.account = account

        self.client.connect(host, port, clientid)
        self.thread = Thread(target=self.client.run)
        self.thread.start()

    def check_connection(self) -> None:
        """Check connection"""
        if self.client.isConnected():
            return

        if self.status:
            self.close()

        self.client.connect(self.host, self.port, self.clientid)

        self.thread = Thread(target=self.client.run)
        self.thread.start()

    def close(self) -> None:
        """Disconnect TWS"""
        if not self.status:
            return

        self.save_contract_data()

        self.status = False
        self.client.disconnect()

    def query_option_portfolio(self, underlying: Contract) -> None:
        """Query option chain contract data"""
        if not self.status:
            return

        #Analyzing IB Options Contracts
        ib_contract: Contract = Contract()
        ib_contract.symbol = underlying.symbol
        ib_contract.currency = underlying.currency

        #Futures options must use designated exchanges
        if underlying.secType == "FUT":
            ib_contract.secType = "FOP"
            ib_contract.exchange = underlying.exchange
        #Spot options support smart routing
        else:
            ib_contract.secType = "OPT"
            ib_contract.exchange = "SMART"

        #Query contract information through TWS
        self.reqid += 1
        self.client.reqContractDetails(self.reqid, ib_contract)

        #Cache query records
        self.reqid_underlying_map[self.reqid] = underlying

    def subscribe(self, req: SubscribeRequest) -> None:
        """Subscribe to tick data updates"""
        if not self.status:
            return

        if req.exchange not in EXCHANGE_VT2IB:
            self.gateway.write_log(f"Unsupported exchange {req.exchange}")
            return

        if " " in req.symbol:
            self.gateway.write_log("Subscription failed, the contract code contains spaces")
            return

        #Filter duplicate subscriptions
        if req.vt_symbol in self.subscribed:
            return
        self.subscribed[req.vt_symbol] = req

        #Analyze IB contract details
        ib_contract: Contract = generate_ib_contract(req.symbol, req.exchange)
        if not ib_contract:
            self.gateway.write_log("Code parsing failed, please check whether the format is correct")
            return

        #Query contract information through TWS
        self.reqid += 1
        self.client.reqContractDetails(self.reqid, ib_contract)

        #If string-style code is used, caching is required
        if "-" in req.symbol:
            self.reqid_symbol_map[self.reqid] = req.symbol

        #Subscribe to tick data and create a tick object buffer
        self.reqid += 1
        self.client.reqMktData(self.reqid, ib_contract, "", False, False, [])

        tick: TickData = TickData(
            symbol=req.symbol,
            exchange=req.exchange,
            datetime=datetime.now(LOCAL_TZ),
            gateway_name=self.gateway_name
        )
        tick.extra = {}

        self.ticks[self.reqid] = tick

    def send_order(self, req: OrderRequest) -> str:
        """Entrust an order"""
        if not self.status:
            return ""

        if req.exchange not in EXCHANGE_VT2IB:
            self.gateway.write_log(f"Unsupported exchange: {req.exchange}")
            return ""

        if req.type not in ORDERTYPE_VT2IB:
            self.gateway.write_log(f"Unsupported price type: {req.type}")
            return ""

        if " " in req.symbol:
            self.gateway.write_log("Delegation failed, the contract code contains spaces")
            return ""

        self.orderid += 1

        ib_contract: Contract = generate_ib_contract(req.symbol, req.exchange)
        if not ib_contract:
            return ""

        ib_order: Order = Order()
        ib_order.orderId = self.orderid
        ib_order.clientId = self.clientid
        ib_order.action = DIRECTION_VT2IB[req.direction]
        ib_order.orderType = ORDERTYPE_VT2IB[req.type]
        ib_order.totalQuantity = Decimal(req.volume)
        ib_order.account = self.account
        ib_order.orderRef = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        if req.type == OrderType.LIMIT:
            ib_order.lmtPrice = req.price
        elif req.type == OrderType.STOP:
            ib_order.auxPrice = req.price

        self.client.placeOrder(self.orderid, ib_contract, ib_order)
        self.client.reqIds(1)

        order: OrderData = req.create_order_data(str(self.orderid), self.gateway_name)
        self.orders[order.orderid] = order
        self.gateway.on_order(order)
        return order.vt_orderid     # type: ignore

    def cancel_order(self, req: CancelRequest) -> None:
        """Order cancellation"""
        if not self.status:
            return

        cancel: OrderCancel = OrderCancel()
        self.client.cancelOrder(int(req.orderid), cancel)

    def query_history(self, req: HistoryRequest) -> list[BarData]:
        """Query historical data"""
        contract: ContractData = self.contracts[req.vt_symbol]
        if not contract:
            self.gateway.write_log(f"Contract not found: {req.vt_symbol}, please subscribe first")
            return []

        self.history_req = req

        self.reqid += 1

        ib_contract: Contract = generate_ib_contract(req.symbol, req.exchange)

        if req.end:
            end: datetime = req.end
        else:
            end = datetime.now(LOCAL_TZ)

        #End timestamp using UTC
        utc_tz: ZoneInfo = ZoneInfo("UTC")
        utc_end: datetime = end.astimezone(utc_tz)
        end_str: str = utc_end.strftime("%Y%m%d-%H:%M:%S")

        delta: timedelta = end - req.start
        days: int = delta.days
        if days < 365:
            duration: str = f"{days} D"
        else:
            duration = f"{delta.days/365:.0f} Y"

        bar_size: str = INTERVAL_VT2IB[req.interval]

        if contract.product in [Product.SPOT, Product.FOREX]:
            bar_type: str = "MIDPOINT"
        else:
            bar_type = "TRADES"

        self.history_reqid = self.reqid
        self.client.reqHistoricalData(
            self.reqid,
            ib_contract,
            end_str,
            duration,
            bar_size,
            bar_type,
            0,
            1,
            False,
            []
        )

        self.history_condition.acquire()    #Wait for asynchronous data to return
        self.history_condition.wait(600)
        self.history_condition.release()

        history: list[BarData] = self.history_buf
        self.history_buf = []       #Innovate new buffer list
        self.history_req = None

        return history

    def load_contract_data(self) -> None:
        """Load local contract data"""
        f = shelve.open(self.data_filepath)
        self.contracts = f.get("contracts", {})
        self.ib_contracts = f.get("ib_contracts", {})
        f.close()

        for contract in self.contracts.values():
            self.gateway.on_contract(contract)

        self.gateway.write_log("Local cache contract information is loaded successfully")

    def save_contract_data(self) -> None:
        """Save contract data locally"""
        #Before saving, ensure that the name of all contract data interfaces is IB to avoid the processing impact of other modules
        contracts: dict[str, ContractData] = {}
        for vt_symbol, contract in self.contracts.items():
            c: ContractData = copy(contract)
            c.gateway_name = "IB"
            contracts[vt_symbol] = c

        f = shelve.open(self.data_filepath)
        f["contracts"] = contracts
        f["ib_contracts"] = self.ib_contracts
        f.close()

    def generate_symbol(self, ib_contract: Contract) -> str:
        """Generate contract code"""
        #Generate string style code
        fields: list = [ib_contract.symbol]

        if ib_contract.secType in ["FUT", "OPT", "FOP"]:
            fields.append(ib_contract.lastTradeDateOrContractMonth)

        if ib_contract.secType in ["OPT", "FOP"]:
            fields.append(ib_contract.right)
            fields.append(str(ib_contract.strike))
            fields.append(str(ib_contract.multiplier))

        fields.append(ib_contract.currency)
        fields.append(ib_contract.secType)

        symbol: str = JOIN_SYMBOL.join(fields)
        exchange: Exchange = EXCHANGE_IB2VT.get(ib_contract.exchange, Exchange.SMART)
        vt_symbol: str = f"{symbol}.{exchange.value}"

        #If the string style code is not found in the contract information, use the numeric code
        if vt_symbol not in self.contracts:
            symbol = str(ib_contract.conId)

        return symbol

    def query_tick(self, vt_symbol: str) -> None:
        """Query market slice"""
        if not self.status:
            return

        contract: ContractData = self.contracts.get(vt_symbol, None)
        if not contract:
            self.gateway.write_log(f"Querying the market slice failed, the contract data corresponding to {vt_symbol} could not be found")
            return

        ib_contract: Contract = self.ib_contracts.get(vt_symbol, None)
        if not contract:
            self.gateway.write_log(f"Failed to query market slices, the IB contract data corresponding to {vt_symbol} could not be found")
            return

        self.reqid += 1
        self.client.reqMktData(self.reqid, ib_contract, "", True, False, [])

        tick: TickData = TickData(
            symbol=contract.symbol,
            exchange=contract.exchange,
            datetime=datetime.now(LOCAL_TZ),
            gateway_name=self.gateway_name
        )
        tick.extra = {}

        self.ticks[self.reqid] = tick

    def unsubscribe(self, req: SubscribeRequest) -> None:
        """Unsubscribe from tick data updates"""
        #Remove subscription record
        if req.vt_symbol not in self.subscribed:
            return
        self.subscribed.pop(req.vt_symbol)

        #Get subscription number
        cancel_id: int = 0
        for reqid, tick in self.ticks.items():
            if tick.vt_symbol == req.vt_symbol:
                cancel_id = reqid
                break

        #Send an unsubscribe request
        self.client.cancelMktData(cancel_id)


def generate_ib_contract(symbol: str, exchange: Exchange) -> Contract | None:
    """Produce IB contract"""
    #String code
    if "-" in symbol:
        try:
            fields: list = symbol.split(JOIN_SYMBOL)

            ib_contract: Contract = Contract()
            ib_contract.exchange = EXCHANGE_VT2IB[exchange]
            ib_contract.secType = fields[-1]
            ib_contract.currency = fields[-2]
            ib_contract.symbol = fields[0]

            if ib_contract.secType in ["FUT", "OPT", "FOP"]:
                ib_contract.lastTradeDateOrContractMonth = fields[1]

            if ib_contract.secType == "FUT":
                if len(fields) == 5:
                    ib_contract.multiplier = int(fields[2])

            if ib_contract.secType in ["OPT", "FOP"]:
                ib_contract.right = fields[2]
                ib_contract.strike = float(fields[3])
                ib_contract.multiplier = int(fields[4])
        except IndexError:
            ib_contract = None
    #Numeric Code (ConId)
    else:
        if symbol.isdigit():
            ib_contract = Contract()
            ib_contract.exchange = EXCHANGE_VT2IB[exchange]
            ib_contract.conId = symbol
        else:
            ib_contract = None

    return ib_contract
