# flake8: noqa
from vnpy.event import EventEngine

from vnpy.trader.engine import MainEngine
from vnpy.trader.ui import MainWindow, create_qapp

from vnpy_bybit import BybitGateway
from vnpy_kis.kis_gateway import KisUnifiedGateway
from vnpy_ib import IbGateway


from vnpy_paperaccount import PaperAccountApp
from vnpy_ctastrategy import CtaStrategyApp
from vnpy_ctabacktester import CtaBacktesterApp
from vnpy_spreadtrading import SpreadTradingApp
from vnpy_algotrading import AlgoTradingApp
from vnpy_optionmaster import OptionMasterApp
from vnpy_portfoliostrategy import PortfolioStrategyApp
from vnpy_scripttrader import ScriptTraderApp
from vnpy_chartwizard import ChartWizardApp
from vnpy_rpcservice import RpcServiceApp
from vnpy_excelrtd import ExcelRtdApp
from vnpy_datamanager import DataManagerApp
from vnpy_datarecorder import DataRecorderApp
from vnpy_riskmanager import RiskManagerApp
from vnpy_webtrader import WebTraderApp
from vnpy_portfoliomanager import PortfolioManagerApp
# from vnpy_novastrategy import NovaStrategyApp
from vnpy.trader.setting import SETTINGS
from vnpy_krx.krx_datafeed import KrxDatafeed
# from vnpy_kis.kis_datafeed import KisDatafeed

import os
import locale

# 환경 변수 설정
os.environ["LANG"] = "en_US.UTF-8"
os.environ["LC_ALL"] = "en_US.UTF-8"

# 로케일 강제 변경
# locale.setlocale(locale.LC_ALL, "en_US.UTF-8")

# 1. 설정 입력 (json 파일이 없다면 코드에서 직접)
# SETTINGS["kis.app_key"] = "PSaYJYiqUO0CJfPD40nxeoehTa6ANiygCzWy"
# SETTINGS["kis.app_secret"] = "kWfOEtpRCpsGh06UGWkBNGF0gdjJ+jmAsMYPjWezsQxFpfsxPY1Nd8/Ys+p9iZBxHpJH6837LgqzYBq2UdeGNEso0UnpQC0Nl3MD2tR8xva7ELOqbfks7C++v3Xp0qtXY7R9mXe4Gvn8LUtQQUpGe9Q7KQUmdYgZoqjjTEZbVahTOwEBwSU=",


def main():
    """"""
    qapp = create_qapp()

    # SETTINGS["datafeed.name"] = "kis"
    
    event_engine = EventEngine()

    main_engine = MainEngine(event_engine)

    main_engine.add_gateway(BybitGateway)
    main_engine.add_gateway(KisUnifiedGateway)
    main_engine.add_gateway(IbGateway)

    # main_engine.add_app(PaperAccountApp)
    main_engine.add_app(CtaStrategyApp)
    main_engine.add_app(CtaBacktesterApp)
    main_engine.add_app(SpreadTradingApp)
    main_engine.add_app(AlgoTradingApp)
    main_engine.add_app(OptionMasterApp)
    main_engine.add_app(PortfolioStrategyApp)
    main_engine.add_app(ScriptTraderApp)
    main_engine.add_app(ChartWizardApp)
    main_engine.add_app(RpcServiceApp)
    main_engine.add_app(ExcelRtdApp)
    main_engine.add_app(DataManagerApp)
    main_engine.add_app(DataRecorderApp)
    main_engine.add_app(RiskManagerApp)
    main_engine.add_app(WebTraderApp)
    main_engine.add_app(PortfolioManagerApp)
    
    main_window = MainWindow(main_engine, event_engine)
    main_window.showMaximized()

    qapp.exec()


if __name__ == "__main__":
    main()
