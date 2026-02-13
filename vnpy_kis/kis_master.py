"""
KIS Master Data Manager — DWS 공개 마스터 파일 기반 종목/지수 메타데이터

- 데이터 소스: KIS Open API가 아닌 DWS 공개 다운로드 (BASE_URL)
- 용도: 코스피/코스닥/해외주식/선물/ELW/채권 등 마스터 파일 파싱, In-Memory/Parquet 캐시
- API 기반 종목검색·시세: kis_api_helper (search_info), kis_datafeed (inquire_daily_itemchartprice 등) 사용
  MCP: search_domestic_stock_api(종목정보), search_overseas_stock_api(search_info) 등

[사용 예]
  mgr = KisMasterManager()
  df_nas = mgr.get_nasdaq(); df_k200 = mgr.get_kospi200(); df_n100 = mgr.get_nasdaq100()
"""

import os
import ssl
import zipfile
import urllib.request
import io
import pandas as pd
import re

# -----------------------------------------------------------------------------
# 1. Configuration (설정 및 컬럼 정의)
# -----------------------------------------------------------------------------

BASE_URL = "https://new.real.download.dws.co.kr/common/master/"

# --- 컬럼 정의 ---

# 1. 국내 주식 (코스피) - Part 2
COLS_KOSPI_P2 = [
    '그룹코드', '시가총액규모', '지수업종대분류', '지수업종중분류', '지수업종소분류',
    '제조업', '저유동성', '지배구조지수종목', 'KOSPI200섹터업종', 'KOSPI100',
    'KOSPI50', 'KRX', 'ETP', 'ELW발행', 'KRX100',
    'KRX자동차', 'KRX반도체', 'KRX바이오', 'KRX은행', 'SPAC',
    'KRX에너지화학', 'KRX철강', '단기과열', 'KRX미디어통신', 'KRX건설',
    'Non1', 'KRX증권', 'KRX선박', 'KRX섹터_보험', 'KRX섹터_운송',
    'SRI', '기준가', '매매수량단위', '시간외수량단위', '거래정지',
    '정리매매', '관리종목', '시장경고', '경고예고', '불성실공시',
    '우회상장', '락구분', '액면변경', '증자구분', '증거금비율',
    '신용가능', '신용기간', '전일거래량', '액면가', '상장일자',
    '상장주수', '자본금', '결산월', '공모가', '우선주',
    '공매도과열', '이상급등', 'KRX300', 'KOSPI', '매출액',
    '영업이익', '경상이익', '당기순이익', 'ROE', '기준년월',
    '시가총액', '그룹사코드', '회사신용한도초과', '담보대출가능', '대주가능'
]

# 2. 국내 주식 (코스닥) - Part 2
COLS_KOSDAQ_P2 = [
    '증권그룹구분코드','시가총액 규모 구분 코드 유가',
    '지수업종 대분류 코드','지수 업종 중분류 코드','지수업종 소분류 코드','벤처기업 여부 (Y/N)',
    '저유동성종목 여부','KRX 종목 여부','ETP 상품구분코드','KRX100 종목 여부 (Y/N)',
    'KRX 자동차 여부','KRX 반도체 여부','KRX 바이오 여부','KRX 은행 여부','기업인수목적회사여부',
    'KRX 에너지 화학 여부','KRX 철강 여부','단기과열종목구분코드','KRX 미디어 통신 여부',
    'KRX 건설 여부','(코스닥)투자주의환기종목여부','KRX 증권 구분','KRX 선박 구분',
    'KRX섹터지수 보험여부','KRX섹터지수 운송여부','KOSDAQ150지수여부 (Y,N)','주식 기준가',
    '정규 시장 매매 수량 단위','시간외 시장 매매 수량 단위','거래정지 여부','정리매매 여부',
    '관리 종목 여부','시장 경고 구분 코드','시장 경고위험 예고 여부','불성실 공시 여부',
    '우회 상장 여부','락구분 코드','액면가 변경 구분 코드','증자 구분 코드','증거금 비율',
    '신용주문 가능 여부','신용기간','전일 거래량','주식 액면가','주식 상장 일자','상장 주수(천)',
    '자본금','결산 월','공모 가격','우선주 구분 코드','공매도과열종목여부','이상급등종목여부',
    'KRX300 종목 여부 (Y/N)','매출액','영업이익','경상이익','단기순이익','ROE(자기자본이익률)',
    '기준년월','전일기준 시가총액 (억)','그룹사 코드','회사신용한도초과여부','담보대출가능여부','대주가능여부'
]

# 3. 코넥스
COLS_KONEX = [
    '단축코드', '표준코드', '종목명', '증권그룹구분코드', '주식 기준가', 
    '정규 시장 매매 수량 단위', '시간외 시장 매매 수량 단위', '거래정지 여부', 
    '정리매매 여부', '관리 종목 여부', '시장 경고 구분 코드', '시장 경고위험 예고 여부', 
    '불성실 공시 여부', '우회 상장 여부', '락구분 코드', '액면가 변경 구분 코드', 
    '증자 구분 코드', '증거금 비율', '신용주문 가능 여부', '신용기간', '전일 거래량', 
    '주식 액면가', '주식 상장 일자', '상장 주수(천)', '자본금', '결산 월', '공모 가격', 
    '우선주 구분 코드', '공매도과열종목여부', '이상급등종목여부', 'KRX300 종목 여부', 
    '매출액', '영업이익', '경상이익', '단기순이익', 'ROE', '기준년월', '전일기준 시가총액(억)', 
    '회사신용한도초과여부', '담보대출가능여부', '대주가능여부'
]

# 4. 해외 주식
COLS_OVERSEAS_STOCK = [
    'National code', 'Exchange id', 'Exchange code', 'Exchange name', 'Symbol', 
    'realtime symbol', 'Korea name', 'English name', 'Security type(1:Index,2:Stock,3:ETP(ETF),4:Warrant)', 
    'currency', 'float position', 'data type', 'base price', 'Bid order size', 'Ask order size', 
    'market start time(HHMM)', 'market end time(HHMM)', 'DR 여부(Y/N)', 'DR 국가코드', '업종분류코드', 
    '지수구성종목 존재 여부(0:구성종목없음,1:구성종목있음)', 'Tick size Type', 
    '구분코드(001:ETF,002:ETN,003:ETC,004:Others,005:VIX Underlying ETF,006:VIX Underlying ETN)','Tick size type 상세'
]

# 5. 해외 선물
COLS_OVERSEAS_FUTURE = [
    '종목코드', '서버자동주문 가능 종목 여부', '서버자동주문 TWAP 가능 종목 여부', '서버자동 경제지표 주문 가능 종목 여부', 
    '필러', '종목한글명', '거래소코드 (ISAM KEY 1)', '품목코드 (ISAM KEY 2)', '품목종류', '출력 소수점', '계산 소수점', 
    '틱사이즈', '틱가치', '계약크기', '가격표시진법', '환산승수', '최다월물여부 0:원월물 1:최다월물', 
    '최근월물여부 0:원월물 1:최근월물', '스프레드여부', '스프레드기준종목 LEG1 여부', '서브 거래소 코드'
]

# 6. ELW
COLS_ELW = [
    '단축코드', '표준코드', '한글종목명', 'ELW권리형태', 'ELW조기종료발생기준가격', 
    '바스켓 여부', '기초자산코드1', '기초자산코드2', '기초자산코드3', 
    '기초자산코드4', '기초자산코드5', '발행사 한글 종목명', '발행사코드', 
    '행사가', '최종거래일', '잔존 일수', '권리 유형 구분 코드', '지급일', 
    '전일시가총액(억)', '상장주수(천)', '시장 참가자 번호1', 
    '시장 참가자 번호2', '시장 참가자 번호3', '시장 참가자 번호4', 
    '시장 참가자 번호5', '시장 참가자 번호6', '시장 참가자 번호7', 
    '시장 참가자 번호8', '시장 참가자 번호9', '시장 참가자 번호10'
]

# 7. 채권
COLS_BOND = ['유형', '채권분류코드', '표준코드', '종목명', '채권이자분류코드', '상장일', '발행일', '상환일']

# 8. CME 야간 선물
COLS_CME = ['상품종류','단축코드','표준코드','한글종목명','행사가','기초자산 단축코드','기초자산 명']

# 9. 국내 지수/주식 선물옵션 (Pipe Separated)
COLS_FUTURE_OPTION = ['상품종류','단축코드','표준코드','한글종목명','ATM구분','행사가','월물구분코드','기초자산 단축코드','기초자산 명']


MASTER_CONFIG = {
    "kospi": {
        "url": BASE_URL + "kospi_code.mst.zip", "zip_file": "kospi_code.zip", "file_name": "kospi_code.mst", "parser": "parse_domestic_stock", "columns": ['단축코드', '표준코드', '한글명'] + COLS_KOSPI_P2
    },
    "kosdaq": {
        "url": BASE_URL + "kosdaq_code.mst.zip", "zip_file": "kosdaq_code.zip", "file_name": "kosdaq_code.mst", "parser": "parse_domestic_stock", "columns": ['단축코드', '표준코드', '한글종목명'] + COLS_KOSDAQ_P2
    },
    "konex": {
        "url": BASE_URL + "konex_code.mst.zip", "zip_file": "konex_code.zip", "file_name": "konex_code.mst", "parser": "parse_konex", "columns": COLS_KONEX
    },
    "overseas_stock": {
        "url_pattern": BASE_URL + "{val}mst.cod.zip", "file_pattern": "{val}mst.cod", "parser": "parse_overseas_stock", "columns": COLS_OVERSEAS_STOCK
    },
    "overseas_index": {
        "url": BASE_URL + "frgn_code.mst.zip", "zip_file": "frgn_code.mst.zip", "file_name": "frgn_code.mst", "parser": "parse_overseas_index",
        "columns": ['구분코드','심볼','영문명','한글명','종목업종코드','다우30 편입종목여부','나스닥100 편입종목여부','S&P 500 편입종목여부','거래소코드','국가구분코드']
    },
    "overseas_future": {
        "url": BASE_URL + "ffcode.mst.zip", "zip_file": "ffcode.mst.zip", "file_name": "ffcode.mst", "parser": "parse_overseas_future", "columns": COLS_OVERSEAS_FUTURE
    },
    "domestic_future": { "parser": "parse_pipe_separated", "columns": COLS_FUTURE_OPTION },
    "commodity_future": {
        "url": BASE_URL + "fo_com_code.mst.zip", "zip_file": "fo_com_code.mst.zip", "file_name": "fo_com_code.mst", "parser": "parse_commodity_future", "columns": ['상품구분','상품종류','단축코드','표준코드','한글종목명', '월물구분코드','기초자산 단축코드','기초자산 명']
    },
    "eurex_option": {
        "url": BASE_URL + "fo_eurex_code.mst.zip", "zip_file": "fo_eurex_code.mst.zip", "file_name": "fo_eurex_code.mst", "parser": "parse_eurex_option", "columns": ['상품종류','단축코드','표준코드','한글종목명','ATM구분','행사가','기초자산 단축코드','기초자산 명']
    },
    "cme_future": {
        "url": BASE_URL + "fo_cme_code.mst.zip", "zip_file": "fo_cme_code.mst.zip", "file_name": "fo_cme_code.mst", "parser": "parse_cme_future", "columns": COLS_CME
    },
    "bond": {
        "url": BASE_URL + "bond_code.mst.zip", "zip_file": "bond_code.zip", "file_name": "bond_code.mst", "parser": "parse_bond", "columns": COLS_BOND
    },
    "elw": {
        "url": BASE_URL + "elw_code.mst.zip", "zip_file": "elw_code.zip", "file_name": "elw_code.mst", "parser": "parse_elw", "columns": COLS_ELW
    },
    "member": {
        "url": BASE_URL + "memcode.mst", "file_name": "memcode.mst", "parser": "parse_member", "columns": ['회원사코드', '회원사명', '구분(0=국내, 1=외국)']
    },
    "sector": {
        "url": BASE_URL + "idxcode.mst.zip", "zip_file": "idxcode.zip", "file_name": "idxcode.mst", "parser": "parse_sector", "columns": ['업종코드', '업종명']
    },
    "theme": {
        "url": BASE_URL + "theme_code.mst.zip", "zip_file": "theme_code.zip", "file_name": "theme_code.mst", "parser": "parse_theme", "columns": ['테마코드', '테마명', '종목코드']
    },
}

# -----------------------------------------------------------------------------
# 2. Parsers (데이터 파싱 로직)
# -----------------------------------------------------------------------------

class KisMasterParser:
    """
    각 마스터 파일의 형식을 해석하는 파서 모음
    """
    
    @staticmethod
    def parse_domestic_stock(file_path, encoding='cp949'):
        """코스피/코스닥 파싱"""
        buffer_part1 = io.StringIO()
        buffer_part2 = io.StringIO()
        
        is_kospi = 'kospi' in file_path.lower()
        cut_idx = -228 if is_kospi else -222
        
        # Fixed Width Specs
        kp_specs = [2, 1, 4, 4, 4, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 9, 5, 5, 1, 1, 1, 2, 1, 1, 1, 2, 2, 2, 3, 1, 3, 12, 12, 8, 15, 21, 2, 7, 1, 1, 1, 1, 9, 9, 9, 5, 9, 8, 9, 3, 1, 1, 1]
        kd_specs = [2, 1, 4, 4, 4, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 9, 5, 5, 1, 1, 1, 2, 1, 1, 1, 2, 2, 2, 3, 1, 3, 12, 12, 8, 15, 21, 2, 7, 1, 1, 1, 1, 9, 9, 9, 5, 9, 8, 9, 3, 1, 1, 1]
        
        specs = kp_specs if is_kospi else kd_specs
        col_names_p1 = ['단축코드', '표준코드', '한글명']
        col_names_p2 = COLS_KOSPI_P2 if is_kospi else COLS_KOSDAQ_P2

        with open(file_path, mode="r", encoding=encoding) as f:
            for row in f:
                rf1 = row[:cut_idx]
                buffer_part1.write(f"{rf1[0:9].rstrip()},{rf1[9:21].rstrip()},{rf1[21:].strip()}\n")
                buffer_part2.write(row[cut_idx:])
        
        buffer_part1.seek(0)
        buffer_part2.seek(0)
        
        df1 = pd.read_csv(buffer_part1, header=None, names=col_names_p1)
        df2 = pd.read_fwf(buffer_part2, widths=specs, header=None)
        
        if len(df2.columns) == len(col_names_p2):
            df2.columns = col_names_p2
            
        return pd.concat([df1, df2], axis=1)

    @staticmethod
    def parse_konex(file_path, encoding='cp949'):
        """코넥스 파싱"""
        data = []
        with open(file_path, mode="r", encoding=encoding) as f:
            for row in f:
                row = row.strip()
                item = [
                    row[0:9].strip(), row[9:21].strip(), row[21:-184].strip(), row[-184:-182].strip(),
                    row[-182:-173].strip(), row[-173:-168].strip(), row[-168:-163].strip(), row[-163:-162].strip(),
                    row[-162:-161].strip(), row[-161:-160].strip(), row[-160:-158].strip(), row[-158:-157].strip(),
                    row[-157:-156].strip(), row[-156:-155].strip(), row[-155:-153].strip(), row[-153:-151].strip(),
                    row[-151:-149].strip(), row[-149:-146].strip(), row[-146:-145].strip(), row[-145:-142].strip(),
                    row[-142:-130].strip(), row[-130:-118].strip(), row[-118:-110].strip(), row[-110:-95].strip(),
                    row[-95:-74].strip(), row[-74:-72].strip(), row[-72:-65].strip(), row[-65:-64].strip(),
                    row[-64:-63].strip(), row[-63:-62].strip(), row[-62:-61].strip(), row[-61:-52].strip(),
                    row[-52:-43].strip(), row[-43:-34].strip(), row[-34:-29].strip(), row[-29:-20].strip(),
                    row[-20:-12].strip(), row[-12:-3].strip(), row[-3:-2].strip(), row[-2:-1].strip(), row[-1:].strip()
                ]
                data.append(item)
        return pd.DataFrame(data, columns=COLS_KONEX)

    @staticmethod
    def parse_overseas_stock(file_path, encoding='cp949'):
        """해외 주식 파싱"""
        df = pd.read_csv(file_path, sep='\t', encoding=encoding, header=None, dtype=str)
        if len(df.columns) == len(COLS_OVERSEAS_STOCK):
            df.columns = COLS_OVERSEAS_STOCK
        return df

    @staticmethod
    def parse_overseas_index(file_path, encoding='cp949'):
        """해외 지수 파싱 (frgn_code.mst)"""
        buffer_p1 = io.StringIO()
        buffer_p2 = io.StringIO()
        
        with open(file_path, mode="r", encoding=encoding) as f:
            for row in f:
                if row[0:1] == 'X':
                    rf1 = row[0:len(row) - 14]
                    rf1_1 = rf1[0:1]
                    rf1_2 = rf1[1:11]
                    rf1_3 = rf1[11:40].replace(",","")
                    rf1_4 = rf1[40:80].replace(",","").strip()
                    buffer_p1.write(f"{rf1_1},{rf1_2},{rf1_3},{rf1_4}\n")
                else:
                    rf1 = row[0:len(row) - 14]
                    rf1_1 = rf1[0:1]
                    rf1_2 = rf1[1:11]
                    rf1_3 = rf1[11:50].replace(",","")
                    rf1_4 = row[50:75].replace(",","").strip()
                    buffer_p1.write(f"{rf1_1},{rf1_2},{rf1_3},{rf1_4}\n")
                
                rf2 = row[-15:]
                buffer_p2.write(rf2)

        buffer_p1.seek(0)
        buffer_p2.seek(0)
        
        df1 = pd.read_csv(buffer_p1, header=None, names=['구분코드','심볼','영문명','한글명'])
        
        field_specs = [4, 1, 1, 1, 4, 3]
        df2 = pd.read_fwf(buffer_p2, widths=field_specs, header=None,
                          names=['종목업종코드','다우30 편입종목여부','나스닥100 편입종목여부',
                                 'S&P 500 편입종목여부','거래소코드','국가구분코드'])
        
        df2['종목업종코드'] = df2['종목업종코드'].astype(str).str.replace(r'[^A-Z]', '', regex=True)
        for col in ['다우30 편입종목여부', '나스닥100 편입종목여부', 'S&P 500 편입종목여부']:
            df2[col] = df2[col].astype(str).str.replace(r'[^0-1]+', '', regex=True)

        return pd.concat([df1, df2], axis=1)

    @staticmethod
    def parse_overseas_future(file_path, encoding='cp949'):
        """해외 선물 파싱"""
        data = []
        with open(file_path, mode="r", encoding=encoding) as f:
            for row in f:
                item = [
                    row[:32], row[32:33].rstrip(), row[33:34].rstrip(), row[34:35], row[35:82].rstrip(),
                    row[82:107].rstrip(), row[-92:-82], row[-82:-72].rstrip(), row[-72:-69].rstrip(),
                    row[-69:-64], row[-64:-59].rstrip(), row[-59:-45].rstrip(), row[-45:-31],
                    row[-31:-21].rstrip(), row[-21:-17].rstrip(), row[-17:-7], row[-7:-6].rstrip(),
                    row[-6:-5].rstrip(), row[-5:-4].rstrip(), row[-4:-3].rstrip(), row[-3:].rstrip()
                ]
                data.append(item)
        return pd.DataFrame(data, columns=COLS_OVERSEAS_FUTURE)

    @staticmethod
    def parse_pipe_separated(file_path, encoding='cp949'):
        """선물/옵션 파싱"""
        df = pd.read_csv(file_path, sep='|', encoding=encoding, header=None)
        if len(df.columns) == len(COLS_FUTURE_OPTION):
            df.columns = COLS_FUTURE_OPTION
        return df

    @staticmethod
    def parse_commodity_future(file_path, encoding='cp949'):
        """상품 선물 파싱"""
        buffer_p1 = io.StringIO()
        buffer_p2 = io.StringIO()
        with open(file_path, mode="r", encoding=encoding) as f:
            for row in f:
                rf1 = row[0:55]
                buffer_p1.write(f"{rf1[0:1]},{rf1[1:2]},{rf1[2:11].strip()},{rf1[11:23].strip()},{rf1[23:55].strip()}\n")
                rf2 = row[55:].lstrip()
                buffer_p2.write(f"{rf2[8:9]},{rf2[9:12]},{rf2[12:].strip()}\n")
        buffer_p1.seek(0)
        buffer_p2.seek(0)
        df = pd.concat([pd.read_csv(buffer_p1, header=None), pd.read_csv(buffer_p2, header=None)], axis=1)
        df.columns = ['상품구분','상품종류','단축코드','표준코드','한글종목명', '월물구분코드','기초자산 단축코드','기초자산 명']
        return df

    @staticmethod
    def parse_eurex_option(file_path, encoding='cp949'):
        """Eurex 옵션 파싱"""
        buffer_p1 = io.StringIO()
        buffer_p2 = io.StringIO()
        with open(file_path, mode="r", encoding=encoding) as f:
             for row in f:
                rf1 = row[0:59]
                buffer_p1.write(f"{rf1[0:1]},{rf1[1:10]},{rf1[10:22].strip()},{rf1[22:59].strip()}\n")
                rf2 = row[59:].lstrip()
                buffer_p2.write(f"{rf2[0:1]},{rf2[1:9]},{rf2[9:17]},{rf2[17:].strip()}\n")
        buffer_p1.seek(0)
        buffer_p2.seek(0)
        df = pd.concat([pd.read_csv(buffer_p1, header=None), pd.read_csv(buffer_p2, header=None)], axis=1)
        df.columns = ['상품종류','단축코드','표준코드','한글종목명','ATM구분','행사가','기초자산 단축코드','기초자산 명']
        return df

    @staticmethod
    def parse_cme_future(file_path, encoding='cp949'):
        """CME 야간 선물 파싱"""
        data = []
        with open(file_path, mode="r", encoding=encoding) as f:
            for row in f:
                data.append([
                    row[0:1], row[1:10].strip(), row[10:22].strip(), 
                    row[22:63].strip(), row[63:72].strip(), row[72:81].strip(), row[81:].strip()
                ])
        return pd.DataFrame(data, columns=COLS_CME)

    @staticmethod
    def parse_bond(file_path, encoding='cp949'):
        """채권 파싱"""
        data = []
        with open(file_path, mode="r", encoding=encoding) as f:
            for row in f:
                row = row.strip()
                data.append([
                    row[0:2].strip(), row[2:4].strip(), row[4:16].strip(), 
                    row[16:-26].rstrip(), row[-26:-24].strip(),
                    row[-24:-16].strip(), row[-16:-8].strip(), row[-8:].strip()
                ])
        return pd.DataFrame(data, columns=COLS_BOND)

    @staticmethod
    def parse_elw(file_path, encoding='cp949'):
        """ELW 파싱"""
        data = []
        with open(file_path, mode="r", encoding=encoding) as f:
            for row in f:
                crow = row[50:].strip()
                item = [
                    row[0:9].strip(), row[9:21].strip(), row[21:50].strip(),
                    crow[:1].strip(), crow[1:14].strip(), crow[14:15].strip(),
                    crow[15:24].strip(), crow[24:33].strip(), crow[33:42].strip(),
                    crow[42:51].strip(), crow[51:60].strip(),
                    row[-11:-110].strip(), row[-110:-105].strip(), row[-105:-96].strip(),
                    row[-96:-88].strip(), row[-88:-84].strip(), row[-84:-83].strip(),
                    row[-83:-75].strip(), row[-75:-66].strip(), row[-66:-51].strip(),
                    row[-51:-46].strip(), row[-46:-41].strip(), row[-41:-36].strip(),
                    row[-36:-31].strip(), row[-31:-26].strip(), row[-26:-21].strip(),
                    row[-21:-16].strip(), row[-16:-11].strip(), row[-11:-6].strip(), row[-6:].strip()
                ]
                data.append(item)
        return pd.DataFrame(data, columns=COLS_ELW)

    @staticmethod
    def parse_member(file_path, encoding='cp949'):
        """회원사 정보"""
        data = []
        with open(file_path, mode="r", encoding=encoding) as f:
            for row in f:
                if row.strip():
                    data.append([row[:5].strip(), row[5:-2].strip(), row[-2:].strip()])
        return pd.DataFrame(data, columns=['회원사코드', '회원사명', '구분(0=국내, 1=외국)'])

    @staticmethod
    def parse_sector(file_path, encoding='cp949'):
        data = []
        with open(file_path, mode="r", encoding=encoding) as f:
            for row in f:
                data.append([row[1:5], row[3:43].rstrip()])
        return pd.DataFrame(data, columns=['업종코드', '업종명'])
        
    @staticmethod
    def parse_theme(file_path, encoding='cp949'):
        data = []
        with open(file_path, mode="r", encoding=encoding) as f:
            for row in f:
                data.append([row[0:3], row[3:-10].rstrip(), row[-10:].rstrip()])
        return pd.DataFrame(data, columns=['테마코드', '테마명', '종목코드'])

# -----------------------------------------------------------------------------
# 3. Manager (통합 관리 클래스)
# -----------------------------------------------------------------------------

class KisMasterManager:
    """
    KIS Master Data Integration Manager
    """
    
    # [Reference] 해외 시장 코드 리스트
    MARKET_CODES = {
        'nas': 'NASDAQ (나스닥)',
        'nys': 'NYSE (뉴욕)',
        'ams': 'AMEX (아멕스)',
        'shs': 'Shanghai (상해)',
        'szs': 'Shenzhen (심천)',
        'tse': 'Tokyo (도쿄)',
        'hks': 'Hong Kong (홍콩)',
        'hnx': 'Hanoi (하노이)',
        'hsx': 'Ho Chi Minh (호치민)',
        'shi': 'Shanghai Index (상해지수 - 별도파일)',
        'szi': 'Shenzhen Index (심천지수 - 별도파일)'
    }
    
    # [Reference] 주요 지수 명칭 및 설명 (Constants)
    # 주요 지수의 구성 종목을 가져오기 위한 Wrapper 함수(get_kospi200 등)에서 사용됨
    MAJOR_INDICES_CONSTANTS = {
        # Domestic (KOSPI/KOSDAQ)
        'KOSPI200': '코스피 200 (KOSPI 200)',
        'KOSDAQ150': '코스닥 150 (KOSDAQ 150)',
        'KRX300': 'KRX 300',
        
        # Overseas (USA) - frgn_code.mst에서 필터링 가능
        'NASDAQ100': '나스닥 100 (NASDAQ 100)',
        'DOW30': '다우존스 30 (Dow Jones Industrial Average 30)',
        'SNP500': 'S&P 500',
        
        # Others
        'VIX': '변동성 지수 (VIX) - 주로 ETF/ETN 기초자산으로 분류됨'
    }

    def __init__(self, base_dir="./master_data"):
        self.base_dir = os.path.abspath(base_dir)
        if not os.path.exists(self.base_dir):
            os.makedirs(self.base_dir)
        self.ssl_context = ssl._create_unverified_context()

    def _get_paths(self, key, file_name):
        mst_path = os.path.join(self.base_dir, file_name)
        cache_path = os.path.join(self.base_dir, f"{file_name}.parquet")
        return mst_path, cache_path

    def _download(self, url, zip_name, file_name):
        """다운로드 및 압축해제"""
        if not url: return None
        print(f"[KIS] Downloading {file_name}...")
        
        # 내부 헬퍼 함수: urlopen을 사용하여 SSL context 적용 다운로드
        def download_with_ssl(download_url, save_path):
            with urllib.request.urlopen(download_url, context=self.ssl_context) as response, open(save_path, 'wb') as out_file:
                out_file.write(response.read())
                        
        if zip_name:
            zip_path = os.path.join(self.base_dir, zip_name)

            # [수정] urlretrieve 대신 커스텀 다운로드 사용
            # urllib.request.urlretrieve(url, zip_path, context=self.ssl_context) 
            download_with_ssl(url, zip_path)
                        
            print(f"[KIS] Extracting {zip_name}...")
            with zipfile.ZipFile(zip_path, 'r') as z:
                z.extractall(self.base_dir)
            os.remove(zip_path)
        else:
            urllib.request.urlretrieve(url, os.path.join(self.base_dir, file_name), context=self.ssl_context)
        return os.path.join(self.base_dir, file_name)

    def get_columns(self, key, market_code=None):
        """컬럼 목록 조회 (Metadata Only)"""
        cfg = MASTER_CONFIG.get(key)
        if not cfg: raise ValueError(f"Unknown key: {key}")
        
        file_name = cfg.get('file_pattern', '').format(val=market_code) if market_code else cfg.get('file_name')
        _, cache_path = self._get_paths(key, file_name)

        if os.path.exists(cache_path):
            try:
                import pyarrow.parquet as pq
                return pq.read_schema(cache_path).names
            except ImportError:
                return list(pd.read_parquet(cache_path).columns)
        
        if cfg.get('columns'):
            return cfg['columns']
            
        return "Columns not available until first download/parse."

    def get_data(self, key, market_code=None, force_update=False, columns=None):
        """데이터 로드 (캐싱 + 컬럼 필터링 지원)"""
        cfg = MASTER_CONFIG.get(key)
        if not cfg: raise ValueError(f"Unknown key: {key}")

        if key == 'overseas_stock':
            if not market_code: raise ValueError("market_code required (e.g., 'nas', 'nys')")
            url = cfg['url_pattern'].format(val=market_code)
            file_name = cfg['file_pattern'].format(val=market_code)
            zip_file = f"{market_code}mst.cod.zip"
        else:
            url = cfg.get('url')
            file_name = cfg.get('file_name')
            zip_file = cfg.get('zip_file')

        mst_path, cache_path = self._get_paths(key, file_name)

        if os.path.exists(cache_path) and not force_update:
            try:
                return pd.read_parquet(cache_path, columns=columns)
            except Exception:
                print("[KIS] Cache corrupted. Re-downloading...")

        self._download(url, zip_file, file_name)
        
        parser_name = cfg['parser']
        parser_func = getattr(KisMasterParser, parser_name)
        
        print(f"[KIS] Parsing {file_name}...")
        df = parser_func(mst_path)
        
        print(f"[KIS] Caching to {cache_path}...")
        df.to_parquet(cache_path, index=False)
        
        if columns:
            valid_cols = [c for c in columns if c in df.columns]
            return df[valid_cols]
        return df

    # --- Convenience Wrappers (자주 사용되는 시장/지수) ---
    
    # 1. 국내 주식/선물
    def get_kospi(self, **kwargs): return self.get_data('kospi', **kwargs)
    def get_kosdaq(self, **kwargs): return self.get_data('kosdaq', **kwargs)
    def get_konex(self, **kwargs): return self.get_data('konex', **kwargs)
    
    # [New] 국내 주요 지수 구성종목 Wrapper
    def get_kospi200(self, **kwargs):
        """KOSPI 200 구성 종목 반환"""
        df = self.get_kospi(**kwargs)
        # 'KOSPI200섹터업종' 코드가 존재하면(비어있지 않으면) 구성종목으로 간주
        # 데이터 타입에 따라 처리 (문자열 혹은 숫자)
        if 'KOSPI200섹터업종' in df.columns:
            return df[df['KOSPI200섹터업종'].notna() & (df['KOSPI200섹터업종'].astype(str).str.strip() != '')]
        return df

    def get_kosdaq150(self, **kwargs):
        """KOSDAQ 150 구성 종목 반환"""
        df = self.get_kosdaq(**kwargs)
        if 'KOSDAQ150지수여부 (Y,N)' in df.columns:
            return df[df['KOSDAQ150지수여부 (Y,N)'] == 'Y']
        return df

    def get_krx300(self, **kwargs):
        """KRX 300 구성 종목 반환"""
        # 코스피와 코스닥 모두에 KRX300 컬럼이 존재함. 각각 가져와서 합쳐야 함.
        # 편의상 코스피에서 먼저 찾고, 필요시 코스닥도 조회 가능하도록 사용자에게 맡기거나 여기서 병합
        # 여기서는 코스피/코스닥 중 호출된 쪽의 KRX300을 반환하거나, 명시적으로 병합
        # 사용성을 위해 각각 호출 권장: get_kospi(krx300_only=True) 패턴보다는 별도 필터링 권장
        # 여기서는 코스피 내 KRX300만 예시로 반환 (KRX300은 통합 지수임)
        df = self.get_kospi(**kwargs)
        if 'KRX300' in df.columns:
            return df[df['KRX300'].notna() & (df['KRX300'] == 'Y')] # 코스피 파일의 컬럼명은 'KRX300'
        return df

    # 2. 국내 선물
    def get_future_index(self, **kwargs): return self.get_future(type='index', **kwargs) # 지수선물 (코스피200 등)
    def get_future_stock(self, **kwargs): return self.get_future(type='stock', **kwargs) # 주식선물 (삼성전자선물 등)

    # 3. 해외 주식 (주요 시장 Wrapper)
    def get_nasdaq(self, **kwargs): return self.get_data('overseas_stock', market_code='nas', **kwargs)
    def get_nyse(self, **kwargs): return self.get_data('overseas_stock', market_code='nys', **kwargs)
    def get_amex(self, **kwargs): return self.get_data('overseas_stock', market_code='ams', **kwargs)
    def get_hongkong(self, **kwargs): return self.get_data('overseas_stock', market_code='hks', **kwargs)
    def get_shanghai(self, **kwargs): return self.get_data('overseas_stock', market_code='shs', **kwargs)
    def get_shenzhen(self, **kwargs): return self.get_data('overseas_stock', market_code='szs', **kwargs)
    def get_tokyo(self, **kwargs): return self.get_data('overseas_stock', market_code='tse', **kwargs)
    def get_hanoi(self, **kwargs): return self.get_data('overseas_stock', market_code='hnx', **kwargs)
    def get_hochiminh(self, **kwargs): return self.get_data('overseas_stock', market_code='hsx', **kwargs)
    
    # 4. [New] 해외 주요 지수 구성종목 Wrapper (frgn_code.mst 활용)
    def get_nasdaq100(self, **kwargs):
        """나스닥 100 구성 종목 반환"""
        df = self.get_overseas_indices(**kwargs)
        if '나스닥100 편입종목여부' in df.columns:
            return df[df['나스닥100 편입종목여부'] == '1']
        return df

    def get_dow30(self, **kwargs):
        """다우존스 30 구성 종목 반환"""
        df = self.get_overseas_indices(**kwargs)
        if '다우30 편입종목여부' in df.columns:
            return df[df['다우30 편입종목여부'] == '1']
        return df
    
    def get_snp500(self, **kwargs):
        """S&P 500 구성 종목 반환"""
        df = self.get_overseas_indices(**kwargs)
        if 'S&P 500 편입종목여부' in df.columns:
            return df[df['S&P 500 편입종목여부'] == '1']
        return df

    def get_vix(self, **kwargs):
        """VIX 관련 종목(ETF/ETN) 반환 (해외 주식 마스터 활용)"""
        # VIX는 종목이 아니라 지수이지만, VIX Underlying ETF/ETN을 찾을 때 사용
        # 구분코드(005:VIX Underlying ETF, 006:VIX Underlying ETN)
        # 전체 해외 주식(미국)을 다 뒤져야 하므로 나스닥/뉴욕/아멕스 통합 검색 필요
        # 성능상 여기서는 대표적으로 NYSE만 예시로 검색하거나, 사용자가 시장을 지정해야 함
        # 편의상 NYSE(nys) 기준으로 반환
        df = self.get_nyse(**kwargs)
        col_name = '구분코드(001:ETF,002:ETN,003:ETC,004:Others,005:VIX Underlying ETF,006:VIX Underlying ETN)'
        if col_name in df.columns:
            return df[df[col_name].isin(['005', '006'])]
        return df

    # 5. 해외/국내 지수 및 기타
    def get_overseas_indices(self, **kwargs): return self.get_data('overseas_index', **kwargs) # 통합 지수 파일
    def get_shanghai_index(self, **kwargs): return self.get_data('overseas_stock', market_code='shi', **kwargs) # 상해지수 별도파일
    def get_shenzhen_index(self, **kwargs): return self.get_data('overseas_stock', market_code='szi', **kwargs) # 심천지수 별도파일
    
    def get_overseas_future(self, **kwargs): return self.get_data('overseas_future', **kwargs)
    def get_sector(self, **kwargs): return self.get_data('sector', **kwargs)
    def get_theme(self, **kwargs): return self.get_data('theme', **kwargs)

    # --- Internal Helpers ---
    def get_future(self, type='index', **kwargs): 
        # type: 'index' (지수선물) or 'stock' (주식선물)
        key = 'domestic_future'
        cfg = MASTER_CONFIG[key]
        if type == 'stock':
            cfg['url'] = BASE_URL + "fo_stk_code_mts.mst.zip"
            cfg['zip_file'] = "fo_stk_code_mts.mst.zip"
            cfg['file_name'] = "fo_stk_code_mts.mst"
        else:
            cfg['url'] = BASE_URL + "fo_idx_code_mts.mst.zip"
            cfg['zip_file'] = "fo_idx_code_mts.mst.zip"
            cfg['file_name'] = "fo_idx_code_mts.mst"
        return self.get_data(key, **kwargs)
        
    def get_commodity_future(self, **kwargs): return self.get_data('commodity_future', **kwargs)
    def get_cme_future(self, **kwargs): return self.get_data('cme_future', **kwargs)
    def get_eurex_option(self, **kwargs): return self.get_data('eurex_option', **kwargs)
    def get_bond(self, **kwargs): return self.get_data('bond', **kwargs)
    def get_elw(self, **kwargs): return self.get_data('elw', **kwargs)
    def get_member(self, **kwargs): return self.get_data('member', **kwargs)


if __name__ == "__main__":
    mgr = KisMasterManager("./master_data")
    print(f"Supported Overseas Markets: {list(mgr.MARKET_CODES.keys())}")
    
    # Example: 나스닥 종목 중 심볼과 한글명만 가져오기
    # df = mgr.get_nasdaq(columns=['Symbol', 'Korea name'])
    print(mgr.get_columns('kospi'))
    df = mgr.get_kospi200(columns=['단축코드', '표준코드', '한글명', '기준가', '매매수량단위', '매출액', '영업이익', '경상이익', '당기순이익', 'ROE', '기준년월', '시가총액'])
    # df.to_excel('nas_master.xlsx',index=False)  # 전체 통합파일
    print(df.head())