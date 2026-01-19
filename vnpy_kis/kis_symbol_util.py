"""
KIS Symbol Utility
Desc: Downloads KOSPI/KOSDAQ master files and maps Name <-> Code.
Author: Gemini (Assisted)
"""

import os
import zipfile
import requests
import pandas as pd
from io import BytesIO

# 한국투자증권 마스터 파일 다운로드 URL (실전투자/모의투자 공용)
MASTER_BASE_URL = "https://new.real.koreainvestment.com/common/master/"

class KisSymbolConverter:
    def __init__(self):
        self.kospi_map = {}   # {Name: Code}
        self.kosdaq_map = {}  # {Name: Code}
        self.code_to_name = {} # {Code: Name}
        
        self.initialized = False

    def initialize(self):
        """마스터 파일을 다운로드하고 파싱하여 메모리에 적재합니다."""
        print("[KisSymbolUtil] 종목 마스터 파일 다운로드 및 파싱 시작...")
        
        try:
            self._process_master("kospi_code.mst.zip", "kospi_code.mst", is_kospi=True)
            self._process_master("kosdaq_code.mst.zip", "kosdaq_code.mst", is_kospi=False)
            self.initialized = True
            print(f"[KisSymbolUtil] 초기화 완료. 총 {len(self.code_to_name)}개 종목 로드됨.")
        except Exception as e:
            print(f"[KisSymbolUtil] 초기화 실패: {e}")

    def _download_and_extract(self, zip_filename, target_filename):
        """ZIP 파일을 메모리상에서 다운로드하고 압축 해제합니다."""
        url = MASTER_BASE_URL + zip_filename
        resp = requests.get(url)
        resp.raise_for_status()
        
        with zipfile.ZipFile(BytesIO(resp.content)) as zf:
            # 압축 파일 내의 데이터를 바이트로 읽음
            return zf.read(target_filename)

    def _process_master(self, zip_name, file_name, is_kospi=True):
        """마스터 파일 파싱 로직"""
        raw_data = self._download_and_extract(zip_name, file_name)
        
        # 인코딩은 cp949 (한글)
        # KOSPI와 KOSDAQ의 레코드 길이가 다름
        # 참고: 한국투자증권 API 문서 기준 포맷
        row_len = 228 if is_kospi else 222
        
        # 전체 바이트 길이
        total_len = len(raw_data)
        
        for i in range(0, total_len, row_len):
            row = raw_data[i : i + row_len]
            
            # 1. 단축코드 (Symbol Code): 0~9번째 바이트 (예: 'A005930')
            # 보통 'A'를 떼고 숫자 6자리만 사용함
            full_code = row[0:9].decode('cp949').strip()
            short_code = full_code[1:] if full_code.startswith('A') else full_code
            
            # 2. 한글명 (Name): 21~71번째 바이트 (KOSPI/KOSDAQ 공통 위치)
            # 공백 제거 (.strip)
            kor_name = row[21:71].decode('cp949').strip()
            
            # 3. 데이터 저장
            if is_kospi:
                self.kospi_map[kor_name] = short_code
            else:
                self.kosdaq_map[kor_name] = short_code
                
            self.code_to_name[short_code] = kor_name

    def get_code(self, name: str) -> str:
        """
        종목명을 입력받아 종목코드를 반환합니다.
        없으면 None 반환.
        """
        if not self.initialized:
            self.initialize()
            
        # 정확히 일치하는 경우
        if name in self.kospi_map:
            return self.kospi_map[name]
        if name in self.kosdaq_map:
            return self.kosdaq_map[name]
            
        return None

    def get_name(self, code: str) -> str:
        """
        종목코드를 입력받아 종목명을 반환합니다.
        """
        if not self.initialized:
            self.initialize()
            
        return self.code_to_name.get(code, None)

    def search_similar(self, keyword: str) -> list:
        """
        검색어가 포함된 종목 리스트를 반환합니다. (검색 보조용)
        """
        if not self.initialized:
            self.initialize()
            
        results = []
        for name, code in self.kospi_map.items():
            if keyword in name:
                results.append(f"[KOSPI] {name} ({code})")
        for name, code in self.kosdaq_map.items():
            if keyword in name:
                results.append(f"[KOSDAQ] {name} ({code})")
        return results

# --- 사용 예시 (직접 실행 시) ---
if __name__ == "__main__":
    converter = KisSymbolConverter()
    converter.initialize()
    
    # 1. 삼성전자 코드 찾기
    name = "삼성전자"
    code = converter.get_code(name)
    print(f"{name} -> {code}")
    
    # 2. 에코프로비엠 코드 찾기
    name2 = "에코프로비엠"
    code2 = converter.get_code(name2)
    print(f"{name2} -> {code2}")
    
    # 3. 잘못된 이름 검색
    print(f"없는종목 -> {converter.get_code('없는종목')}")
    
    # 4. 검색 기능
    print("검색결과 '카카오':", converter.search_similar("카카오"))