import logging
from datetime import datetime
import pandas as pd

logger = logging.getLogger(__name__)


class ETLBase:
    """ETL 작업의 기본 클래스"""
    
    def __init__(self, job_name):
        self.job_name = job_name
        self.start_time = None
        self.end_time = None
        
    def extract(self):
        """데이터 추출 - 서브클래스에서 구현"""
        raise NotImplementedError
    
    def transform(self, data):
        """데이터 변환 - 서브클래스에서 구현"""
        raise NotImplementedError
    
    def load(self, data):
        """데이터 적재 - 서브클래스에서 구현"""
        raise NotImplementedError
    
    def run(self):
        """ETL 파이프라인 실행"""
        try:
            self.start_time = datetime.now()
            logger.info(f"[{self.job_name}] ETL 시작: {self.start_time}")
            
            # Extract
            logger.info(f"[{self.job_name}] 데이터 추출 시작")
            data = self.extract()
            logger.info(f"[{self.job_name}] 추출된 데이터: {len(data)} 건")
            
            if len(data) == 0:
                logger.warning(f"[{self.job_name}] 추출된 데이터가 없습니다.")
                return
            
            # Transform
            logger.info(f"[{self.job_name}] 데이터 변환 시작")
            transformed_data = self.transform(data)
            logger.info(f"[{self.job_name}] 변환된 데이터: {len(transformed_data)} 건")
            
            # Load
            logger.info(f"[{self.job_name}] 데이터 적재 시작")
            self.load(transformed_data)
            logger.info(f"[{self.job_name}] 데이터 적재 완료")
            
            self.end_time = datetime.now()
            duration = (self.end_time - self.start_time).total_seconds()
            logger.info(f"[{self.job_name}] ETL 완료: {duration:.2f}초 소요")
            
        except Exception as e:
            logger.error(f"[{self.job_name}] ETL 실패: {e}")
            raise