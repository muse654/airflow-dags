import pandas as pd
from etl_base import ETLBase
from db_connectors import DatabaseConnector
import logging
import gc
from datetime import datetime
import re

logger = logging.getLogger(__name__)


class GuavaClickHouseETL(ETLBase):
    """Guava ClickHouse에서 Target ClickHouse로 증분 데이터 이관 (최대 10만 건)"""
    
    def __init__(self, table_name, query=None, batch_size=500, max_rows=100000):
        super().__init__(f"Guava_ClickHouse_{table_name}")
        self.table_name = table_name
        self.base_query = query
        self.batch_size = batch_size
        self.max_rows = max_rows  # 최대 처리 행 수
        self.source_client = None
        self.target_client = None
        
        # OTel 테이블별 타임스탬프 및 타입 정의
        self.timestamp_info = {
            'otel_logs': {'column': 'Timestamp', 'type': 'DateTime64'},
            'otel_metrics_exponential_histogram': {'column': 'TimeUnix', 'type': 'Int64'},
            'otel_metrics_histogram': {'column': 'TimeUnix', 'type': 'Int64'},
            'otel_metrics_sum': {'column': 'TimeUnix', 'type': 'Int64'},
            'otel_metrics_gauge': {'column': 'TimeUnix', 'type': 'Int64'},
            'otel_metrics_summary': {'column': 'TimeUnix', 'type': 'Int64'},
            'otel_traces': {'column': 'Timestamp', 'type': 'DateTime64'}
        }
    
    def _get_last_timestamp(self, timestamp_col):
        """타겟 테이블의 마지막 타임스탬프 조회"""
        try:
            result = self.target_client.query(f"EXISTS TABLE `{self.table_name}`")
            table_exists = result.result_rows[0][0]
            
            if not table_exists:
                logger.info(f"타겟 테이블 `{self.table_name}` 없음")
                return None
            
            count_result = self.target_client.query(f"SELECT count() FROM `{self.table_name}`")
            row_count = count_result.result_rows[0][0]
            
            if row_count == 0:
                logger.info(f"타겟 테이블 `{self.table_name}` 비어있음 (0행)")
                return None
            
            # 타입에 따라 다르게 조회
            ts_info = self.timestamp_info.get(self.table_name, {'column': 'Timestamp', 'type': 'DateTime64'})
            
            if ts_info['type'] == 'Int64':
                # TimeUnix - 이미 나노초 정수
                result = self.target_client.query(
                    f"SELECT max(`{timestamp_col}`) FROM `{self.table_name}`"
                )
                last_ts = result.result_rows[0][0]
                
                if last_ts:
                    logger.info(f"마지막 {timestamp_col} (Int64): {last_ts} ({row_count:,}행)")
                    return int(last_ts)
            else:
                # DateTime64 - 문자열로 반환
                result = self.target_client.query(
                    f"SELECT toString(max(`{timestamp_col}`)) FROM `{self.table_name}`"
                )
                last_ts = result.result_rows[0][0]
                
                if last_ts:
                    logger.info(f"마지막 {timestamp_col} (DateTime64): {last_ts} ({row_count:,}행)")
                    return last_ts
            
            return None
            
        except Exception as e:
            logger.warning(f"마지막 타임스탬프 조회 실패: {e}")
            return None
    
    def _build_where_clause(self, timestamp_col, timestamp_type, last_value):
        """WHERE 조건 생성"""
        if not last_value:
            logger.info("⚠️ 전체 마이그레이션: 타겟 테이블이 비어있음")
            return ""
        
        logger.info(f"✅ 증분 마이그레이션: {timestamp_col} > {last_value} (타입: {timestamp_type})")
        
        # Int64 타입 (이미 나노초 정수)
        if timestamp_type == 'Int64' and isinstance(last_value, (int, float)):
            return f"WHERE `{timestamp_col}` > {int(last_value)}"
        
        # DateTime64 타입 (문자열)
        if timestamp_type == 'DateTime64' and isinstance(last_value, str):
            # 타임존 제거
            timestamp_str = last_value.replace('T', ' ')
            for tz in ['+09:00', '+00:00', 'Z', '-00:00', '+01:00', '-01:00']:
                timestamp_str = timestamp_str.replace(tz, '')
            timestamp_str = timestamp_str.strip()
            
            logger.info(f"정리된 타임스탬프: '{timestamp_str}'")
            return f"WHERE `{timestamp_col}` > '{timestamp_str}'"
        
        # Int64인데 문자열로 왔을 때
        if timestamp_type == 'Int64' and isinstance(last_value, str):
            try:
                # 타임존 제거
                timestamp_str = last_value.replace('T', ' ')
                for tz in ['+09:00', '+00:00', 'Z', '-00:00', '+01:00', '-01:00']:
                    timestamp_str = timestamp_str.replace(tz, '')
                timestamp_str = timestamp_str.strip()
                
                # Python에서 나노초 계산
                match = re.match(r'(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2}):(\d{2})\.?(\d{0,9})?', timestamp_str)
                if match:
                    year, month, day, hour, minute, second, nanosec = match.groups()
                    nanosec = (nanosec or '0').ljust(9, '0')[:9]
                    
                    dt_obj = datetime(int(year), int(month), int(day), int(hour), int(minute), int(second))
                    timestamp_seconds = int(dt_obj.timestamp())
                    timestamp_nanos = timestamp_seconds * 1000000000 + int(nanosec)
                    
                    logger.info(f"계산된 나노초: {timestamp_nanos}")
                    return f"WHERE `{timestamp_col}` > {timestamp_nanos}"
                else:
                    logger.error(f"타임스탬프 파싱 실패: {timestamp_str}")
                    return ""
            except Exception as e:
                logger.error(f"타임스탬프 변환 실패: {e}")
                return ""
        
        logger.error(f"지원하지 않는 타입 조합: {timestamp_type}, {type(last_value)}")
        return ""
    
    def run(self):
        """ETL 파이프라인 실행 (최대 10만 건)"""
        try:
            self.start_time = datetime.now()
            logger.info(f"[{self.job_name}] ETL 시작: {self.start_time}")
            logger.info(f"⚠️ 최대 처리 행 수: {self.max_rows:,}건")
            
            # 클라이언트 연결
            self.source_client = DatabaseConnector.get_clickhouse_client('GUAVA')
            self.target_client = DatabaseConnector.get_clickhouse_client('TARGET')
            
            # 테이블 구조 복제
            self._create_otel_table_from_source(self.table_name)
            
            # 증분 조건 확인
            ts_info = self.timestamp_info.get(self.table_name, {'column': 'Timestamp', 'type': 'DateTime64'})
            timestamp_col = ts_info['column']
            timestamp_type = ts_info['type']
            
            last_value = self._get_last_timestamp(timestamp_col)
            
            # WHERE 조건 구성
            where_clause = self._build_where_clause(timestamp_col, timestamp_type, last_value)
            
            if not where_clause and last_value:
                logger.error(f"❌ WHERE 조건 생성 실패, '{self.table_name}' 테이블 스킵")
                return
            
            # 전체 행 수 확인
            count_query = f"SELECT count() FROM `{self.table_name}` {where_clause}"
            logger.info(f"Count 쿼리: {count_query}")
            
            count_result = self.source_client.query(count_query)
            total_rows = count_result.result_rows[0][0]
            
            # 10만 건으로 제한
            if total_rows > self.max_rows:
                logger.warning(f"⚠️ 전체 {total_rows:,}행 중 {self.max_rows:,}행만 처리")
                total_rows = self.max_rows
            else:
                logger.info(f"총 {total_rows:,}행 처리 예정 (배치 크기: {self.batch_size:,})")
            
            if total_rows == 0:
                logger.info("✅ 처리할 신규 데이터 없음")
                return
            
            # 배치별로 처리
            self._process_batches(where_clause, timestamp_col, total_rows)
            
            self.end_time = datetime.now()
            duration = (self.end_time - self.start_time).total_seconds()
            logger.info(f"[{self.job_name}] ✅ ETL 완료: {duration:.2f}초 소요")
            
        except Exception as e:
            logger.error(f"[{self.job_name}] ❌ ETL 실패: {e}")
            import traceback
            logger.error(traceback.format_exc())
            raise
        finally:
            if self.source_client:
                try:
                    self.source_client.close()
                except:
                    pass
            if self.target_client:
                try:
                    self.target_client.close()
                except:
                    pass
    
    def _process_batches(self, where_clause, timestamp_col, total_rows):
        """배치별 데이터 처리 (ORDER BY 제거로 메모리 절약)"""
        offset = 0
        batch_num = 0
        total_processed = 0
        consecutive_failures = 0
        max_consecutive_failures = 3
        
        while offset < total_rows:
            batch_num += 1
            
            # 남은 행 수 계산
            remaining = total_rows - offset
            current_batch_size = min(self.batch_size, remaining)
            
            # Extract (ORDER BY 제거 - 메모리 절약)
            # WHERE 조건으로 이미 타임스탬프 필터링되므로 ORDER BY 불필요
            query = f"SELECT * FROM `{self.table_name}` {where_clause} LIMIT {current_batch_size} OFFSET {offset}"
            
            logger.info(f"배치 {batch_num}: {offset:,} ~ {min(offset + current_batch_size, total_rows):,}")
            
            try:
                df_batch = self.source_client.query_df(query)
                consecutive_failures = 0
            except Exception as e:
                consecutive_failures += 1
                error_msg = str(e)
                
                # 메모리 부족 에러 감지
                if 'MEMORY_LIMIT_EXCEEDED' in error_msg:
                    logger.error(f"⚠️ 메모리 부족 감지, 배치 크기 축소 시도")
                    
                    # 배치 크기를 절반으로
                    if current_batch_size > 100:
                        smaller_batch = current_batch_size // 2
                        query = f"SELECT * FROM `{self.table_name}` {where_clause} LIMIT {smaller_batch} OFFSET {offset}"
                        
                        gc.collect()
                        
                        try:
                            logger.info(f"배치 크기 축소: {current_batch_size} → {smaller_batch}")
                            df_batch = self.source_client.query_df(query)
                            current_batch_size = smaller_batch  # 이후 배치도 작게
                            self.batch_size = smaller_batch
                            consecutive_failures = 0
                            logger.info(f"✅ 작은 배치로 성공")
                        except Exception as e2:
                            logger.error(f"배치 {batch_num} 축소 후에도 실패: {e2}")
                            # 스킵하고 다음으로
                            offset += smaller_batch
                            continue
                    else:
                        logger.error(f"배치 크기가 이미 최소값({current_batch_size}), 스킵")
                        offset += current_batch_size
                        continue
                else:
                    logger.error(f"배치 {batch_num} 추출 실패 ({consecutive_failures}/{max_consecutive_failures}): {e}")
                
                if consecutive_failures >= max_consecutive_failures:
                    logger.error(f"연속 {max_consecutive_failures}번 실패, ETL 중단")
                    logger.info(f"✅ {total_processed:,}건까지 처리 완료")
                    return  # 중단하지 않고 종료
                
                gc.collect()
                
                try:
                    logger.info(f"배치 {batch_num} 재시도...")
                    df_batch = self.source_client.query_df(query)
                    consecutive_failures = 0
                except Exception as retry_e:
                    logger.error(f"배치 {batch_num} 재시도 실패: {retry_e}")
                    offset += current_batch_size
                    continue
            
            if len(df_batch) == 0:
                logger.info(f"배치 {batch_num}: 데이터 없음, 종료")
                break
            
            # Transform
            try:
                df_transformed = self.transform(df_batch)
            except Exception as e:
                logger.error(f"배치 {batch_num} 변환 실패: {e}")
                offset += current_batch_size
                continue
            
            # Load
            try:
                self._load_batch(df_transformed)
            except Exception as e:
                logger.error(f"배치 {batch_num} 적재 실패: {e}")
                # 적재 실패는 중요하지만 계속 진행
                logger.warning(f"배치 {batch_num} 스킵하고 계속 진행")
                offset += current_batch_size
                continue
            
            total_processed += len(df_batch)
            progress = (total_processed / total_rows * 100) if total_rows > 0 else 0
            
            if batch_num % 10 == 0 or batch_num == 1:
                logger.info(f"📊 진행률: {total_processed:,}/{total_rows:,} ({progress:.1f}%) - 배치 {batch_num}")
            
            del df_batch
            del df_transformed
            gc.collect()
            
            offset += current_batch_size
        
        logger.info(f"✅ 총 {total_processed:,}건 처리 완료")
    
    def extract(self):
        pass
    
    def transform(self, data):
        """데이터 변환 (DateTime 타입 처리 강화)"""
        if len(data) == 0:
            return data
        
        # 1. 단일 datetime 컬럼 처리
        datetime_columns = ['StartTimeUnix', 'TimeUnix', 'Timestamp']
        for col in datetime_columns:
            if col in data.columns:
                if 'datetime64' in str(data[col].dtype):
                    data[col] = pd.to_datetime(data[col])
                    logger.debug(f"'{col}': datetime64 유지")
        
        # 2. 배열 컬럼 처리 - Events.*, Exemplars.*, Links.* 등
        array_columns = [col for col in data.columns if '.' in col]
        
        for col in array_columns:
            if col not in data.columns:
                continue
            
            # 배열 안에 datetime이 있는 경우
            if 'Timestamp' in col or 'TimeUnix' in col:
                def convert_datetime_array(val):
                    if not isinstance(val, list):
                        return []
                    
                    result = []
                    for item in val:
                        if pd.notna(item):
                            try:
                                # numpy.datetime64 → pandas Timestamp
                                if hasattr(item, '__class__') and 'datetime64' in str(type(item)):
                                    result.append(pd.Timestamp(item))
                                elif isinstance(item, str):
                                    result.append(pd.to_datetime(item))
                                elif hasattr(item, 'to_pydatetime'):
                                    result.append(pd.Timestamp(item))
                                else:
                                    result.append(pd.Timestamp(item))
                            except Exception as e:
                                logger.warning(f"배열 항목 변환 실패 ({col}): {e}")
                                pass
                    return result
                
                data[col] = data[col].apply(convert_datetime_array)
                logger.debug(f"'{col}': datetime 배열 변환 완료")
            
            # 일반 배열 (빈 배열로 처리)
            else:
                data[col] = data[col].apply(lambda x: x if isinstance(x, list) else [])
        
        # 3. Map 타입 컬럼
        map_columns = ['ResourceAttributes', 'ScopeAttributes', 'Attributes']
        for col in map_columns:
            if col in data.columns:
                data[col] = data[col].apply(lambda x: x if isinstance(x, dict) else {})
        
        # 4. 일반 NaN 처리
        for col in data.columns:
            try:
                if col in datetime_columns or col in array_columns or col in map_columns:
                    continue
                
                if data[col].dtype == 'object':
                    data[col] = data[col].fillna('')
                elif data[col].dtype in ['int64', 'int32', 'uint64', 'uint32', 'uint16', 'uint8']:
                    data[col] = data[col].fillna(0)
                elif data[col].dtype in ['float64', 'float32']:
                    data[col] = data[col].fillna(0.0)
            except:
                pass
        
        return data
    
    def _load_batch(self, data):
        """배치 데이터 적재"""
        if len(data) == 0:
            return
        
        try:
            self.target_client.insert_df(self.table_name, data)
        except Exception as e:
            logger.error(f"배치 적재 실패: {e}")
            raise
    
    def load(self, data):
        pass
    
    def _create_otel_table_from_source(self, table_name):
        """소스 ClickHouse의 테이블 구조를 복제"""
        try:
            show_create_result = self.source_client.query(f"SHOW CREATE TABLE `{table_name}`")
            create_statement = show_create_result.result_rows[0][0]
            
            create_statement = create_statement.replace('CREATE TABLE', 'CREATE TABLE IF NOT EXISTS', 1)
            
            self.target_client.command(create_statement)
            logger.info(f"✅ 테이블 `{table_name}` 생성/확인 완료")
        except Exception as e:
            if 'already exists' not in str(e).lower():
                logger.warning(f"테이블 생성 시도 중 경고: {e}")
    
    def __del__(self):
        if self.source_client:
            try:
                self.source_client.close()
            except:
                pass
        if self.target_client:
            try:
                self.target_client.close()
            except:
                pass