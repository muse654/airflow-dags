import pandas as pd
from etl_base import ETLBase
from db_connectors import DatabaseConnector
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class AppleMangoOpenSearchETL(ETLBase):
    """AppleMango OpenSearch에서 Target ClickHouse로 데이터 이관 (각 인덱스별 최대 10만 건)"""
    
    def __init__(self, index_pattern='perfhist-*', timestamp_field='@timestamp', max_rows_per_index=100000):
        super().__init__(f"AppleMango_OpenSearch_{index_pattern}")
        self.index_pattern = index_pattern
        self.timestamp_field = timestamp_field
        self.max_rows_per_index = max_rows_per_index
        self.source_client = None
        self.target_client = None
        
    def run(self):
        """ETL 실행 - 각 인덱스별로 처리"""
        try:
            self.start_time = datetime.now()
            logger.info(f"[{self.job_name}] ETL 시작: {self.start_time}")
            
            self.source_client = DatabaseConnector.get_opensearch_client('APPLEMANGO')
            self.target_client = DatabaseConnector.get_clickhouse_client('TARGET')
            
            # 패턴에 맞는 실제 인덱스 목록 가져오기
            indices = self._get_matching_indices()
            
            if not indices:
                logger.warning(f"패턴 '{self.index_pattern}'에 맞는 인덱스 없음")
                return
            
            logger.info(f"처리할 인덱스: {len(indices)}개 - {indices}")
            
            # 각 인덱스별로 ETL 수행
            for index_name in indices:
                try:
                    logger.info(f"\n{'='*80}")
                    logger.info(f"인덱스 처리 시작: {index_name}")
                    logger.info(f"{'='*80}")
                    
                    self._process_single_index(index_name)
                    
                except Exception as e:
                    logger.error(f"❌ 인덱스 '{index_name}' 처리 실패: {e}")
                    import traceback
                    logger.error(traceback.format_exc())
                    continue
            
            self.end_time = datetime.now()
            duration = (self.end_time - self.start_time).total_seconds()
            logger.info(f"\n[{self.job_name}] ✅ 전체 ETL 완료: {duration:.2f}초 소요")
            
        except Exception as e:
            logger.error(f"[{self.job_name}] ❌ ETL 실패: {e}")
            import traceback
            logger.error(traceback.format_exc())
            raise
        finally:
            if self.target_client:
                try:
                    self.target_client.close()
                except:
                    pass
    
    def _get_matching_indices(self):
        """패턴에 맞는 실제 인덱스 목록 조회"""
        try:
            response = self.source_client.cat.indices(index=self.index_pattern, format='json')
            indices = [idx['index'] for idx in response]
            indices.sort()
            logger.info(f"발견된 인덱스: {indices}")
            return indices
        except Exception as e:
            logger.error(f"인덱스 목록 조회 실패: {e}")
            return []
    
    def _process_single_index(self, index_name):
        """단일 인덱스 처리"""
        if 'sms' in index_name.lower():
            target_table = 'perfhist_sms'
        elif 'nms' in index_name.lower():
            target_table = 'perfhist_nms'
        else:
            target_table = f"perfhist_{index_name.split('-')[1]}" if '-' in index_name else 'perfhist_other'
        
        logger.info(f"타겟 테이블: {target_table}")
        
        data = self._extract_from_index(index_name, target_table)
        
        if len(data) == 0:
            logger.info(f"인덱스 '{index_name}'에서 처리할 데이터 없음")
            return
        
        data_transformed = self.transform(data)
        self._load_to_table(data_transformed, target_table)  
    
    def _extract_from_index(self, index_name, target_table):
        """특정 인덱스에서 증분 데이터 추출 (최대 10만 건)"""
        try:
            last_timestamp = self._get_last_timestamp(target_table)
            
            if last_timestamp:
                logger.info(f"✅ 증분 마이그레이션: {last_timestamp} 이후 데이터")
                query = {
                    "query": {
                        "range": {
                            self.timestamp_field: {
                                "gt": last_timestamp.isoformat()
                            }
                        }
                    },
                    "sort": [{self.timestamp_field: {"order": "asc"}}]
                }
            else:
                logger.info("⚠️ 전체 마이그레이션: 타겟 테이블이 비어있음")
                query = {
                    "query": {"match_all": {}},
                    "sort": [{self.timestamp_field: {"order": "asc"}}]
                }
            
            logger.info(f"⚠️ 최대 처리 행 수: {self.max_rows_per_index:,}건")
            
            results = []
            page = self.source_client.search(
                index=index_name,
                body=query,
                scroll='2m',
                size=1000
            )
            
            scroll_id = page['_scroll_id']
            hits = page['hits']['hits']
            results.extend(hits)
            
            logger.info(f"첫 배치: {len(hits):,}개 문서")
            
            while len(hits) > 0 and len(results) < self.max_rows_per_index:
                page = self.source_client.scroll(scroll_id=scroll_id, scroll='2m')
                scroll_id = page['_scroll_id']
                hits = page['hits']['hits']
                
                remaining = self.max_rows_per_index - len(results)
                if len(hits) > remaining:
                    hits = hits[:remaining]
                
                results.extend(hits)
                
                if len(hits) > 0:
                    logger.info(f"추가 배치: {len(hits):,}개 문서 (누적: {len(results):,}개)")
                
                if len(results) >= self.max_rows_per_index:
                    logger.warning(f"⚠️ 최대 {self.max_rows_per_index:,}건 도달, 추출 중단")
                    break
            
            data = [hit['_source'] for hit in results]
            df = pd.DataFrame(data)
            
            logger.info(f"인덱스 '{index_name}'에서 총 {len(df):,}개 문서 추출 완료")
            return df
            
        except Exception as e:
            logger.error(f"인덱스 '{index_name}' 데이터 추출 실패: {e}")
            raise
    
    def _get_last_timestamp(self, table_name):
        """특정 테이블의 마지막 타임스탬프 조회"""
        try:
            result = self.target_client.query(f"EXISTS TABLE `{table_name}`")
            table_exists = result.result_rows[0][0]
            
            if not table_exists:
                logger.info(f"타겟 테이블 `{table_name}` 없음")
                return None
            
            count_result = self.target_client.query(f"SELECT count() FROM `{table_name}`")
            row_count = count_result.result_rows[0][0]
            
            if row_count == 0:
                logger.info(f"타겟 테이블 `{table_name}` 비어있음")
                return None
            
            result = self.target_client.query(f"""
                SELECT max(`opensearch_timestamp`) 
                FROM `{table_name}`
            """)
            
            last_ts = result.result_rows[0][0]
            
            if last_ts:
                if isinstance(last_ts, str):
                    last_ts = pd.to_datetime(last_ts)
                elif hasattr(last_ts, 'to_pydatetime'):
                    last_ts = last_ts.to_pydatetime()
                
                logger.info(f"'{table_name}'의 마지막 타임스탬프: {last_ts} ({row_count:,}행)")
                return last_ts
            return None
            
        except Exception as e:
            logger.warning(f"{table_name} 마지막 타임스탬프 조회 실패: {e}")
            return None
    
    def transform(self, data):
        """데이터 변환 (bool을 string으로)"""
        if len(data) == 0:
            return data
        
        new_columns = {}
        for col in data.columns:
            if col == '@timestamp':
                new_col = 'opensearch_timestamp'
            elif col == '@metadata':
                new_col = 'opensearch_metadata'
            elif col == '@version':
                new_col = 'opensearch_version'
            else:
                new_col = col.replace('.', '_').replace('-', '_').replace('@', 'at_')
            
            if new_col in new_columns.values():
                suffix = 1
                while f"{new_col}_{suffix}" in new_columns.values():
                    suffix += 1
                new_col = f"{new_col}_{suffix}"
            
            new_columns[col] = new_col
        
        data = data.rename(columns=new_columns)
        
        for col in data.columns:
            dtype = data[col].dtype
            
            try:
                if 'datetime64' in str(dtype):
                    pass
                elif dtype == 'bool':
                    data[col] = data[col].astype(str).replace({'True': 'true', 'False': 'false'})
                elif dtype in ['int64', 'int32', 'int16', 'int8', 'uint64', 'uint32', 'uint16', 'uint8']:
                    data[col] = data[col].fillna(0)
                elif dtype in ['float64', 'float32']:
                    data[col] = data[col].fillna(0.0)
                elif dtype == 'object':
                    def safe_convert(x):
                        if pd.isna(x):
                            return None
                        elif isinstance(x, bool):
                            return 'true' if x else 'false'
                        else:
                            return x
                    data[col] = data[col].apply(safe_convert)
            except Exception as e:
                logger.warning(f"컬럼 '{col}' 처리 중 경고: {e}")
        
        logger.info(f"✅ 변환 완료: {len(data):,}행, {len(data.columns)}개 컬럼")
        return data
    
    def _load_to_table(self, data, table_name):
        """특정 테이블에 데이터 적재 (동적 컬럼 추가)"""
        if len(data) == 0:
            return
        
        # 테이블 생성
        self._create_table_if_not_exists(table_name, data)
        
        # 기존 테이블에 없는 컬럼 추가 ⭐ 핵심
        self._add_missing_columns(table_name, data)
        
        # 배치로 삽입
        total_rows = len(data)
        batch_size = 10000
        
        logger.info(f"[{table_name}] {total_rows:,}행 적재 시작 (배치: {batch_size:,})")
        
        for i in range(0, total_rows, batch_size):
            batch_data = data.iloc[i:i+batch_size]
            try:
                self.target_client.insert_df(table_name, batch_data)
                logger.info(f"[{table_name}] 진행: {min(i+batch_size, total_rows):,}/{total_rows:,}")
            except Exception as e:
                logger.error(f"[{table_name}] 배치 {i//batch_size + 1} 적재 실패: {e}")
                raise
        
        logger.info(f"✅ [{table_name}] {total_rows:,}행 적재 완료")
    
    def _add_missing_columns(self, table_name, df):
        """기존 테이블에 없는 컬럼 추가"""
        try:
            schema_result = self.target_client.query(f"DESCRIBE TABLE `{table_name}`")
            existing_columns = {row[0] for row in schema_result.result_rows}
            
            new_columns = set(df.columns)
            missing_columns = new_columns - existing_columns
            
            if not missing_columns:
                return
            
            logger.info(f"[{table_name}] 누락된 컬럼 {len(missing_columns)}개 추가: {missing_columns}")
            
            type_mapping = {
                'int64': 'Int64',
                'int32': 'Int32',
                'int16': 'Int16',
                'int8': 'Int8',
                'uint64': 'UInt64',
                'uint32': 'UInt32',
                'uint16': 'UInt16',
                'uint8': 'UInt8',
                'float64': 'Float64',
                'float32': 'Float32',
                'object': 'String',
                'datetime64[ns]': 'DateTime64(3)',
                'bool': 'String',
            }
            
            for col in missing_columns:
                dtype_str = str(df[col].dtype)
                ch_type = type_mapping.get(dtype_str, 'String')
                
                try:
                    alter_sql = f"ALTER TABLE `{table_name}` ADD COLUMN `{col}` Nullable({ch_type})"
                    self.target_client.command(alter_sql)
                    logger.info(f"✅ [{table_name}] 컬럼 추가: `{col}` {ch_type}")
                except Exception as e:
                    if 'already exists' in str(e).lower():
                        pass
                    else:
                        logger.error(f"❌ [{table_name}] 컬럼 `{col}` 추가 실패: {e}")
            
        except Exception as e:
            logger.error(f"[{table_name}] 컬럼 추가 중 오류: {e}")
            raise
    
    def _create_table_if_not_exists(self, table_name, df):
        """ClickHouse 테이블 자동 생성"""
        type_mapping = {
            'int64': 'Int64',
            'int32': 'Int32',
            'int16': 'Int16',
            'int8': 'Int8',
            'uint64': 'UInt64',
            'uint32': 'UInt32',
            'uint16': 'UInt16',
            'uint8': 'UInt8',
            'float64': 'Float64',
            'float32': 'Float32',
            'object': 'String',
            'datetime64[ns]': 'DateTime64(3)',
            'bool': 'String',
        }
        
        columns = []
        
        for col_name, dtype in df.dtypes.items():
            dtype_str = str(dtype)
            ch_type = type_mapping.get(dtype_str, 'String')
            
            if col_name == 'opensearch_timestamp':
                columns.append(f"`{col_name}` {ch_type}")
            else:
                columns.append(f"`{col_name}` Nullable({ch_type})")
        
        if 'opensearch_timestamp' in df.columns:
            order_by = "ORDER BY `opensearch_timestamp`"
        else:
            order_by = "ORDER BY tuple()"
        
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS `{table_name}` (
            {', '.join(columns)}
        ) ENGINE = MergeTree
        {order_by}
        """
        
        self.target_client.command(create_table_sql)
        logger.info(f"✅ 테이블 `{table_name}` 생성/확인 완료")
    
    def extract(self):
        pass
    
    def load(self, data):
        pass
    
    def __del__(self):
        if self.target_client:
            try:
                self.target_client.close()
            except:
                pass