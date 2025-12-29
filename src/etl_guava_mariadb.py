import pandas as pd
from etl_base import ETLBase
from db_connectors import DatabaseConnector
import logging

logger = logging.getLogger(__name__)


class GuavaMariaDBETL(ETLBase):
    """Guava MariaDB에서 Target ClickHouse로 데이터 이관 (PK 기준 중복 제거)"""
    
    def __init__(self, table_name, query=None, primary_key=None):
        super().__init__(f"Guava_MariaDB_{table_name}")
        self.table_name = table_name
        self.base_query = query or f"SELECT * FROM {table_name}"
        
        # primary_key 처리 수정
        if primary_key is None:
            self.primary_key = None
        elif isinstance(primary_key, str):
            self.primary_key = [primary_key]  # 문자열 → 리스트
        elif isinstance(primary_key, (list, tuple)):
            self.primary_key = list(primary_key)  # 리스트/튜플 → 리스트
        else:
            self.primary_key = [str(primary_key)]  # 기타 → 문자열 변환 후 리스트
        
        self.source_conn = None
        self.target_client = None
        
    def extract(self):
        """MariaDB에서 데이터 추출"""
        try:
            self.source_conn = DatabaseConnector.get_mariadb_connection('GUAVA')
            self.target_client = DatabaseConnector.get_clickhouse_client('TARGET')
            
            logger.info(f"전체 데이터 추출 (PK '{self.primary_key}' 기준 중복 자동 제거)")
            
            df = pd.read_sql(self.base_query, self.source_conn)
            logger.info(f"추출 완료: {len(df):,}건")
            return df
            
        except Exception as e:
            logger.error(f"데이터 추출 실패: {e}")
            raise
    
    def transform(self, data):
        """데이터 변환"""
        if len(data) == 0:
            return data
        
        # 헤더 행 제거
        first_col = data.columns[0]
        data = data[data[first_col] != first_col]
        
        if len(data) == 0:
            logger.warning("헤더 행 제거 후 데이터가 비어있음")
            return data
        
        # 컬럼명을 소문자로 변환
        data.columns = [col.lower() for col in data.columns]
        
        # NULL 값 처리
        for col in data.columns:
            if data[col].dtype == 'object':
                data[col] = data[col].fillna('')
            elif data[col].dtype in ['int64', 'int32', 'float64', 'float32']:
                data[col] = data[col].fillna(0)
            elif data[col].dtype == 'datetime64[ns]':
                data[col] = data[col].apply(
                    lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notna(x) else None
                )
                logger.debug(f"DateTime 컬럼 '{col}' → String 변환")
        
        # 시스템 출처 컬럼 추가
        data['source_system'] = 'guava'
        data['source_table'] = self.table_name
        
        logger.info(f"변환 완료: {len(data):,}건")
        
        return data
    
    def load(self, data):
        """ClickHouse에 데이터 적재 (PK 기준 중복 제거)"""
        if len(data) == 0:
            logger.info("적재할 데이터 없음")
            return
            
        try:
            if not self.target_client:
                self.target_client = DatabaseConnector.get_clickhouse_client('TARGET')
                
            target_table = f"guava_{self.table_name}"
            
            result = self.target_client.query(f"EXISTS TABLE `{target_table}`")
            table_exists = result.result_rows[0][0]
            
            if not table_exists:
                self._create_table_if_not_exists(target_table, data)
            
            logger.info(f"삽입할 데이터: {len(data):,}건")
            
            column_names = list(data.columns)
            data_list = data.values.tolist()
            
            self.target_client.insert(
                table=target_table,
                data=data_list,
                column_names=column_names
            )
            
            logger.info(f"✅ {len(data):,}건 삽입 완료")
            
            logger.info(f"🔄 중복 제거 시작 (PK: {self.primary_key})...")
            self.target_client.command(f"OPTIMIZE TABLE `{target_table}` FINAL")
            logger.info(f"✅ 중복 제거 완료")
            
            verify_result = self.target_client.query(f"SELECT count() FROM `{target_table}`")
            total_count = verify_result.result_rows[0][0]
            logger.info(f"📊 최종 테이블 건수: {total_count:,}건")
            
        except Exception as e:
            logger.error(f"데이터 적재 실패: {e}")
            raise
    
    def _create_table_if_not_exists(self, table_name, df):
        """ClickHouse 테이블 자동 생성 (복합 PK 지원)"""
        type_mapping = {
            'int64': 'Int64',
            'int32': 'Int32',
            'uint64': 'UInt64',
            'uint32': 'UInt32',
            'float64': 'Float64',
            'float32': 'Float32',
            'object': 'String',
            'datetime64[ns]': 'String',
            'bool': 'UInt8'
        }
        
        columns = []
        
        if self.primary_key:
            missing_keys = [pk for pk in self.primary_key if pk not in df.columns]
            if missing_keys:
                logger.warning(f"지정된 PK {missing_keys}가 데이터에 없음, 자동 감지")
                self.primary_key = None
        
        if not self.primary_key:
            detected_pks = [col for col in df.columns if col.endswith('_id')]
            if detected_pks:
                self.primary_key = detected_pks
            else:
                self.primary_key = [df.columns[0]]
        
        pk_str = ', '.join(self.primary_key)
        logger.info(f"Primary Key: {pk_str}")
        
        # 컬럼 정의
        for col_name, dtype in df.dtypes.items():
            dtype_str = str(dtype)
            ch_type = type_mapping.get(dtype_str, 'String')
            
            # 리스트에 포함 확인
            if col_name in self.primary_key or col_name in ['source_system', 'source_table']:
                columns.append(f"`{col_name}` {ch_type}")
            else:
                columns.append(f"`{col_name}` Nullable({ch_type})")
        
        order_by_clause = ', '.join([f"`{pk}`" for pk in self.primary_key])
        
        create_table_sql = f"""
        CREATE TABLE `{table_name}` (
            {', '.join(columns)}
        ) ENGINE = ReplacingMergeTree()
        ORDER BY ({order_by_clause})
        """
        
        logger.info(f"테이블 생성 SQL:\n{create_table_sql}")
        
        self.target_client.command(create_table_sql)
        logger.info(f"✅ 테이블 `{table_name}` 생성 완료")
    
    def __del__(self):
        """리소스 정리"""
        if self.source_conn:
            self.source_conn.close()
        if self.target_client:
            self.target_client.close()