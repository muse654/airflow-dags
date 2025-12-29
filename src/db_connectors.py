import os
from dotenv import load_dotenv
import clickhouse_connect
import pymysql
from opensearchpy import OpenSearch
import logging

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DatabaseConnector:
    """데이터베이스 연결을 관리하는 기본 클래스"""
    
    @staticmethod
    def get_mariadb_connection(prefix):
        """MariaDB 연결 생성"""
        try:
            conn = pymysql.connect(
                host=os.getenv(f'{prefix}_MARIADB_HOST'),
                port=int(os.getenv(f'{prefix}_MARIADB_PORT')),
                user=os.getenv(f'{prefix}_MARIADB_USER'),
                password=os.getenv(f'{prefix}_MARIADB_PASSWORD'),
                database=os.getenv(f'{prefix}_MARIADB_DATABASE')
            )
            logger.info(f"{prefix} MariaDB 연결 성공")
            return conn
        except Exception as e:
            logger.error(f"{prefix} MariaDB 연결 실패: {e}")
            raise
    
    @staticmethod
    def get_clickhouse_client(prefix):
        """ClickHouse 클라이언트 생성"""
        try:
            settings = {}
            
            # GUAVA 클라이언트는 메모리 절약 모드
            if prefix == 'GUAVA':
                settings = {
                    'max_memory_usage': '5000000000',  # 5GB로 제한
                    'max_block_size': '1000',  # 블록 크기 축소
                    'max_threads': '2',  # 스레드 수 제한
                }
            
            client = clickhouse_connect.get_client(
                host=os.getenv(f'{prefix}_CLICKHOUSE_HOST'),
                port=int(os.getenv(f'{prefix}_CLICKHOUSE_PORT')),
                username=os.getenv(f'{prefix}_CLICKHOUSE_USER'),
                password=os.getenv(f'{prefix}_CLICKHOUSE_PASSWORD'),
                database=os.getenv(f'{prefix}_CLICKHOUSE_DATABASE'),
                settings=settings
            )
            logger.info(f"{prefix} ClickHouse 연결 성공")
            return client
        except Exception as e:
            logger.error(f"{prefix} ClickHouse 연결 실패: {e}")
            raise
    
    @staticmethod
    def get_opensearch_client(prefix):
        """OpenSearch 클라이언트 생성"""
        try:
            client = OpenSearch(
                hosts=[{
                    'host': os.getenv(f'{prefix}_OPENSEARCH_HOST'),
                    'port': int(os.getenv(f'{prefix}_OPENSEARCH_PORT'))
                }],
                http_compress=True,
                use_ssl=False,
                verify_certs=False,
                ssl_assert_hostname=False,
                ssl_show_warn=False
            )
            logger.info(f"{prefix} OpenSearch 연결 성공")
            return client
        except Exception as e:
            logger.error(f"{prefix} OpenSearch 연결 실패: {e}")
            raise