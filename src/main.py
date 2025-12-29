import logging
from datetime import datetime
import os
import sys

# 로그 디렉토리 생성
current_dir = os.path.dirname(os.path.abspath(__file__))
project_dir = os.path.dirname(current_dir)
log_dir = os.path.join(project_dir, 'logs')
os.makedirs(log_dir, exist_ok=True)

# 로그 파일명
log_filename = f'etl_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
log_filepath = os.path.join(log_dir, log_filename)

# 기존 핸들러 제거
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

# 새로운 로거 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# 포맷 설정
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# 파일 핸들러
file_handler = logging.FileHandler(log_filepath, encoding='utf-8', mode='w')
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# 콘솔 핸들러
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# 로그 시작
logger.info(f"=" * 80)
logger.info(f"로그 파일: {log_filepath}")
logger.info(f"=" * 80)



from etl_applemango_mariadb import AppleMangoMariaDBETL
from etl_applemango_opensearch import AppleMangoOpenSearchETL
from etl_guava_mariadb import GuavaMariaDBETL
from etl_guava_clickhouse import GuavaClickHouseETL



def run_all_etl():
    """모든 ETL 작업 실행"""
    logger.info("=" * 80)
    logger.info("ETL 파이프라인 시작")
    logger.info("=" * 80)
    
    try:
        # ========================================
        # AppleMango ETL
        # ========================================
        
        logger.info("\n" + "=" * 80)
        logger.info("AppleMango 시스템 ETL")
        logger.info("=" * 80)
        
        # AppleMango MariaDB 테이블들 (테이블명, PK)
        applemango_mariadb_tables = [
            ('obj', 'obj_id'),
            ('objtype', 'objtype_id'),
            ('obj_rsc_key', 'hashkey_id'),
            ('rsc', ['managetype_id', 'rsctype_id', 'rsc_id']),
            ('rsctype', ['managetype_id', 'rsctype_id']),
            ('eventlevel', 'eventlevel_id'),
            ('event_hist', 'seq_number'),
            ('lms_field_info', 'field_id'),
            ('lms_data_field', ['dataindex_id', 'field_full_id']),
            ('lms_data_index', 'dataindex_id'),
            ('topologymap_element', ['map_id', 'element_id']),
            ('map_model_obj', 'model_id'),
            ('map_element', 'element_id'),
            ('obj_company', 'obj_company_id'),
            ('server', 'server_id'),
            ('netdevice', 'netdevice_id'),
            ('dbms', 'dbms_id'),
            ('netinterface', ['netdevice_id', 'netinterface_id']),
            ('map_model_obj_automap', ['model_id', 'ipaddress']),
        ]
        
        for idx, (table_name, primary_key) in enumerate(applemango_mariadb_tables, 1):
            logger.info(f"\n[{idx}/26] AppleMango MariaDB - {table_name} 테이블")
            try:
                AppleMangoMariaDBETL(
                    table_name=table_name,
                    primary_key=primary_key
                ).run()
            except Exception as e:
                logger.error(f"❌ {table_name} 테이블 ETL 실패: {e}")
                continue
        
        # AppleMango OpenSearch - perfhist-* (각 인덱스별 최대 10만 건)
        logger.info(f"\n[{len(applemango_mariadb_tables)+1}/26] AppleMango OpenSearch - perfhist-* (각 인덱스별 최대 10만 건)")
        try:
            AppleMangoOpenSearchETL(
                index_pattern='perfhist-*',
                timestamp_field='@timestamp',
                max_rows_per_index=100000  # 각 인덱스당 10만 건
            ).run()
        except Exception as e:
            logger.error(f"❌ perfhist-* 인덱스 ETL 실패: {e}")
        
        # ========================================
        # Guava ETL
        # ========================================
        
        logger.info("\n" + "=" * 80)
        logger.info("Guava 시스템 ETL")
        logger.info("=" * 80)
        
        # Guava MariaDB 테이블들 (테이블명, PK)
        guava_mariadb_tables = [
            ('obj', 'obj_id'),
            ('obj_topology', ['from_obj_id', 'to_obj_id']),
            ('incident', 'incident_uid'),
            ('incident_event_hist', ['incident_uid', 'event_hist_uid']),
            ('event_hist', 'uid'),
            ('eventlevel', 'eventlevel_id'),
        ]
        
        base_idx = len(applemango_mariadb_tables) + 2
        for idx, (table_name, primary_key) in enumerate(guava_mariadb_tables, base_idx):
            logger.info(f"\n[{idx}/26] Guava MariaDB - {table_name} 테이블")
            try:
                GuavaMariaDBETL(
                    table_name=table_name,
                    primary_key=primary_key
                ).run()
            except Exception as e:
                logger.error(f"❌ {table_name} 테이블 ETL 실패: {e}")
                continue
        
        # Guava ClickHouse OTel 테이블들 (최대 10만 건)
        guava_clickhouse_tables = [
            'otel_metrics_gauge',
            'otel_metrics_sum',
            'otel_metrics_histogram',
            'otel_traces',
            'otel_logs'
        ]
        
        base_idx = len(applemango_mariadb_tables) + 1 + len(guava_mariadb_tables) + 1
        for idx, table_name in enumerate(guava_clickhouse_tables, base_idx):
            logger.info(f"\n[{idx}/26] Guava ClickHouse - {table_name} 테이블 (최대 10만 건)")
            try:
                GuavaClickHouseETL(
                    table_name=table_name,
                    batch_size=500,
                    max_rows=100000
                ).run()
            except Exception as e:
                logger.error(f"❌ {table_name} 테이블 ETL 실패: {e}")
                continue
        
        logger.info("\n" + "=" * 80)
        logger.info("✅ 모든 ETL 작업 완료!")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.error(f"\n❌ ETL 파이프라인 실행 중 오류 발생: {e}")
        raise


def run_applemango_only():
    """AppleMango만 실행"""
    logger.info("=" * 80)
    logger.info("AppleMango ETL만 실행")
    logger.info("=" * 80)
    
    # MariaDB
    applemango_mariadb_tables = [
        ('obj', 'obj_id'),
        ('objtype', 'objtype_id'),
        ('obj_rsc_key', 'hashkey_id'),
        ('rsc', ['managetype_id', 'rsctype_id', 'rsc_id']),
        ('rsctype', ['managetype_id', 'rsctype_id']),
        ('eventlevel', 'eventlevel_id'),
        ('event_hist', 'seq_number'),
        ('lms_field_info', 'field_id'),
        ('lms_data_field', ['dataindex_id', 'field_full_id']),
        ('lms_data_index', 'dataindex_id'),
        ('topologymap_element', ['map_id', 'element_id']),
        ('map_model_obj', 'model_id'),
        ('map_element', 'element_id'),
        ('obj_company', 'obj_company_id'),
        ('server', 'server_id'),
        ('netdevice', 'netdevice_id'),
        ('dbms', 'dbms_id'),
        ('netinterface', ['netdevice_id', 'netinterface_id']),
        ('map_model_obj_automap', ['model_id', 'ipaddress']),
    ]
    
    for table_name, primary_key in applemango_mariadb_tables:
        try:
            AppleMangoMariaDBETL(table_name=table_name, primary_key=primary_key).run()
        except Exception as e:
            logger.error(f"❌ {table_name} 실패: {e}")
            continue
    
    # OpenSearch
    try:
        AppleMangoOpenSearchETL(index_pattern='perfhist-*', timestamp_field='@timestamp', max_rows_per_index=100000).run()
    except Exception as e:
        logger.error(f"❌ perfhist-* 실패: {e}")


def run_guava_only():
    """Guava만 실행"""
    logger.info("=" * 80)
    logger.info("Guava ETL만 실행")
    logger.info("=" * 80)
    
    # MariaDB
    guava_mariadb_tables = [
        ('obj', 'obj_id'),
        ('obj_topology', ['from_obj_id', 'to_obj_id']),
        ('incident', 'incident_uid'),
        ('incident_event_hist', ['incident_uid', 'event_hist_uid']),
        ('event_hist', 'uid'),
        ('eventlevel', 'eventlevel_id'),
    ]
    
    for table_name, primary_key in guava_mariadb_tables:
        try:
            GuavaMariaDBETL(table_name=table_name, primary_key=primary_key).run()
        except Exception as e:
            logger.error(f"❌ {table_name} 실패: {e}")
            continue
    
    # ClickHouse OTel 테이블들
    guava_clickhouse_tables = [
        'otel_metrics_gauge',
        'otel_metrics_sum',
        'otel_metrics_histogram',
        'otel_traces',
        'otel_logs'
    ]
    
    for table_name in guava_clickhouse_tables:
        try:
            GuavaClickHouseETL(table_name=table_name, batch_size=500, max_rows=100000).run()
        except Exception as e:
            logger.error(f"❌ {table_name} 실패: {e}")
            continue


if __name__ == "__main__":
    # 전체 실행
    run_all_etl()
    
    # 또는 개별 실행
    # run_applemango_only()
    # run_guava_only()