import cx_Oracle

def clone_table(conn, source_table, target_table):
    """
    Oracle 데이터베이스에서 원본 테이블을 대상 테이블로 복제합니다.
    대상 테이블이 이미 존재하는 경우 복제를 수행하지 않습니다.

    Parameters:
    - conn (cx_Oracle.Connection): Oracle 데이터베이스 연결 객체
    - source_table (str): 복제할 원본 테이블 이름
    - target_table (str): 생성할 대상 테이블 이름
    """
    try:
        cursor = conn.cursor()
        
        # 현재 연결된 사용자의 소유 테이블인지 확인
        # 'USER_TABLES'를 사용하여 현재 스키마의 테이블만 조회
        check_query = """
            SELECT COUNT(*)
            FROM user_tables
            WHERE table_name = :table_name         
        """
        cursor.execute(check_query, table_name=target_table.upper())
        count = cursor.fetchone()[0]
        
        if count > 0:
            print(f"테이블 '{target_table}'이(가) 이미 존재합니다. 복제를 건너뜁니다.")
            return
        else:
            # 대상 테이블이 존재하지 않으면 복제 수행
            # WHERE 1=0을 사용하여 구조만 복제하고 데이터를 제외하려면 이 조건을 추가
            # 전체 데이터를 복제하려면 WHERE 절을 제거하세요.
            clone_query = f"CREATE TABLE {target_table} AS SELECT * FROM {source_table}"
            cursor.execute(clone_query)
            conn.commit()
            print(f"테이블 '{target_table}'이(가) 성공적으로 '{source_table}'에서 복제되었습니다.")
    
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print(f"데이터베이스 오류 발생: {error.message}")
    
    except Exception as e:
        print(f"테이블 복제 중 오류가 발생했습니다: {e}")
    
    finally:
        # 커서를 반드시 닫아 자원을 해제
        cursor.close()
