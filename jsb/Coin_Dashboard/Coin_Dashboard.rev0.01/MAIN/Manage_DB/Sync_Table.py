import cx_Oracle

def sync_table_data(conn, source_table, target_table):
    """
    source_table에서 target_table로 데이터를 동기화합니다.
    중복 검사를 하지 않습니다.

    Parameters:
    - conn (cx_Oracle.Connection): Oracle 데이터베이스 연결 객체
    - source_table (str): 데이터 동기화 원본 테이블 이름
    - target_table (str): 데이터 동기화 대상 테이블 이름
    """
    try:
        with conn.cursor() as cursor:
            # SQL 쿼리 작성
            sync_query = f"""
                SELECT *
                FROM {source_table}
            """
            print(f"'{source_table}'에서 '{target_table}'로 데이터를 동기화합니다.")
            
            # 데이터 조회
            cursor.execute(sync_query)
            rows = cursor.fetchall()
            
            # 컬럼명 가져오기
            column_names = [desc[0] for desc in cursor.description]
            
            # INSERT 쿼리 준비
            insert_query = f"""
                INSERT INTO {target_table} ({', '.join(column_names)})
                VALUES ({', '.join([':' + col for col in column_names])})
            """
            
            # 배치 삽입
            cursor.executemany(insert_query, rows)
            inserted_rows = cursor.rowcount
            conn.commit()
            print(f"데이터 동기화가 완료되었습니다. {inserted_rows}개의 행이 '{target_table}'에 삽입되었습니다.")

    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print(f"데이터베이스 오류 발생: {error.message}")

    except Exception as e:
        print(f"데이터 동기화 중 오류가 발생했습니다: {e}")
