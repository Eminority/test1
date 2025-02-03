import cx_Oracle

def clone_table(conn, source_table, target_table):
    """
    Oracle 데이터베이스에서 원본 테이블을 대상 테이블로 복제하고,
    SOURCE_TABLE의 PRIMARY KEY를 TARGET_TABLE에 동일하게 적용합니다.
    
    대상 테이블이 이미 존재하는 경우 복제를 수행하지 않습니다.
    복제된 테이블에 SOURCE_TABLE과 동일한 컬럼으로 PRIMARY KEY를 설정합니다.

    Parameters:
    - conn (cx_Oracle.Connection): Oracle 데이터베이스 연결 객체
    - source_table (str): 복제할 원본 테이블 이름
    - target_table (str): 생성할 대상 테이블 이름
    """
    try:
        cursor = conn.cursor()
        
        # 대상 테이블 존재 여부 확인
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
            clone_query = f"CREATE TABLE {target_table} AS SELECT * FROM {source_table}"
            cursor.execute(clone_query)
            conn.commit()
            print(f"테이블 '{target_table}'이(가) 성공적으로 '{source_table}'에서 복제되었습니다.")

            # SOURCE_TABLE의 PRIMARY KEY 컬럼 조회
            pk_query = """
                SELECT cols.column_name
                FROM user_constraints cons
                JOIN user_cons_columns cols ON cons.constraint_name = cols.constraint_name
                WHERE cons.table_name = :table_name
                  AND cons.constraint_type = 'P'
                ORDER BY cols.position
            """
            cursor.execute(pk_query, table_name=source_table.upper())
            pk_columns = [row[0] for row in cursor.fetchall()]

            if not pk_columns:
                print(f"원본 테이블 '{source_table}'에 PRIMARY KEY가 없습니다. PRIMARY KEY 설정 없이 완료합니다.")
            else:
                # PRIMARY KEY 컬럼이 하나 이상일 수 있으므로 콤마로 연결
                pk_columns_str = ", ".join(pk_columns)

                constraint_name = f"PK_{target_table.upper()}"
                alter_query = f"ALTER TABLE {target_table} ADD CONSTRAINT {constraint_name} PRIMARY KEY ({pk_columns_str})"
                cursor.execute(alter_query)
                conn.commit()
                print(f"테이블 '{target_table}'에 '{source_table}'와 동일한 PRIMARY KEY({pk_columns_str})가 설정되었습니다.")

    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print(f"데이터베이스 오류 발생: {error.message}")
    except Exception as e:
        print(f"테이블 복제 중 오류가 발생했습니다: {e}")
    finally:
        # 커서를 반드시 닫아 자원을 해제
        cursor.close()
