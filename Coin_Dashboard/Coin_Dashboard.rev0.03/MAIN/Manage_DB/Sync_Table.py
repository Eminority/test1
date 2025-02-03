import cx_Oracle

def sync_table_data(conn, source_table, target_table):
    """
    source_table에서 target_table로 데이터를 MERGE를 사용하여 동기화합니다.
    PK로 MARKET, CANDLE_DATE_TIME_UTC 컬럼을 가정했습니다.
    """
    try:
        with conn.cursor() as cursor:
            # 소스 테이블 컬럼명 가져오기
            cursor.execute(f"SELECT * FROM {source_table} WHERE ROWNUM = 1")
            column_names = [desc[0] for desc in cursor.description]

            # MERGE 구문
            # PK로 (MARKET, CANDLE_DATE_TIME_UTC)를 사용한다고 가정
            merge_query = f"""
                MERGE INTO {target_table} t
                USING {source_table} s
                ON (t.MARKET = s.MARKET AND t.CANDLE_DATE_TIME_UTC = s.CANDLE_DATE_TIME_UTC)
                WHEN NOT MATCHED THEN
                  INSERT ({', '.join(column_names)})
                  VALUES ({', '.join(['s.' + col for col in column_names])})
            """

            print(f"'{source_table}'에서 '{target_table}'로 데이터를 MERGE를 통해 동기화합니다.")
            cursor.execute(merge_query)
            conn.commit()
            print("데이터 동기화(MERGE)가 완료되었습니다.")

    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print(f"데이터베이스 오류 발생: {error.message}")

    except Exception as e:
        print(f"데이터 동기화 중 오류가 발생했습니다: {e}")
