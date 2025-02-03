def add_missing_columns(conn, table_name, required_columns):
    try:
        cursor = conn.cursor()

        # 기존 컬럼 정보를 가져오기
        cursor.execute(f"SELECT COLUMN_NAME FROM USER_TAB_COLUMNS WHERE TABLE_NAME = '{table_name.upper()}'")
        existing_columns = {row[0] for row in cursor.fetchall()}

        # 누락된 컬럼을 확인하고 추가
        for column_name, column_def in required_columns.items():
            if column_name.upper() not in existing_columns:
                print(f"Adding missing column: {column_name}")
                cursor.execute(f"ALTER TABLE {table_name} ADD {column_name} {column_def}")

        conn.commit()
        print("Missing columns added successfully.")

    except Exception as e:
        print(f"Error while adding missing columns: {e}")