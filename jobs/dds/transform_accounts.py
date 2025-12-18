import psycopg2

def run():
    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow"
    )
    cursor = conn.cursor()

    cursor.execute("""
        SELECT account_number, contractor_inn, amount, created_at
        FROM ods.accounts
    """)
    rows = cursor.fetchall()

    for acc_number, inn, amount, created_at in rows:
        cursor.execute("""
            INSERT INTO dds.contractors(inn)
            VALUES (%s)
            ON CONFLICT (inn) DO NOTHING
            RETURNING id
        """, (inn,))

        result = cursor.fetchone()

        if result is None:
            cursor.execute(
                "SELECT id FROM dds.contractors WHERE inn = %s",
                (inn,)
            )
            contractor_id = cursor.fetchone()[0]
        else:
            contractor_id = result[0]

        cursor.execute("""
            INSERT INTO dds.accounts(
                account_number, contractor_id, amount, created_at
            )
            VALUES (%s, %s, %s, %s)
        """, (acc_number, contractor_id, amount, created_at))

    conn.commit()
    cursor.close()
    conn.close()
