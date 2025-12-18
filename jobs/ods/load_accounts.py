import psycopg2

def run():
    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow"
    )
    cursor = conn.cursor()

    sample_data = [
        ("ACC-001", "7701234567", 1000),
        ("ACC-002", "7809876543", 2500),
        ("ACC-003", "7701234567", 500),
    ]

    for acc in sample_data:
        cursor.execute("""
            INSERT INTO ods.accounts(
                account_number,
                contractor_inn,
                amount
            )
            VALUES (%s, %s, %s)
        """, acc)

    conn.commit()
    cursor.close()
    conn.close()
