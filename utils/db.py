import psycopg2

def get_connection():
    return psycopg2.connect(
        host="127.0.0.1",
        database="fraudstream",
        user="fraud_user",
        password="fraud_pass",
        port=5433
    )