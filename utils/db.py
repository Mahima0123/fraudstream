import psycopg2

def get_connection():
    return psycopg2.connect(
        host="localhost",
        database="fraudstream",
        user="fraud_user",
        password="fraud_pass",
        port=5432
    )