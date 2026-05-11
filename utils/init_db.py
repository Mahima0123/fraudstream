from .db import get_connection

conn = get_connection()
cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS fraud_alerts (
    id SERIAL PRIMARY KEY,
    alert_type TEXT,
    event JSONB,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
)
""")

conn.commit()
cur.close()
conn.close()

print("Database initialized.")