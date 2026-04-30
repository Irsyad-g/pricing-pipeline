import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="gkomunika",
    user="postgres",
    password="postgres"
)

cur = conn.cursor()

# Lihat semua tabel
cur.execute("""
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'public'
""")

print("=== TABEL ===")
for row in cur.fetchall():
    print(row[0])

cur.close()
conn.close()