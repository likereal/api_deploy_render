import csv
import sqlite3

def csv_to_sqlite(csv_path, db_path):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute('CREATE TABLE IF NOT EXISTS bse_securities (scrip_code NUMBER, name TEXT, Security_Id TEXT)')
    with open(csv_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            c.execute('INSERT INTO bse_securities (scrip_code, name, Security_Id) VALUES (?, ?)', (row['SCRIP_CODE'], row['NAME'],row['Security_Id']))
    conn.commit()
    conn.close()

# Run this once to create the DB
# csv_to_sqlite('bse_securities.csv', 'bse_securities.db')

def nl2sql_stock_name(nl_query, db_path='bse_securities.db'):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    # Normalize input for better matching
    search = nl_query.replace(' ', '').lower()
    # Try exact match first
    c.execute("SELECT scrip_code, name FROM bse_securities WHERE REPLACE(LOWER(name), ' ', '') = ?", (search,))
    result = c.fetchone()
    if not result:
        # Try partial match
        c.execute("SELECT scrip_code, name FROM bse_securities WHERE LOWER(name) LIKE ?", (f"%{nl_query.lower()}%",))
        result = c.fetchone()
    conn.close()
    return result  # (scrip_code, name) or None


csv_to_sqlite('Equity.csv', 'bse_securities.db')
