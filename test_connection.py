from db import DEFAULT_DB_PATH, get_db, list_tables

def main():
    print(f"Connecting to DuckDB database at: {DEFAULT_DB_PATH}")
    
    with get_db(read_only=True) as conn:
        version = conn.execute("SELECT version()").fetchone()[0]
        print(f"DuckDB Engine Version: {version}")

    tables = list_tables()
    print(f"\nSuccessfully connected! Found {len(tables)} tables:")
    for idx, table in enumerate(tables[:15], 1):
        print(f"  {idx}. {table}")
    
    if len(tables) > 15:
        print(f"  ... and {len(tables) - 15} more tables.")

if __name__ == "__main__":
    main()
