import psycopg2
import csv

def connect():
    return psycopg2.connect(
        host="localhost",
        dbname="phonebook",
        user="postgres",
        password="Lovelovelove3."
    )

def create_table():
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS phonebook (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(50),
                    phone VARCHAR(20)
                )
            """) # Python арқылы SQL сұранысын орындау


# INSERT FROM CONSOLE 
def upsert_user(p_name, p_phone):
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("CALL upsert_user(%s, %s)", (p_name, p_phone))
    print("Upserted!")



"""def insert_from_console():
    name = input("Enter name: ")
    phone = input("Enter phone: ")
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO phonebook (name, phone) VALUES (%s, %s)", (name, phone))
    print("Added!")"""

# INSERT FROM CSV
def insert_from_csv(path):
    with connect() as conn:
        with conn.cursor() as cur:
            with open(path, 'r') as f:
                reader = csv.reader(f)
                for row in reader:
                    cur.execute("INSERT INTO phonebook (name, phone) VALUES (%s, %s)", (row[0], row[1]))
    print("CSV imported!")

# UPDATE 
"""def update_user(name, new_phone):
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE phonebook SET phone = %s WHERE name = %s", (new_phone, name))
    print("Updated!")"""

# QUERY 
def query_all():
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM phonebook")
            for row in cur.fetchall():
                print(row)

"""def query_by_name(name):
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM phonebook WHERE name = %s", (name,))
            print(cur.fetchall()) # сұраныстың бәрін тізім (list) түрінде қайтарады."""

def search_by_pattern(pattern):
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM search_phonebook(%s)", (pattern,))
            results = cur.fetchall()
            for row in results:
                print(row)

# DELETE
"""def delete_user(name):
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM phonebook WHERE name = %s", (name,))
    print("Deleted!")"""
def delete_user(value):
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("CALL delete_user_by_value(%s)", (value,))



def bulk_insert_users(names, phones):
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("CALL bulk_insert_users(%s::TEXT[], %s::TEXT[])", (names, phones))

def query_with_pagination(limit, offset):
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM get_users_paginated(%s, %s)", (limit, offset))
            results = cur.fetchall()
            for row in results:
                print(row)

# MAIN MENU
def menu():
    while True:
        print("\n1. Upsert user (insert or update)")
        print("2. Insert (CSV)")
        print("3. Insert many users")
        print("4. View all")
        print("5. Search by pattern")
        print("6. Delete user")
        print("7. Exit")
        print("8. View with pagination")


        choice = input("Choose: ")

        if choice == '1':
            upsert_user(input("Name: "), input("Phone: "))
        elif choice == '2':
            insert_from_csv(input("CSV path: "))
        elif choice == '3':
            names = input("Enter names (comma-separated): ").split(',')
            phones = input("Enter phones (comma-separated): ").split(',')
            bulk_insert_users(names, phones)
        elif choice == '4':
            query_all()
        elif choice == '5':
            search_by_pattern(input("Pattern: "))
        elif choice == '6':
            value = input("Enter name or phone to delete: ")
            delete_user(value)
        elif choice == '7':
            break
        elif choice == '8':
            limit = int(input("Enter limit (how many to show): "))
            offset = int(input("Enter offset (how many to skip): "))
            query_with_pagination(limit, offset)
        else:
            print("Invalid choice.")


if __name__ == "__main__":
    create_table()
    menu()
