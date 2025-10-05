# Maestro Database (MDB)

Maestro Database (MDB) is a lightweight Python-based database system that stores tables as JSON files. It supports **CLI**, **GUI**, and **REST API** interfaces for managing data, with optional schema validation, transactions, backups, and CSV export.

---

## ⚠️ Disclaimer

MDB was created **for learning, fun, and educational purposes only**.  
It is **not intended for production use** and may not comply with data protection, privacy, or other legal requirements. DO NOT USE FOR STORING SENSITIVE DATA, YOU ARE RESPONISBLE FOR THE PROTECTION OF YOUR OWN DATA IF YOU USE THIS AND SOMETHING HAPPENS YOU HAVE BEEN WARNED AND I TAKE NO RESPONSIBILITY.

---

## Features

- Create, load, drop tables
- Insert, update, delete, and query records
- Optional table schemas for data validation
- Transactions: begin, commit, rollback
- Backup tables and export CSV
- GUI: spreadsheet-style interface with forms and filters
- REST API for integration with web applications

---

# Create a table with schema
db.create_table("users", schema={"id": int, "name": str, "email": str})

# Insert records
db.insert("users", {"id": 1, "name": "Alice", "email": "alice@example.com"})
db.insert("users", {"id": 2, "name": "Bob", "email": "bob@example.com"})

# Query records
db.select("users")
db.select("users", name="Alice")

# Update records
db.update("users", {"id": 2}, {"name": "Robert"})

# Delete records
db.delete("users", id=1)

# Transactions
db.begin_transaction("users")
db.insert("users", {"id":3,"name":"Charlie"})
db.rollback("users")  # undo changes
db.commit("users")    # commit changes

# Export
db.export_csv("users", "users.csv")

# REST API Usage
Launch the API:
python maestrodatabase_api.py

Default credentials:
Username: admin
Password: password123

# ⚠️ Important: Change the default password immediately before using the API. The API is for demo/educational use only and is not secure for production.

# Example Endpoints
Endpoint	            Method	  Description
/create_table	        POST	    Create a new table
/insert/<table_name>	POST	    Insert a record
/select/<table_name>	GET	      Query table records
/update/<table_name>	PUT	      Update records
/delete/<table_name>	DELETE	  Delete records
/tables	              GET	      List all tables

# Best Practices

Always define schemas to validate data types
Use transactions when performing multiple inserts/updates
Backup your tables regularly
Change the default API password immediately
Keep the data/ folder safe — all tables are stored as JSON
Do not use MDB for production or sensitive data
