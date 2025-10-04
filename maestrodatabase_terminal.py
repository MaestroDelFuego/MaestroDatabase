# mdb_database.py
import os
import json
import csv
from datetime import datetime
from copy import deepcopy

class MDB:
    def __init__(self, folder="data", extension=".mdb"):
        self.folder = folder
        self.extension = extension
        os.makedirs(folder, exist_ok=True)
        self.tables = {}
        self.schemas = {}  # store table schemas
        self._transactions = {}  # for rollback support

    # -------------------
    # Utility functions
    # -------------------
    def _path(self, table_name):
        return os.path.join(self.folder, table_name + self.extension)

    def _check_table_exists(self, table_name):
        if table_name not in self.tables:
            raise ValueError(f"Table '{table_name}' does not exist.")

    def _validate_record(self, table_name, record):
        if table_name not in self.schemas:
            return
        schema = self.schemas[table_name]
        for col, dtype in schema.items():
            if col not in record:
                raise ValueError(f"Missing column '{col}' in record.")
            if dtype and not isinstance(record[col], dtype):
                # allow None
                if record[col] is not None:
                    raise TypeError(f"Column '{col}' must be of type {dtype.__name__}")

    # -------------------
    # Table operations
    # -------------------
    def create_table(self, table_name, schema=None):
        if table_name in self.tables:
            raise ValueError(f"Table '{table_name}' already exists.")
        self.tables[table_name] = []
        if schema:
            self.schemas[table_name] = schema
        self._save(table_name)
        print(f"Table '{table_name}' created with schema: {schema}")

    def load_table(self, table_name):
        path = self._path(table_name)
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                self.tables[table_name] = json.load(f)
            print(f"Table '{table_name}' loaded from disk.")
        else:
            raise FileNotFoundError(f"No saved table '{table_name}' found.")

    def drop_table(self, table_name):
        self._check_table_exists(table_name)
        del self.tables[table_name]
        if table_name in self.schemas:
            del self.schemas[table_name]
        path = self._path(table_name)
        if os.path.exists(path):
            os.remove(path)
        print(f"Table '{table_name}' dropped.")

    # -------------------
    # CRUD operations
    # -------------------
    def insert(self, table_name, record: dict, key_column=None):
        self._check_table_exists(table_name)
        self._validate_record(table_name, record)
    
        # Check for duplicates if key_column is provided
        if key_column:
            for existing in self.tables[table_name]:
                if existing.get(key_column) == record.get(key_column):
                    raise ValueError(f"Duplicate entry for '{key_column}' = {record.get(key_column)}")

        self.tables[table_name].append(record)
        self._save(table_name)
        print(f"Inserted into '{table_name}': {record}")


    def select(self, table_name, **conditions):
        self._check_table_exists(table_name)
        results = []
        for record in self.tables[table_name]:
            if all(record.get(k) == v for k, v in conditions.items()):
                results.append(record)
        return results

    def update(self, table_name, conditions: dict, updates: dict):
        self._check_table_exists(table_name)
        updated_count = 0
        for record in self.tables[table_name]:
            if all(record.get(k) == v for k, v in conditions.items()):
                for uk, uv in updates.items():
                    record[uk] = uv
                updated_count += 1
        self._save(table_name)
        print(f"Updated {updated_count} records in '{table_name}'.")

    def delete(self, table_name, **conditions):
        self._check_table_exists(table_name)
        original_len = len(self.tables[table_name])
        self.tables[table_name] = [
            r for r in self.tables[table_name]
            if not all(r.get(k) == v for k, v in conditions.items())
        ]
        deleted_count = original_len - len(self.tables[table_name])
        self._save(table_name)
        print(f"Deleted {deleted_count} records from '{table_name}'.")

    # -------------------
    # Persistence
    # -------------------
    def _save(self, table_name):
        path = self._path(table_name)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(self.tables[table_name], f, indent=2)

    def backup_table(self, table_name):
        self._check_table_exists(table_name)
        backup_path = self._path(table_name) + f".backup.{datetime.now().strftime('%Y%m%d%H%M%S')}"
        with open(backup_path, "w", encoding="utf-8") as f:
            json.dump(self.tables[table_name], f, indent=2)
        print(f"Backup of '{table_name}' created at {backup_path}")

    def export_csv(self, table_name, file_path):
        self._check_table_exists(table_name)
        if not self.tables[table_name]:
            print("No data to export.")
            return
        with open(file_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=self.tables[table_name][0].keys())
            writer.writeheader()
            writer.writerows(self.tables[table_name])
        print(f"Table '{table_name}' exported to CSV: {file_path}")

    # -------------------
    # Transactions
    # -------------------
    def begin_transaction(self, table_name):
        self._check_table_exists(table_name)
        self._transactions[table_name] = deepcopy(self.tables[table_name])
        print(f"Transaction started for '{table_name}'.")

    def rollback(self, table_name):
        if table_name in self._transactions:
            self.tables[table_name] = self._transactions.pop(table_name)
            print(f"Transaction rolled back for '{table_name}'.")
        else:
            print(f"No active transaction for '{table_name}'.")

    def commit(self, table_name):
        if table_name in self._transactions:
            self._transactions.pop(table_name)
            self._save(table_name)
            print(f"Transaction committed for '{table_name}'.")
        else:
            print(f"No active transaction for '{table_name}'.")

# -------------------
# Interactive Demo
# -------------------
def interactive_demo():
    try:
        import pyreadline as readline  # optional, ignore if not installed
    except ImportError:
        pass

    print("Welcome to MDB Ultimate!")
    print("Type Python commands using 'db'. Example:")
    print("db.create_table('users', schema={'id': int, 'name': str})")
    print("db.insert('users', {'id':1,'name':'Alice'})")
    print("db.select('users', name='Alice')")
    print("Type 'exit' to quit.\n")

    db = MDB()
    # Use a single globals dictionary with builtins and db
    globals_dict = {"__builtins__": __builtins__, "db": db}

    while True:
        try:
            cmd = input("mdb> ")
            if cmd.lower() in ("exit", "quit"):
                print("Goodbye!")
                break
            # Use exec instead of eval so multiple statements work
            exec(cmd, globals_dict)
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    interactive_demo()
