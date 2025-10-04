# mdb_gui_standalone_fixed.py
import os, json, csv
from datetime import datetime
from copy import deepcopy
import tkinter as tk
from tkinter import ttk, messagebox, filedialog, simpledialog

# ---------------- MDB Core ----------------
class MDB:
    def __init__(self, folder="data", extension=".mdb"):
        self.folder = folder
        self.extension = extension
        os.makedirs(folder, exist_ok=True)
        self.tables = {}
        self.schemas = {}
        self._transactions = {}

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
            if dtype and record[col] is not None and not isinstance(record[col], dtype):
                raise TypeError(f"Column '{col}' must be of type {dtype.__name__}")

    # Table operations
    def create_table(self, table_name, schema=None):
        if table_name in self.tables:
            raise ValueError(f"Table '{table_name}' already exists.")
        self.tables[table_name] = []
        if schema:
            self.schemas[table_name] = schema
        self._save(table_name)

    def load_table(self, table_name):
        path = self._path(table_name)
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                # old format: just list of rows
                self.tables[table_name] = data
                if data:
                    schema = {}
                    for k, v in data[0].items():
                        if isinstance(v, int): schema[k] = int
                        elif isinstance(v, float): schema[k] = float
                        elif isinstance(v, str): schema[k] = str
                        else: schema[k] = None
                    self.schemas[table_name] = schema
                else:
                    self.schemas[table_name] = {}
            elif isinstance(data, dict):
                schema_data = data.get("schema", {})
                schema = {}
                for k, v in schema_data.items():
                    if v == "int": schema[k] = int
                    elif v == "float": schema[k] = float
                    elif v == "str": schema[k] = str
                    else: schema[k] = None
                self.schemas[table_name] = schema
                self.tables[table_name] = data.get("rows", [])
            else:
                raise ValueError("Unsupported .mdb format")
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

    # CRUD
    def insert(self, table_name, record: dict, key_column=None):
        self._check_table_exists(table_name)
        self._validate_record(table_name, record)
        if key_column:
            for existing in self.tables[table_name]:
                if existing.get(key_column) == record.get(key_column):
                    raise ValueError(f"Duplicate entry for '{key_column}' = {record.get(key_column)}")
        self.tables[table_name].append(record)
        self._save(table_name)

    def update(self, table_name, conditions: dict, updates: dict):
        self._check_table_exists(table_name)
        updated_count = 0
        for record in self.tables[table_name]:
            if all(record.get(k) == v for k,v in conditions.items()):
                for uk, uv in updates.items():
                    record[uk] = uv
                updated_count += 1
        self._save(table_name)
        return updated_count

    def delete(self, table_name, **conditions):
        self._check_table_exists(table_name)
        original_len = len(self.tables[table_name])
        self.tables[table_name] = [
            r for r in self.tables[table_name] if not all(r.get(k) == v for k, v in conditions.items())
        ]
        deleted_count = original_len - len(self.tables[table_name])
        self._save(table_name)
        return deleted_count

    # Persistence
    def _save(self, table_name):
        path = self._path(table_name)
        data = {
            "schema": {k: v.__name__ if v else None for k,v in self.schemas.get(table_name, {}).items()},
            "rows": self.tables[table_name]
        }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

    def backup_table(self, table_name):
        self._check_table_exists(table_name)
        backup_path = self._path(table_name) + f".backup.{datetime.now().strftime('%Y%m%d%H%M%S')}"
        with open(backup_path, "w", encoding="utf-8") as f:
            json.dump({
                "schema": {k: v.__name__ if v else None for k,v in self.schemas.get(table_name, {}).items()},
                "rows": self.tables[table_name]
            }, f, indent=2)

    def export_csv(self, table_name, file_path):
        self._check_table_exists(table_name)
        if not self.tables[table_name]:
            return
        with open(file_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=self.tables[table_name][0].keys())
            writer.writeheader()
            writer.writerows(self.tables[table_name])

    # Transactions
    def begin_transaction(self, table_name):
        self._check_table_exists(table_name)
        self._transactions[table_name] = deepcopy(self.tables[table_name])

    def rollback(self, table_name):
        if table_name in self._transactions:
            self.tables[table_name] = self._transactions.pop(table_name)

    def commit(self, table_name):
        if table_name in self._transactions:
            self._transactions.pop(table_name)
            self._save(table_name)

# ---------------- GUI ----------------
class MDBGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("MDB GUI Standalone")
        self.root.geometry("1000x700")
        self.db = MDB()
        self.create_ui()
        self.refresh_table_menu()

    def create_ui(self):
        self.create_top_bar()
        self.create_transaction_bar()
        self.create_filter_bar()
        self.create_table_view()
        self.create_crud_bar()
        self.create_status_bar()

    # ---- Top Bar ----
    def create_top_bar(self):
        frame = tk.Frame(self.root)
        frame.pack(fill=tk.X, padx=10, pady=5)
        tk.Label(frame, text="Table:").pack(side=tk.LEFT)
        self.table_var = tk.StringVar()
        self.table_menu = ttk.Combobox(frame, textvariable=self.table_var, state="readonly")
        self.table_menu.pack(side=tk.LEFT, padx=5)
        self.table_menu.bind("<<ComboboxSelected>>", lambda e: self.refresh_table_view())

        ttk.Button(frame, text="Create Table", command=self.create_table).pack(side=tk.LEFT, padx=5)
        ttk.Button(frame, text="Load Table", command=self.load_table).pack(side=tk.LEFT, padx=5)
        ttk.Button(frame, text="Drop Table", command=self.drop_table).pack(side=tk.LEFT, padx=5)
        ttk.Button(frame, text="Backup Table", command=self.backup_table).pack(side=tk.LEFT, padx=5)
        ttk.Button(frame, text="Export CSV", command=self.export_csv).pack(side=tk.LEFT, padx=5)

    # ---- Transaction Bar ----
    def create_transaction_bar(self):
        frame = tk.Frame(self.root)
        frame.pack(fill=tk.X, padx=10, pady=5)
        ttk.Button(frame, text="Begin Transaction", command=self.begin_transaction).pack(side=tk.LEFT, padx=5)
        ttk.Button(frame, text="Rollback", command=self.rollback).pack(side=tk.LEFT, padx=5)
        ttk.Button(frame, text="Commit", command=self.commit).pack(side=tk.LEFT, padx=5)

    # ---- Filter Bar ----
    def create_filter_bar(self):
        frame = tk.Frame(self.root)
        frame.pack(fill=tk.X, padx=10, pady=5)
        tk.Label(frame, text="Filter Column:").pack(side=tk.LEFT)
        self.filter_col_var = tk.StringVar()
        self.filter_col_entry = ttk.Combobox(frame, textvariable=self.filter_col_var, state="readonly")
        self.filter_col_entry.pack(side=tk.LEFT, padx=5)
        tk.Label(frame, text="Value:").pack(side=tk.LEFT)
        self.filter_val_var = tk.StringVar()
        tk.Entry(frame, textvariable=self.filter_val_var).pack(side=tk.LEFT, padx=5)
        ttk.Button(frame, text="Apply Filter", command=self.apply_filter).pack(side=tk.LEFT, padx=5)
        ttk.Button(frame, text="Clear Filter", command=self.clear_filter).pack(side=tk.LEFT, padx=5)

    # ---- Table View ----
    def create_table_view(self):
        frame = tk.Frame(self.root)
        frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)
        self.tree = ttk.Treeview(frame)
        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        vsb = ttk.Scrollbar(frame, orient="vertical", command=self.tree.yview)
        vsb.pack(side=tk.RIGHT, fill=tk.Y)
        self.tree.configure(yscrollcommand=vsb.set)

    # ---- CRUD Bar ----
    def create_crud_bar(self):
        frame = tk.Frame(self.root)
        frame.pack(fill=tk.X, padx=10, pady=5)
        ttk.Button(frame, text="Insert Row", command=self.insert_record).pack(side=tk.LEFT, padx=5)
        ttk.Button(frame, text="Edit Row", command=self.edit_record).pack(side=tk.LEFT, padx=5)
        ttk.Button(frame, text="Delete Row", command=self.delete_record).pack(side=tk.LEFT, padx=5)

    # ---- Status Bar ----
    def create_status_bar(self):
        self.status_var = tk.StringVar()
        self.status_var.set("Ready")
        tk.Label(self.root, textvariable=self.status_var, relief=tk.SUNKEN, anchor=tk.W).pack(fill=tk.X, side=tk.BOTTOM)

    # ---- Table Management ----
    def refresh_table_menu(self):
        tables = list(self.db.tables.keys())
        self.table_menu['values'] = tables
        if tables:
            self.table_var.set(tables[0])
            self.refresh_table_view()

    def refresh_table_view(self, filtered_rows=None):
        table_name = self.table_var.get()
        if not table_name or table_name not in self.db.tables:
            return
        self.tree.delete(*self.tree.get_children())
        rows = filtered_rows if filtered_rows else self.db.tables[table_name]
        if not rows:
            self.tree['columns'] = []
            return
        cols = list(rows[0].keys())
        self.tree['columns'] = cols
        self.filter_col_entry['values'] = cols
        self.tree.heading("#0", text="", anchor=tk.W)
        for c in cols:
            self.tree.heading(c, text=c)
            self.tree.column(c, width=100)
        for i, row in enumerate(rows):
            values = [row.get(c) for c in cols]
            self.tree.insert("", tk.END, iid=str(i), values=values)

    # ---- Transactions ----
    def begin_transaction(self):
        table_name = self.table_var.get()
        if table_name:
            self.db.begin_transaction(table_name)
            self.status_var.set(f"Transaction started for '{table_name}'")

    def rollback(self):
        table_name = self.table_var.get()
        if table_name:
            self.db.rollback(table_name)
            self.refresh_table_view()
            self.status_var.set(f"Transaction rolled back for '{table_name}'")

    def commit(self):
        table_name = self.table_var.get()
        if table_name:
            self.db.commit(table_name)
            self.status_var.set(f"Transaction committed for '{table_name}'")

    # ---- CRUD ----
    def insert_record(self):
        self.edit_row_dialog("Insert Record")

    def edit_record(self):
        selected = self.tree.selection()
        if not selected:
            messagebox.showwarning("Warning", "No row selected")
            return
        self.edit_row_dialog("Edit Record", int(selected[0]))

    def delete_record(self):
        table_name = self.table_var.get()
        selected = self.tree.selection()
        if not selected:
            messagebox.showwarning("Warning", "No row selected")
            return
        row_idx = int(selected[0])
        try:
            self.db.delete(table_name, **self.db.tables[table_name][row_idx])
            self.refresh_table_view()
            self.status_var.set("Row deleted")
        except Exception as e:
            messagebox.showerror("Error", str(e))

    # ---- Row Dialog ----
    def edit_row_dialog(self, title, row_idx=None):
        table_name = self.table_var.get()
        schema = self.db.schemas.get(table_name)
        if not schema:
            messagebox.showerror("Error", "No schema defined for this table")
            return
        dialog = tk.Toplevel(self.root)
        dialog.title(title)
        entries = {}
        row_data = self.db.tables[table_name][row_idx] if row_idx is not None else {}
        for i, (col, dtype) in enumerate(schema.items()):
            tk.Label(dialog, text=f"{col} ({dtype.__name__ if dtype else 'any'})").grid(row=i, column=0, sticky=tk.W)
            val = row_data.get(col, "")
            entry = tk.Entry(dialog)
            entry.insert(0, val)
            entry.grid(row=i, column=1)
            entries[col] = (entry, dtype)

        def save():
            try:
                record = {}
                for col, (entry, dtype) in entries.items():
                    val = entry.get()
                    if dtype:
                        record[col] = dtype(val) if val else None
                    else:
                        record[col] = val
                if row_idx is None:
                    self.db.insert(table_name, record)
                else:
                    old_row = self.db.tables[table_name][row_idx]
                    self.db.update(table_name, old_row, record)
                dialog.destroy()
                self.refresh_table_view()
                self.status_var.set("Row saved")
            except Exception as e:
                messagebox.showerror("Error", str(e))

        ttk.Button(dialog, text="Save", command=save).grid(row=len(schema), column=0, columnspan=2, pady=5)

    # ---- Filter ----
    def apply_filter(self):
        table_name = self.table_var.get()
        col = self.filter_col_var.get()
        val = self.filter_val_var.get()
        if not table_name or not col:
            return
        filtered = [r for r in self.db.tables[table_name] if str(r.get(col)) == val]
        self.refresh_table_view(filtered_rows=filtered)

    def clear_filter(self):
        self.filter_col_var.set("")
        self.filter_val_var.set("")
        self.refresh_table_view()

    # ---- Table Actions ----
    def create_table(self):
        name = simpledialog.askstring("Table Name", "Enter table name:")
        if not name: return
        schema_str = simpledialog.askstring("Schema", "Enter schema as JSON (e.g., {\"id\": \"int\"})")
        if not schema_str: return
        try:
            schema_dict = json.loads(schema_str)
            schema = {}
            for k,v in schema_dict.items():
                if v.lower()=="int": schema[k]=int
                elif v.lower()=="str": schema[k]=str
                elif v.lower()=="float": schema[k]=float
                else: schema[k]=None
            self.db.create_table(name,schema)
            self.refresh_table_menu()
            self.status_var.set(f"Table '{name}' created")
        except Exception as e:
            messagebox.showerror("Error", str(e))

    def load_table(self):
        # List available .mdb files
        files = [f[:-4] for f in os.listdir(self.db.folder) if f.endswith(self.db.extension)]
        if not files:
            messagebox.showinfo("Info", "No tables found in data folder.")
            return
        name = simpledialog.askstring("Load Table", f"Enter table name:\nAvailable: {', '.join(files)}")
        if not name:
            return
        try:
            self.db.load_table(name)
            self.refresh_table_menu()
            self.status_var.set(f"Table '{name}' loaded")
        except Exception as e:
            messagebox.showerror("Error", str(e))

    def drop_table(self):
        table_name = self.table_var.get()
        if not table_name: return
        try:
            self.db.drop_table(table_name)
            self.refresh_table_menu()
            self.status_var.set(f"Table '{table_name}' dropped")
        except Exception as e:
            messagebox.showerror("Error", str(e))

    def backup_table(self):
        table_name = self.table_var.get()
        if not table_name: return
        try:
            self.db.backup_table(table_name)
            self.status_var.set(f"Backup created for '{table_name}'")
        except Exception as e:
            messagebox.showerror("Error", str(e))

    def export_csv(self):
        table_name = self.table_var.get()
        if not table_name: return
        path = filedialog.asksaveasfilename(defaultextension=".csv")
        if not path: return
        try:
            self.db.export_csv(table_name,path)
            self.status_var.set(f"Table '{table_name}' exported to CSV")
        except Exception as e:
            messagebox.showerror("Error", str(e))

# ---------------- Main ----------------
if __name__ == "__main__":
    root = tk.Tk()
    app = MDBGUI(root)
    root.mainloop()
