import os
import json
from flask import Flask, render_template

DATA_FOLDER = "data"   # Your MDB folder
EXTENSION = ".mdb"

app = Flask(__name__)

# -----------------------------
# Utility functions
# -----------------------------
def list_tables():
    return [f.replace(EXTENSION, "") for f in os.listdir(DATA_FOLDER) if f.endswith(EXTENSION)]

def load_table(table_name):
    path = os.path.join(DATA_FOLDER, table_name + EXTENSION)
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

# -----------------------------
# Routes
# -----------------------------
@app.route("/")
def index():
    tables = list_tables()
    data = {}
    for table in tables:
        data[table] = load_table(table)
    return render_template("example.html", tables=tables, data=data)

# -----------------------------
# Run the app
# -----------------------------
if __name__ == "__main__":
    os.makedirs(DATA_FOLDER, exist_ok=True)
    app.run(debug=True, port=3000)  # Use port 3000 to avoid conflicts
