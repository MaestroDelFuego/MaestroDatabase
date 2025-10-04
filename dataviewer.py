# mdb_web_viewer.py
import os
import json
from flask import Flask, render_template, request

DATA_FOLDER = "data"
EXTENSION = ".mdb"

app = Flask(__name__)

def list_tables():
    return [f.replace(EXTENSION, "") for f in os.listdir(DATA_FOLDER) if f.endswith(EXTENSION)]

def load_table(table_name):
    path = os.path.join(DATA_FOLDER, table_name + EXTENSION)
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

@app.route("/")
def index():
    tables = list_tables()
    return render_template("index.html", tables=tables, rows=None, selected_table=None)

@app.route("/view", methods=["POST"])
def view_table_route():
    table_name = request.form.get("table_name")
    tables = list_tables()
    rows = load_table(table_name)
    return render_template("index.html", tables=tables, rows=rows, selected_table=table_name)

if __name__ == "__main__":
    os.makedirs(DATA_FOLDER, exist_ok=True)
    app.run(debug=True)
