# mdb_api.py
from flask import Flask, request, jsonify
from maestrodatabase_terminal import MDB

app = Flask(__name__)

# Simple in-memory user system (for demo/educational use) - do not use in production
USERS = {
    "admin": "password123",  # username: password
}

# Database instance
db = MDB()

# -----------------------
# Authentication decorator
# -----------------------
def require_auth(func):
    def wrapper(*args, **kwargs):
        auth = request.authorization
        if not auth or USERS.get(auth.username) != auth.password:
            return jsonify({"error": "Unauthorized"}), 401
        return func(*args, **kwargs)
    wrapper.__name__ = func.__name__
    return wrapper


# -----------------------
# Routes
# -----------------------

@app.route("/create_table", methods=["POST"])
@require_auth
def create_table():
    data = request.json
    name = data.get("table_name")
    schema = data.get("schema", None)

    # Convert string type names to real Python types
    type_map = {"int": int, "str": str, "float": float, "bool": bool}
    if schema:
        schema = {
            k: type_map.get(v, None) if isinstance(v, str) else v
            for k, v in schema.items()
        }

    try:
        db.create_table(name, schema)
        return jsonify({"message": f"Table '{name}' created."}), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route("/insert/<table_name>", methods=["POST"])
@require_auth
def insert_record(table_name):
    record = request.json
    try:
        db.insert(table_name, record)
        return jsonify({"message": "Record inserted successfully."}), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route("/select/<table_name>", methods=["GET"])
@require_auth
def select_records(table_name):
    try:
        filters = request.args.to_dict()
        result = db.select(table_name, **filters)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route("/update/<table_name>", methods=["PUT"])
@require_auth
def update_records(table_name):
    data = request.json
    conditions = data.get("conditions", {})
    updates = data.get("updates", {})
    try:
        db.update(table_name, conditions, updates)
        return jsonify({"message": "Records updated."})
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route("/delete/<table_name>", methods=["DELETE"])
@require_auth
def delete_records(table_name):
    conditions = request.json or {}
    try:
        db.delete(table_name, **conditions)
        return jsonify({"message": "Records deleted."})
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route("/tables", methods=["GET"])
@require_auth
def list_tables():
    return jsonify({"tables": list(db.tables.keys())})


if __name__ == "__main__":
    app.run(debug=True)
