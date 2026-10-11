"""Check that every copy of the Trade schema agrees with configs/schemas/trade_schema.avsc.

Static check only: parses source with `ast`, imports nothing from the project, and needs
no third-party packages. Run from the repository root:

    python3 .claude/skills/change-trade-schema/check_schema_alignment.py

Exit code 0 = aligned, 1 = mismatch found, 2 = a copy could not be located (the code
moved; update this script).
"""

import ast
import json
import sys
from pathlib import Path

ROOT = Path.cwd()
AVSC = ROOT / "configs/schemas/trade_schema.avsc"
PRODUCER = ROOT / "producers/src/trades.py"
CONSUMER = ROOT / "consumers/src/trades.py"
SPARK_UTILS = ROOT / "spark/jobs/utils.py"

# Avro primitive -> expected pyarrow constructor name in ARROW_SCHEMA
AVRO_TO_ARROW = {
    "string": "string",
    "double": "float64",
    "float": "float32",
    "long": "int64",
    "int": "int32",
    "boolean": "bool_",
    "bytes": "binary",
}

# Columns Spark adds from the Hive path, not present in Avro
SPARK_PARTITION_COLUMNS = {"date"}


def find_func(tree: ast.AST, name: str) -> ast.FunctionDef:
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return node
    raise LookupError(name)


def avro_fields() -> dict[str, str]:
    schema = json.loads(AVSC.read_text())
    fields = {}
    for f in schema["fields"]:
        t = f["type"]
        if isinstance(t, list):  # union, e.g. ["null", "string"]
            t = next((x for x in t if x != "null"), "null")
        fields[f["name"]] = t if isinstance(t, str) else json.dumps(t)
    return fields


def producer_keys() -> list[str]:
    func = find_func(ast.parse(PRODUCER.read_text()), "generate")
    for node in ast.walk(func):
        if isinstance(node, ast.Return) and isinstance(node.value, ast.Dict):
            return [k.value for k in node.value.keys if isinstance(k, ast.Constant)]
    raise LookupError("generate() return dict")


def arrow_fields() -> dict[str, str]:
    tree = ast.parse(CONSUMER.read_text())
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Assign)
            and any(isinstance(t, ast.Name) and t.id == "ARROW_SCHEMA" for t in node.targets)
        ):
            call = node.value  # pa.schema([...])
            fields = {}
            for elt in call.args[0].elts:
                name = elt.elts[0].value
                type_call = elt.elts[1]
                fields[name] = type_call.func.attr
            return fields
    raise LookupError("ARROW_SCHEMA")


def list_assigned_in(func_name: str, var: str) -> list[str]:
    func = find_func(ast.parse(SPARK_UTILS.read_text()), func_name)
    for node in ast.walk(func):
        if (
            isinstance(node, ast.Assign)
            and any(isinstance(t, ast.Name) and t.id == var for t in node.targets)
            and isinstance(node.value, ast.List)
        ):
            return [e.value for e in node.value.elts]
    raise LookupError(f"{func_name}.{var}")


def main() -> int:
    try:
        avro = avro_fields()
        prod = producer_keys()
        arrow = arrow_fields()
        spark_cols = list_assigned_in("write_to_iceberg", "columns")
        required = list_assigned_in("validate_dataframe", "required_columns")
    except (LookupError, AttributeError, IndexError) as e:
        print(f"ERROR: could not locate a schema copy ({e}). Update this script.")
        return 2

    problems = []
    avro_names = list(avro)

    if set(prod) != set(avro_names):
        problems.append(f"producer generate() keys {sorted(prod)} != Avro fields {sorted(avro_names)}")

    if list(arrow) != avro_names:
        problems.append(f"consumer ARROW_SCHEMA order/names {list(arrow)} != Avro {avro_names}")
    for name, avro_type in avro.items():
        expected = AVRO_TO_ARROW.get(avro_type)
        actual = arrow.get(name)
        if actual is not None and expected != actual:
            problems.append(f"type mismatch for '{name}': Avro {avro_type} -> expected pa.{expected}(), found pa.{actual}()")

    spark_data_cols = [c for c in spark_cols if c not in SPARK_PARTITION_COLUMNS]
    if spark_data_cols != avro_names:
        problems.append(f"spark write_to_iceberg columns {spark_data_cols} (+partition cols) != Avro {avro_names}")

    missing_required = [c for c in required if c not in avro_names]
    if missing_required:
        problems.append(f"validate_dataframe required_columns not in Avro: {missing_required}")

    print("Avro fields:            ", avro_names)
    print("Producer generate():    ", prod)
    print("Consumer ARROW_SCHEMA:  ", list(arrow))
    print("Spark write columns:    ", spark_cols)
    print("Spark required columns: ", required)
    if problems:
        print("\nMISMATCHES:")
        for p in problems:
            print("  -", p)
        return 1
    print("\nOK: all schema copies aligned.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
