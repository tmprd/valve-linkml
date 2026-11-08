# Hello

import os
import csv
from typing import List, Callable

RLTBL_SCHEMA = {
    "tables": {
        "table": {
            "headers": ["table", "path", "description", "type"],
        },
        "column": {
            "headers": ["table", "column", "nulltype", "datatype", "structure", "description"],
        },
        "datatype": {
            "headers": [
                "datatype",
                "parent",
                "transform",
                "condition",
                "structure",
                "description",
                "SQLite type",
                "PostgreSQL type",
                "RDF type",
                "HTML type",
            ],
        },
    },
    "defaults": {"datatype": "text", "primary_key": "id"},
}


def table_row(table_name: str, table_description: str, table_dir: str):
    formatted_table_name = format_table_name(table_name)
    return {
        "table": formatted_table_name,
        "path": f"{table_dir}/{formatted_table_name}.tsv",
        "description": table_description.strip() if table_description else None,
        "type": None,
    }


def column_row(
    column_name: str,
    table_name: str,
    description: str,
    datatype: str,
    structure: str,
    is_required: bool,
):
    formatted_table_name = format_table_name(table_name)
    return {
        "table": formatted_table_name,
        "column": column_name,
        "nulltype": "empty" if not is_required else None,
        "datatype": datatype,
        "structure": structure,
        "description": description,
    }


def datatype_row(datatype_name: str, datatype_description, regex: str):
    # TODO Map slot minimum_value & maximum_value to ...?
    return {
        "datatype": datatype_name,
        "parent": RLTBL_SCHEMA["defaults"]["datatype"],
        "transform": None,
        "condition": (f"match(/{regex}/)" if regex else None),
        "structure": None,
        "description": datatype_description
        or f"a {datatype_name}",  # default to name if no description
        "SQLite type": None,  # TODO
        "PostgreSQL type": None,  # TODO
        "RDF type": None,  # TODO
        "HTML type": None,  # TODO
    }


def prepend_rltbl_tables(schema_tables: dict, output_dir: str, logger):
    schema_tables["table"]["rows"] = (
        init_rltbl_table(
            "test/rltbl_sample_schema/table.tsv",
            RLTBL_SCHEMA["tables"],
            lambda row: map_table_path(row, output_dir),
        )
        + schema_tables["table"]["rows"]
    )
    schema_tables["column"]["rows"] = (
        init_rltbl_table("test/rltbl_sample_schema/column.tsv", RLTBL_SCHEMA["tables"])
        + schema_tables["column"]["rows"]
    )

    all_datatypes = init_rltbl_table("test/rltbl_sample_schema/datatype.tsv", None)
    # Add new mapped datatypes only if there's no duplicately named RLTBL datatype.
    # Choose the RLTBL datatype over the mapped one.
    for d in schema_tables["datatype"]["rows"]:
        if d["datatype"] not in [v["datatype"] for v in all_datatypes]:
            all_datatypes.append(d)
        else:
            logger.warning(f"RLTBL datatype {d['datatype']} already exists. Skipping.")
    schema_tables["datatype"]["rows"] = all_datatypes
    return schema_tables


def init_rltbl_table(
    rltbl_tsv_path: str, column_filter: dict, map_rltbl_row: Callable = lambda row: row
):
    # TODO: Use RLTBL lib to get metadata tables instead of a sample schema
    with open(rltbl_tsv_path, "r") as table_file:
        reader = csv.DictReader(table_file, delimiter="\t")
        return [
            map_rltbl_row(row)
            for row in reader
            if (not column_filter) or (row.get("table") in column_filter)
        ]


def map_table_path(row: dict, output_dir: str):
    """Returns copy of row with "path" modified"""
    return dict(row, path=os.path.join(output_dir, row.get("table") + ".tsv"))


def format_table_name(table_name):
    return table_name.replace(" ", "_")


def primary_structure():
    return "primary"


def is_from_structure(structure: str):
    return structure.startswith("from(")


def from_structure(table_name: str, column_name: str):
    formatted_table_name = format_table_name(table_name)
    return f"from({formatted_table_name}.{column_name})"


def from_structure2table_column(from_structure: str):
    return from_structure.replace("from(", "").replace(")", "").split(".")


def format_enum_datatype_condition(enum_values: List[str]):
    return f"in({format_enum_str(enum_values)})"


def format_enum_str(enum_values: List[str]):
    return ",".join([f"'{v}'" for v in enum_values])
