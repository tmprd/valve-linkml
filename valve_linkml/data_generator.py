#!/usr/bin/env python3
import os
import csv
from typing import List

from .generate_from_synthea import generate_tables_from_fhir_mapping

def is_enum_table(table_name, enum_primary_key, column_dicts):
    """Determine if a table is an enum table based on its name and column dicts."""
    # Enum tables have a single column with the same name as the table
    return any(c for c in column_dicts if c["table"] == table_name and c["column"] == enum_primary_key)

def generate_schema_data(data_table_dicts: List[dict], data_column_dicts: List[dict], logger):
    """Generate data given some data table and column dicts. Exclude Enum tables. Don't use this with VALVE config metadata."""
    logger.info("Generating data tables...")

    # Map some pre-generated data to our data tables if we have the right kind
    table_names = [t["table"] for t in data_table_dicts]
    pregenerated_table_data:dict = None
    if "Person" in table_names and "Address" in table_names:
        pregenerated_table_data = generate_tables_from_fhir_mapping(logger)

    # Create the data tables themselves
    for table_dict in data_table_dicts:
        # Skip enum tables! Don't overwrite
        if is_enum_table(table_dict["table"], "permissible_value", data_column_dicts):
            logger.info(f"Skipping data generation for enum table {table_dict['table']}")
            continue # Skip enums to avoid overwriting the values

        table_name = table_dict["table"]
        table_path = table_dict["path"]

        # Create the directory if needed
        os.makedirs(os.path.dirname(table_path), exist_ok=True)
        
        # if os.path.exists(table_path): continue
        with open(table_path, 'w') as table_file:
            table_columns = [c for c in data_column_dicts if c["table"] == table_dict["table"]]
            table_column_names = [c["column"] for c in table_columns]
            # Create the table
            writer = csv.DictWriter(table_file, delimiter="\t", fieldnames=table_column_names, lineterminator="\n")
            writer.writeheader()
           
            # Try using pre-generated, mapped data
            if pregenerated_table_data is not None:
                matching_generated_data = pregenerated_table_data.get(table_name) # case-sensitive
                if matching_generated_data is not None:
                    # Write generated data to the table
                    writer.writerows(matching_generated_data)

            
                    
                
            logger.info(f"Wrote data to '{table_path}'")

            # Experimental generation
            # create_generation_prompt(table_name, table_columns)

def create_generation_prompt(table_name: str, table_column_dicts: List[str]):
    """Warning: experimental! This just creates a prompt and doesn't do anything with it."""

    print(f"The {table_name} table has these columns:")
    for c in table_column_dicts:
        column_description = f"Column '{c['column']}' has datatype '{c['datatype']}'. "
        if c["structure"]:
            column_description += f"The value of this column should be an ID in the {from_structure_to_table_name(c['structure'])} table."
        print(column_description)

    # TODO: Attempt referential integrity
    print("\nPlease generate a TSV with 20 rows that have these columns filled in with realistic values. Format the TSV in a code block with actual tabs, not spaces.")
    print("\n")

def from_structure_to_table_name(structure_name: str):
    return structure_name.replace("from(", "").replace(")", "").strip(".id")
