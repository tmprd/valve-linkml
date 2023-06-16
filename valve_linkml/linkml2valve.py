#!/usr/bin/env python3
import os
import csv
import logging
from typing import List, Optional, Callable
from argparse import ArgumentParser

from linkml_runtime.utils.schemaview import SchemaView, SlotDefinition, ClassDefinition, ClassDefinitionName, EnumDefinition
import linkml.utils.converter

from .utils import write_dicts2tsv
from .data_generator import generate_schema_data

"""Usage: python3 -m valve_linkml.linkml2valve <linkml-yaml-schema-path> -d <linkml-yaml-data-directory>"""


LOGGER = logging.getLogger("linkml2valve")

VALVE_SCHEMA = {
    "table": {
        "headers": ["table", "path", "description", "type"],
    },
    "column": {
        "headers": ["table", "column", "nulltype", "datatype", "structure", "description"],
    },
    "datatype": {
        "headers": ["datatype", "parent", "transform", "condition", "structure", "description", "SQLite type", "PostgreSQL type", "RDF type", "HTML type"],
    }
}

DEFAULT_DATATYPE = 'text'
DEFAULT_PARENT_DATATYPE = DEFAULT_DATATYPE

DEFAULT_PRIMARY_KEY = 'id'
DEFAULT_PRIMARY_KEY_DATATYPE = DEFAULT_DATATYPE

# TODO change this to local var used in datatype mapping
SCHEMA_DEFAULT_RANGE = DEFAULT_DATATYPE

ENUM_PRIMARY_KEY = "permissible_value"
ENUM_PRIMARY_KEY_DATATYPE = DEFAULT_DATATYPE
ENUM_MEANING_DATATYPE = "CURIE"

def main():
    # CLI
    parser = ArgumentParser()
    parser.add_argument('yaml_schema_path', type=str, help="Path to LinkML YAML schema file")
    parser.add_argument("-o", "--output-dir", required=True, help="Output directory for VALVE tables")
    parser.add_argument("-d", "--data-dir", help="Directory of LinkML YAML data files. These are NOT schemas!")
    parser.add_argument("-g", "--generate-data", help="Boolean option to generate data files from the schema.")
    parser.add_argument("-v", "--verbose", help="Boolean option log verbosely.")
    args = parser.parse_args()

    # Run
    linkml2valve(args.yaml_schema_path, args.output_dir, args.data_dir, args.generate_data, args.verbose)

def linkml2valve(yaml_schema_path: str, output_dir: str, data_dir: str = None, generate_data: bool = False, log_verbosely: bool = False):
    global LOGGER
    if log_verbosely: LOGGER.setLevel(level=logging.DEBUG)

    # Map LinkML schema to VALVE tables
    schema_tables = map_schema(yaml_schema_path, output_dir)

    # Create the actual data table files with some generated data (exclude VALVE metadata rows and Enum tables)
    if generate_data:
        generate_schema_data(schema_tables["table"]["rows"], schema_tables["column"]["rows"], LOGGER)

    # Prepend VALVE metadata rows to mapped tables
    all_tables = init_valve_table("test/valve_sample_schema/table.tsv", VALVE_SCHEMA, lambda row: map_table_path(row, output_dir)) + schema_tables["table"]["rows"]
    all_columns = init_valve_table("test/valve_sample_schema/column.tsv", VALVE_SCHEMA) + schema_tables["column"]["rows"]
    all_datatypes = init_valve_table("test/valve_sample_schema/datatype.tsv", None)

    # Add mapped LinkML datatypes only if there's no duplicately named VALVE datatype. Choose the VALVE datatype over the LinkML one.
    for d in schema_tables["datatype"]["rows"]:
        if d["datatype"] not in [v["datatype"] for v in all_datatypes]:
            all_datatypes.append(d)
        else:
            LOGGER.warning(f"VALVE datatype {d['datatype']} already exists. Skipping.")
    
    schema_tables["table"]["rows"] = all_tables
    schema_tables["column"]["rows"] = all_columns
    schema_tables["datatype"]["rows"] = all_datatypes

    # Serialize the combined VALVE schema and mapped LinkML schema to TSVs
    for table_name in schema_tables:
        table_dict = schema_tables[table_name]
        table_path = table_dict["path"]
        LOGGER.debug(f"Wrote {len(table_dict['rows'])} rows to '{table_path}'")
        write_dicts2tsv(table_path, table_dict["rows"], VALVE_SCHEMA[table_name]["headers"])

        # TODO add Enum to data tables here
        if table_name == "table":
            for table_row in table_dict["rows"]:
                if table_row["type"] == "enum":
                    # LOGGER.info(f"Adding Enum data to table '{table_row['table']}'")
                    enum_table_path = table_row["path"]
                    enum_table_headers = [c for c in schema_tables["column"]["rows"] if c["table"] == table_row["table"]]


    # Map LinkML yaml data and serialize to VALVE data TSVs
    if data_dir is not None:
        map_data(yaml_schema_path, data_dir)


def table_row(class_name: str, class_description: str, table_dir: str):
    return {
        "table": class_name,
        "path": f'{table_dir}/{class_name}.tsv',
        "description": class_description,
        "type": None,
    }

def column_row(column_name: str, table_name: str, description: str, datatype: str, structure: str, is_required: bool):
    return {
        "table": table_name,
        "column": column_name,
        "nulltype": 'empty' if not is_required else None,
        "datatype": datatype,
        "structure": structure,
        "description": description,
    }

def datatype_row(slot_name: str, slot_description, slot_pattern: str):
    # TODO Map slot minimum_value & maximum_value to ...?
    return {
        "datatype": slot_name,
        "parent": DEFAULT_PARENT_DATATYPE,
        "transform": None,
        # A slot's pattern maps to a Datatype "condition"
        "condition": (f"match(/{slot_pattern}/)" if slot_pattern else None),
        "structure": None, 
        "description": slot_description or f"a {slot_name}", # default to name if no description
        "SQLite type": None, # TODO
        "PostgreSQL type": None, # TODO
        "RDF type": None, # TODO
        "HTML type": None, # TODO
    }

def map_schema(yaml_schema_path: str, output_dir: str) -> dict[str, dict[str, str]]:
    # Data tables go in a subdirectory of the schema directory by default
    data_table_dir = os.path.join(output_dir, "data")

    # Parse schema
    linkml_schema = SchemaView(yaml_schema_path)
    all_classes = linkml_schema.all_classes().values()
    all_slots = linkml_schema.all_slots().values()
    all_enums = linkml_schema.all_enums().values()
    all_types = linkml_schema.all_types().values()
    SCHEMA_DEFAULT_RANGE = linkml_schema.schema.default_range
    
    class_count = len(all_classes)
    slot_count = len(all_slots)
    enum_count = len(all_enums)
    LOGGER.debug(f"{class_count} classes, {slot_count} slots, {enum_count} enums parsed from '{yaml_schema_path}'")
    validate_schema(all_classes, all_slots, all_enums)

    table_rows: list[dict] = []
    all_column_rows: list[dict] = []
    datatype_rows: list[dict] = []

    # Map all types to Datatype table rows
    for type in all_types:
        datatype_rows.append(datatype_row(type.name, None, None))
    
    # Map classes to Table table rows
    for linkml_class in all_classes:
        table_rows.append(table_row(linkml_class.name, linkml_class.description, data_table_dir))

        class_has_primary_key = False
        
        # Map slots of (transitively) inherited classes to additional rows for this class in the Column table. (Note: these include slots/attributes inherited from "mixins".)
        inherited_slots = get_inherited_class_slots(linkml_schema, linkml_class)
        for inherited_slot in inherited_slots:
            new_column_row = map_class_slot(linkml_schema, inherited_slot, linkml_class, all_classes, all_enums)
            class_has_primary_key = class_has_primary_key or (new_column_row["structure"] == "primary") if new_column_row else class_has_primary_key
            # Add new column row
            all_column_rows = (all_column_rows + [new_column_row]) if new_column_row else all_column_rows

        # # Map class slots and attributes to Column table rows
        slot_names = list(linkml_class.slots) + list(linkml_class.attributes)
        for slot_name in slot_names:
            # Map slot to Column table row
            slot = next((s for s in all_slots if s.name == slot_name), None) # TODO check if specified class slot isn't in the list of all slots ...
            new_column_row = map_class_slot(linkml_schema, slot, linkml_class, all_classes, all_enums)
            class_has_primary_key = class_has_primary_key or (new_column_row["structure"] == "primary") if new_column_row else class_has_primary_key
            # Map slot usage to Datatype table row if its range isn't a class or enum. This should be the dataype of the Column row.
            slot_usage = linkml_class.slot_usage.get(slot_name)
            if slot_usage:
                new_datatype_row = slot_usage2datatype_row(slot_usage, linkml_class, all_classes, all_enums)
                # Update datatype of new column row
                new_column_row["datatype"] = new_datatype_row["datatype"]
                # Add new datatype row
                datatype_rows = (datatype_rows + [new_datatype_row]) if new_datatype_row else datatype_rows

            # Add new column row
            all_column_rows = (all_column_rows + [new_column_row]) if new_column_row else all_column_rows


        # If no identifier slot was found and used as a primary key, we need to make one for this class and add it to the Column table
        if not class_has_primary_key:
            LOGGER.info(f"Class '{linkml_class.name}' has no identifier slot. Creating primary key '{DEFAULT_PRIMARY_KEY}' and adding it to the Column table.")
            # Add new primary key as the first column
            mapped_table_column_rows = [c for c in all_column_rows if c["table"] == linkml_class.name]
            new_primary_key_column = column_row(DEFAULT_PRIMARY_KEY, linkml_class.name, "generated column", 
                                                SCHEMA_DEFAULT_RANGE, "primary", is_required=True)
            table_columns_index = all_column_rows.index(mapped_table_column_rows[0]) if len(mapped_table_column_rows) > 0 else len(all_column_rows)
            all_column_rows.insert(table_columns_index, new_primary_key_column)
            
    # Map multivalued slots to new columns in the class table of the multivalued slot's range. This has to be done after adding primary keys for missing class identifiers.
    for slot in all_slots:
        if not slot.multivalued: continue
        slot_class = next((c for c in all_classes if slot.name in c.slots), None)
        if slot_class is None: continue
        map_multivalued_slot(slot, slot_class, all_column_rows)

    # Map Enums
    for enum in all_enums:
        # Map enum to Table table, with a Permissible Value column and IRI/meaning column
        enum_table = table_row(enum.name, enum.description, output_dir)
        table_rows.append(enum_table)
        # Use the value as the primary key so it can serve as a foreign key
        all_column_rows.append(column_row(ENUM_PRIMARY_KEY, enum.name, "Permissible Value", ENUM_PRIMARY_KEY_DATATYPE, "primary", is_required=True))
        all_column_rows.append(column_row("meaning", enum.name, "CURIE meaning", ENUM_MEANING_DATATYPE, None, is_required=False))
        # Map enum values to rows in the enum table
        permissible_values = enum.permissible_values
        enum_row_dicts = [ {ENUM_PRIMARY_KEY: v, "meaning": permissible_values[v].meaning} for v in permissible_values ]
        
        write_dicts2tsv(enum_table["path"], enum_row_dicts, [ENUM_PRIMARY_KEY, "meaning"])
        LOGGER.debug(f"Wrote {len(enum_row_dicts)} rows to '{enum_table['path']}'")
    

    # Check invariants
    assert len(table_rows) >= class_count, f"{class_count} classes mapped to {len(table_rows)} tables. Expected at least as many tables as classes."
    assert len(all_column_rows) >= slot_count, f"{slot_count} slots mapped to {len(all_column_rows)} columns. Expected at least as many columns as slots."

    return {
        "table": {"rows":table_rows, "path": output_dir + '/table.tsv'},
        "column": {"rows": all_column_rows, "path": output_dir + '/column.tsv'}, 
        "datatype": {"rows": datatype_rows, "path": output_dir + '/datatype.tsv'}
    }

# Map the range of a class-specific slot_usage to a new Datatype row. Example: "primary_email" in Person uses a "person_primary_email" datatype.
# If slot_usage has a class as its range, then map that to a from() foreign key "structure" in the Column table.
# TODO: If slot_usage has an enum as its range, then map that to a from() foreign key "structure" in the Column table.
def slot_usage2datatype_row(slot_usage: SlotDefinition, slot_class: ClassDefinition, all_classes: List[ClassDefinition], all_enums: List[EnumDefinition]):
    # A class-specific slot_usage maps to a Datatype row only if its range is not a class or enum
    slot_usage_class = next((c for c in all_classes if c.name == slot_usage.range), None)
    slot_usage_enum = next((e for e in all_enums if e.name == slot_usage.range), None)
    if slot_usage_class is not None or slot_usage_enum is not None:
        # TODO get datatype of identifier of range class of slot usage
        # slot_usage_datatype = ...
        return None
    else:
        # Create a new Datatype for this slot usage. Then set that as the "datatype" in the Column table.
        slot_usage_datatype = f"{slot_class.name.lower()}_{slot_usage.name}"
        return datatype_row(slot_usage_datatype, None, slot_usage.pattern)


def map_class_slot(schemaView: SchemaView, slot: SlotDefinition, slot_class: ClassDefinition, all_classes: List[ClassDefinition], all_enums: List[EnumDefinition]):
    # If the slot is multivalued, don't add it as a column for this table.
    # Instead, this will be mapped to another table after all class slots have been mapped and primary keys are generated.
    if slot.multivalued:
        LOGGER.info(f"Skipping multivalued slot '{slot.name}' in class '{slot_class.name}' for now.")
        return None
    
    column_datatype = DEFAULT_DATATYPE
    column_structure = None

    # Map identifier slot to "primary" structure of the Column
    if slot.identifier or slot.key:
        column_datatype = DEFAULT_PRIMARY_KEY_DATATYPE
        column_structure = "primary"
    
    # Map slot range to the Column's datatype and structure
    elif slot.range is not None:
        range_class = next((c for c in all_classes if c.name == slot.range), None)
        if range_class is None:
            range_enum = next((e for e in all_enums if e.name == slot.range), None)
            if range_enum:
                # Map range enum value to the datatype of the Column
                column_datatype = ENUM_PRIMARY_KEY_DATATYPE
                column_structure = from_structure(range_enum.name, ENUM_PRIMARY_KEY)
            else:
                # Map range type value to the datatype of the Column
                column_datatype =  slot.range
        else:
            # Map range class value to the structure of the Column => from(<range_class>.<range_class_identifier>)
            range_class_identifier = get_identifier_or_key_slot(schemaView, range_class.name)
            if range_class_identifier is not None:
                # Map range class identifier type to the datatype of the Column
                column_datatype = range_class_identifier.range or SCHEMA_DEFAULT_RANGE
                column_structure = from_structure(range_class.name, range_class_identifier.name)
                # TODO: Can identifier slots have slot_usages?
            else:
                LOGGER.info(f"Slot '{slot.name}' has range '{range_class.name}', but '{range_class.name}' has no identifier. Using {range_class.name}.{DEFAULT_PRIMARY_KEY} as the foreign key.")
                column_datatype = DEFAULT_PRIMARY_KEY_DATATYPE
                column_structure = from_structure(range_class.name, DEFAULT_PRIMARY_KEY)

    # Map slot to Column
    return column_row(slot.name, slot_class.name, slot.description, column_datatype, column_structure, slot.required)



def map_multivalued_slot(slot: SlotDefinition, slot_class: ClassDefinition, column_rows: List[dict]):
    """Add a new column to the range class of this slot, where the "structure" is from(<range_class>.<identifier>)"""
    # Ex. Person
    #       - has_medical_history:
    #           multivalued: true
    #           range: MedicalEvent
    #  => MedicalEvent
    #       - person:
    #           range: Person

    new_column_table_name = slot.range # ex. MedicalEvent
    new_column_name = slot_class.name.lower() # ex. person   

    # Get the primary key of the table that serves as the range of this slot
    slot_class_table_rows = [c for c in column_rows if c["table"] == slot_class.name]
    slot_range_class_primary_key_column = next((c for c in slot_class_table_rows if c["structure"] == "primary"), None)
    if slot_range_class_primary_key_column is None:
        raise Exception(f"Cannot map multivalued slot '{slot.name}' with range '{slot.range}' in class '{slot_class.name}'. No primary key found for range class '{slot.range}' but every class table should have a primary key.")

    new_column_datatype = slot_range_class_primary_key_column["datatype"] # ex. string
    new_column_structure = from_structure(slot_class.name, slot_range_class_primary_key_column["column"]) # ex. from(Person.id)
    
    LOGGER.info(f"Mapping multivalued slot '{slot.name}' with range '{slot.range}' in class '{slot_class.name}' => to new column '{new_column_name}' in '{new_column_table_name}'")
    
    column_rows.append(column_row(new_column_name, new_column_table_name, f"generated column from multivalued slot {slot_class.name}.{slot.name}", 
                                        new_column_datatype, new_column_structure, slot.required))

def map_data(yaml_schema_path: str, yaml_data_dir: str):
    raise NotImplementedError("LinkML data mapping not implemented yet")


def get_inherited_class_slots(schemaView: SchemaView, linkml_class: ClassDefinition) -> List[SlotDefinition]:
    """Get only slots of this class that are inherited from some ancestor class, so we can track these separately from the non-inherited slots/attributes"""
    return [slot for slot in schemaView.class_induced_slots(linkml_class.name) if (slot.name not in linkml_class.slots) and (slot.name not in linkml_class.attributes)]

# Copied from https://github.com/linkml/linkml/blob/c933c7c0c82e3eaa48d815f9cae033360626438e/linkml/generators/typescriptgen.py#L140
def get_identifier_or_key_slot(sv: SchemaView, cn: ClassDefinitionName) -> Optional[SlotDefinition]:
    """Get class's identifier slot, or a slot that's an identifier from some (transitively) inherited class"""
    # TODO: Get other imported identifiers from, e.g. Address's class_uri: schema:PostalAddress
    id_slot = sv.get_identifier_slot(cn)
    if id_slot:
        return id_slot
    else:
        for s in sv.class_induced_slots(cn):
            if s.key:
                return s
        return None

def from_structure(table_name: str, column_name: str): return f'from({table_name}.{column_name})'

def format_enum_datatype_condition(enum_values: List[str]): return f"in({format_enum_str(enum_values)})"
    
def format_enum_str(enum_values: List[str]): return ','.join([f"'{v}'" for v in enum_values])

def validate_schema(classes: List[ClassDefinition], slots: List[SlotDefinition], enums: List[EnumDefinition]):
    # Check for slots not associated to a class
    class_slots = [slot for c in classes for slot in c.slots] + [slot for c in classes for slot in c.attributes]
    classless_slots = [s.name for s in slots if s.name not in class_slots]
    if classless_slots:
        LOGGER.warning(f"Slots not associated to a class won't be mapped: {', '.join(classless_slots)}")

def init_valve_table(valve_tsv_path: str, column_filter: dict, map_valve_row: Callable = lambda row: row):
    # TODO: Use VALVE lib to get metadata tables instead of a sample schema
    with open(valve_tsv_path, 'r') as table_file:
        reader = csv.DictReader(table_file, delimiter='\t')
        return [map_valve_row(row) for row in reader if (not column_filter) or (row.get("table") in column_filter)]

def map_table_path(row: dict, output_dir: str):
    """Returns copy of row with "path" modified"""
    return dict(row, path=os.path.join(output_dir, row.get("table") + ".tsv"))

if __name__ == "__main__":
    main()