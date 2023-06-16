#!/usr/bin/env python3
import os
import logging
from typing import List, Optional
from argparse import ArgumentParser

from linkml_runtime.utils.schemaview import SchemaView, SlotDefinition, ClassDefinition, ClassDefinitionName, EnumDefinition

from .valve_schema import VALVE_SCHEMA, DEFAULT_DATATYPE, DEFAULT_PRIMARY_KEY, PRIMARY_KEY_STRUCTURE, \
                            table_row, column_row, datatype_row, from_structure, prepend_valve_tables
from .utils import write_dicts2tsv
from .data_generator import generate_schema_data

"""Usage: python3 -m valve_linkml.linkml2valve <linkml-yaml-schema-path> -d <linkml-yaml-data-directory>"""

LOGGER = logging.getLogger("linkml2valve")

ENUM_PRIMARY_KEY = "permissible_value"
ENUM_PRIMARY_KEY_DATATYPE = DEFAULT_DATATYPE
ENUM_MEANING_DATATYPE = "CURIE"

# TODO change this to local var used in datatype mapping
SCHEMA_DEFAULT_RANGE = DEFAULT_DATATYPE

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
    if log_verbosely:
        LOGGER.setLevel(level=logging.DEBUG)

    # Map LinkML schema to VALVE tables
    mapped_valve_schema: dict = map_schema(yaml_schema_path, output_dir)
    schema_tables = mapped_valve_schema["schema_tables"]

    # Create the data table files with some generated data (exclude VALVE metadata rows by not adding them yet, and Enum tables)
    if generate_data:
        generate_schema_data(schema_tables["table"]["rows"], schema_tables["column"]["rows"], LOGGER)

    # Prepend VALVE metadata rows to mapped schema tables
    schema_tables = prepend_valve_tables(schema_tables, output_dir, LOGGER)

    # Write to TSVs
    serialize_valve_tables(schema_tables, mapped_valve_schema["data_tables"])

    # Map LinkML yaml data and serialize to VALVE data TSVs
    if data_dir is not None:
        map_data(yaml_schema_path, data_dir)


def serialize_valve_tables(schema_tables: List[dict], data_tables: List[dict]):
    # Serialize the combined VALVE schema and mapped LinkML schema to TSVs
    for meta_table_name in schema_tables:
        table_dict = schema_tables[meta_table_name]
        table_path = table_dict["path"]
        LOGGER.debug(f"Wrote {len(table_dict['rows'])} rows to '{table_path}'")
        write_dicts2tsv(table_path, table_dict["rows"], VALVE_SCHEMA[meta_table_name]["headers"])

        # Serialize Enum tables listed in the Table table, and add data to them
        if meta_table_name == "table":
            # For each table in the metatable
            for table_row in table_dict["rows"]:
                table_name = table_row["table"]
                # If enum table
                if is_enum_table(table_name, ENUM_PRIMARY_KEY, schema_tables["column"]["rows"]):
                    enum_table_path = table_row["path"]
                    enum_table_headers = [c["column"] for c in schema_tables["column"]["rows"] if c["table"] == table_name]
                    # Get the enum data and write it to the enum table
                    enum_data_table = next((t for t in data_tables if t["table"] == table_name), None)
                    if enum_data_table is not None:
                        write_dicts2tsv(enum_table_path, enum_data_table["rows"], enum_table_headers)
                        LOGGER.debug(f"Wrote {len(enum_data_table['rows'])} enum rows to '{enum_table_path}'")


def map_schema(yaml_schema_path: str, output_dir: str) -> dict[str, dict[str, str]]:
    global SCHEMA_DEFAULT_RANGE
    # Data tables go in a subdirectory of the schema directory by default
    data_table_dir = os.path.join(output_dir, "data")

    # Parse schema
    linkml_schema = SchemaView(yaml_schema_path)
    all_classes = linkml_schema.all_classes().values()
    all_slots = linkml_schema.all_slots().values()
    all_enums = linkml_schema.all_enums().values()
    all_types = linkml_schema.all_types().values()
    SCHEMA_DEFAULT_RANGE = linkml_schema.schema.default_range

    LOGGER.debug(f"{(len(all_classes))} classes, {len(all_slots)} slots, {len(all_enums)} enums parsed from '{yaml_schema_path}'")
    validate_schema(all_classes, all_slots, all_enums)

    all_table_rows: List[dict] = []
    all_column_rows: List[dict] = []
    all_datatype_rows: List[dict] = []
    data_tables: List[dict] = []

    # Map all types to Datatype table rows
    for type in all_types:
        all_datatype_rows.append(datatype_row(type.name, None, None))

    # Map classes to Table table rows
    for linkml_class in all_classes:
        all_table_rows.append(table_row(linkml_class.name, linkml_class.description, data_table_dir))

        # Map slots to Column table rows and slot usages to Datatype table rows
        all_column_rows, all_datatype_rows = map_class_slots(linkml_schema, linkml_class, all_column_rows, all_datatype_rows, all_classes, all_slots, all_enums)

    # Map multivalued slots to new columns in the class table of the multivalued slot's range. This has to be done after adding primary keys for missing class identifiers.
    all_column_rows = map_multivalued_slots(all_slots, all_classes, all_column_rows)

    # Map Enums
    data_tables = map_enums(all_enums, all_table_rows, all_column_rows, output_dir)

    # Check invariants
    assert len(all_table_rows) >= len(all_classes), f"{len(all_classes)} classes mapped to {len(all_table_rows)} tables. Expected at least as many tables as classes."
    assert len(all_column_rows) >= len(all_slots), f"{len(all_classes)} slots mapped to {len(all_column_rows)} columns. Expected at least as many columns as slots."

    # Return all mappings here
    return {
        "schema_tables": {
            "table": {"rows":all_table_rows, "path": output_dir + '/table.tsv'},
            "column": {"rows": all_column_rows, "path": output_dir + '/column.tsv'},
            "datatype": {"rows": all_datatype_rows, "path": output_dir + '/datatype.tsv'},
        },
        "data_tables": data_tables
    }


def map_class_slots(linkml_schema: SchemaView, linkml_class: ClassDefinition, 
                    all_column_rows: List[dict], all_datatype_rows: List[dict],
                    all_classes: List[dict], all_slots: List[dict], all_enums: List[dict]) -> List[dict]:
    
    new_column_rows = []
    new_datatype_rows = []
    class_has_primary_key = False

    # Map slots of (transitively) inherited classes to additional rows for this class in the Column table. (Note: these include slots/attributes inherited from "mixins".)
    inherited_slots = get_inherited_class_slots(linkml_schema, linkml_class)
    for inherited_slot in inherited_slots:
        new_column_row = map_class_slot(linkml_schema, inherited_slot, linkml_class, all_classes, all_enums)   
        # Add new column row
        if new_column_row is not None:
            new_column_rows.append(new_column_row)
            class_has_primary_key = class_has_primary_key or (new_column_row["structure"] == PRIMARY_KEY_STRUCTURE)

    # # Map class slots and attributes to Column table rows
    slot_names = list(linkml_class.slots) + list(linkml_class.attributes)
    for slot_name in slot_names:
        # Map slot to Column table row
        slot = next((s for s in all_slots if s.name == slot_name), None) # TODO check if specified class slot isn't in the list of all slots ...
        new_column_row = map_class_slot(linkml_schema, slot, linkml_class, all_classes, all_enums)
        
        if new_column_row is not None:
            class_has_primary_key = class_has_primary_key or (new_column_row["structure"] == PRIMARY_KEY_STRUCTURE)
            # Map slot usage to Datatype table row if its range isn't a class or enum. This should be the dataype of the Column row.
            slot_usage = linkml_class.slot_usage.get(slot_name)
            if slot_usage:
                new_datatype_row = slot_usage2datatype_row(slot_usage, linkml_class, all_classes, all_enums)
                if new_datatype_row:
                    # Update datatype of new column row
                    new_column_row["datatype"] = new_datatype_row["datatype"]
                    # Add new datatype row
                    new_datatype_rows.append(new_datatype_row)

            # Add new column row
            new_column_rows.append(new_column_row)

    # If no identifier slot was found and used as a primary key, we need to make one for this class and add it to the Column table
    if not class_has_primary_key:
        new_column_rows = generate_table_primary_key(linkml_class, new_column_rows)
    
    return [all_column_rows + new_column_rows, 
            all_datatype_rows + new_datatype_rows]


def generate_table_primary_key(linkml_class: ClassDefinition, column_rows: List[dict]) -> List[dict]:
    LOGGER.info(f"Class '{linkml_class.name}' has no identifier slot. Creating primary key '{DEFAULT_PRIMARY_KEY}' and adding it to the Column table.")
    # Add new primary key as the first column
    mapped_table_column_rows = [c for c in column_rows if c["table"] == linkml_class.name]
    new_primary_key_column = column_row(DEFAULT_PRIMARY_KEY, linkml_class.name, "generated column",
                                        SCHEMA_DEFAULT_RANGE, PRIMARY_KEY_STRUCTURE, is_required=True)
    print(new_primary_key_column)
    table_index = column_rows.index(mapped_table_column_rows[0]) if len(mapped_table_column_rows) > 0 else len(column_rows)
    return column_rows[:table_index] + [new_primary_key_column] + column_rows[table_index:]
    

def map_multivalued_slots(all_slots: List[SlotDefinition], all_classes: List[ClassDefinition], all_column_rows: List[dict]) -> List[dict]:
    column_rows = []
    for slot in all_slots:
        if not slot.multivalued: continue
        slot_class = next((c for c in all_classes if slot.name in c.slots), None)
        if slot_class is None: continue
        column_rows.append(map_multivalued_slot(slot, slot_class, all_column_rows))
    return all_column_rows + column_rows


def map_enums(all_enums: List[EnumDefinition], table_rows: List[dict], all_column_rows: List[dict], output_dir: str) -> List[dict]:
    data_tables = []
    for enum in all_enums:
        # Map enum to Table table, with a Permissible Value column and IRI/meaning column
        enum_table = table_row(enum.name, enum.description, output_dir)
        table_rows.append(enum_table)
        # Use the value as the primary key so it can serve as a foreign key
        all_column_rows.append(column_row(ENUM_PRIMARY_KEY, enum.name, "Permissible Value", ENUM_PRIMARY_KEY_DATATYPE, PRIMARY_KEY_STRUCTURE, is_required=True))
        all_column_rows.append(column_row("meaning", enum.name, "CURIE meaning", ENUM_MEANING_DATATYPE, None, is_required=False))
        # Map enum values to rows in the enum table
        permissible_values = enum.permissible_values
        data_tables.append({
            "table": enum.name,
            "rows": [ {ENUM_PRIMARY_KEY: v, "meaning": permissible_values[v].meaning} for v in permissible_values ],
        })
    return data_tables


# Map the range of a class-specific slot_usage to a new Datatype row. Example: "primary_email" in Person uses a "person_primary_email" datatype.
# If slot_usage has a class as its range, then map that to a from() foreign key "structure" in the Column table.
# TODO: If slot_usage has an enum as its range, then map that to a from() foreign key "structure" in the Column table.
def slot_usage2datatype_row(slot_usage: SlotDefinition, slot_class: ClassDefinition, all_classes: List[ClassDefinition], all_enums: List[EnumDefinition]) -> Optional[dict]:
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


def map_class_slot(schemaView: SchemaView, slot: SlotDefinition, slot_class: ClassDefinition, all_classes: List[ClassDefinition], all_enums: List[EnumDefinition]) -> Optional[dict]:
    # If the slot is multivalued, don't add it as a column for this table.
    # Instead, this will be mapped to another table after all class slots have been mapped and primary keys are generated.
    if slot.multivalued:
        LOGGER.info(f"Skipping multivalued slot '{slot.name}' in class '{slot_class.name}' for now.", )
        return None

    column_datatype = DEFAULT_DATATYPE
    column_structure = None

    # Map identifier slot to primary key structure of the Column
    if slot.identifier or slot.key:
        column_datatype = DEFAULT_DATATYPE
        column_structure = PRIMARY_KEY_STRUCTURE

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
                column_datatype = DEFAULT_DATATYPE
                column_structure = from_structure(range_class.name, DEFAULT_PRIMARY_KEY)

    # Map slot to Column
    return column_row(slot.name, slot_class.name, slot.description, column_datatype, column_structure, slot.required)


def map_multivalued_slot(slot: SlotDefinition, slot_class: ClassDefinition, all_column_rows: List[dict]):
    """Add a new column to the range class of this slot, where the "structure" is from(<range_class>.<identifier>)"""
    # Ex. Person
    #       - has_medical_history:
    #           multivalued: true
    #           range: MedicalEvent
    #  => MedicalEvent
    #       - person:
    #           range: Person

    # Get the primary key of the table that serves as the range of this slot
    slot_class_table_rows = [c for c in all_column_rows if c["table"] == slot_class.name]
    slot_range_class_primary_key_column = next((c for c in slot_class_table_rows if c["structure"] == PRIMARY_KEY_STRUCTURE), None)

    mapping_message = f"Mapping multivalued slot '{slot.name}' with range '{slot.range}' in class '{slot_class.name}'"
    if slot_range_class_primary_key_column is None:
        raise Exception(f"Error: {mapping_message}. No primary key found for range class '{slot.range}' but every class table should have a primary key.")

    column_table_name = slot.range # ex. MedicalEvent
    column_name = slot_class.name.lower() # ex. person
    column_description = f"generated column from multivalued slot {slot_class.name}.{slot.name}"
    column_datatype = slot_range_class_primary_key_column["datatype"] # ex. string
    column_structure = from_structure(slot_class.name, slot_range_class_primary_key_column["column"]) # ex. from(Person.id)

    LOGGER.info(f"{mapping_message} => to new column '{column_name}' in '{column_table_name}'")

    return column_row(column_name, column_table_name, column_description, column_datatype, column_structure, slot.required)


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


def is_enum_table(table_name, enum_primary_key, column_dicts):
    return any(c for c in column_dicts if c["table"] == table_name and c["column"] == enum_primary_key)


def validate_schema(classes: List[ClassDefinition], slots: List[SlotDefinition], enums: List[EnumDefinition]):
    # Check for slots not associated to a class
    class_slots = [slot for c in classes for slot in c.slots] + [slot for c in classes for slot in c.attributes]
    classless_slots = [s.name for s in slots if s.name not in class_slots]
    if classless_slots:
        LOGGER.warning(f"Slots not associated to a class won't be mapped: {', '.join(classless_slots)}")


if __name__ == "__main__":
    main()