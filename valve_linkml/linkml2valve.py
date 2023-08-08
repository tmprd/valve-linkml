#!/usr/bin/env python3
import os
import logging
from typing import List, Optional
from argparse import ArgumentParser

from linkml_runtime.utils.schemaview import SchemaView, SlotDefinition, ClassDefinition, ClassDefinitionName, EnumDefinition

from .valve_schema import VALVE_SCHEMA, table_row, column_row, datatype_row, primary_structure, from_structure, format_table_name, prepend_valve_tables
from .utils import write_dicts2tsv
from .data_generator import generate_schema_data

"""Usage: python3 -m valve_linkml.linkml2valve <linkml-yaml-schema-path> -d <linkml-yaml-data-directory>"""

LOGGER = logging.getLogger("linkml2valve")

# VALVE default constants
DEFAULT_DATATYPE = VALVE_SCHEMA["defaults"]["datatype"]
DEFAULT_PRIMARY_KEY = VALVE_SCHEMA["defaults"]["primary_key"]

# LinkML default constants
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

    # Validate args
    # check if output dir exists
    if not os.path.isdir(args.output_dir):
        raise ValueError(f"Output directory '{args.output_dir}' does not exist.")

    # Run
    linkml2valve(args.yaml_schema_path, args.output_dir, args.data_dir, args.generate_data, args.verbose)


def linkml2valve(yaml_schema_path: str, output_dir: str, data_dir: str = None, generate_data: bool = False, log_verbosely: bool = False):
    if log_verbosely:
        LOGGER.setLevel(level=logging.DEBUG)

    # Map LinkML schema to VALVE tables
    mapped_valve_schema: dict = map_schema(yaml_schema_path, output_dir)
    schema_tables = mapped_valve_schema["schema_tables"]

    # Write data table TSVs (without VALVE metadata rows)
    serialize_data_tables(schema_tables, mapped_valve_schema["data_tables"])

    # Create the data table files with some generated data (exclude VALVE metadata rows by not adding them yet, and Enum tables)
    if generate_data:
        generate_schema_data(schema_tables["table"]["rows"], schema_tables["column"]["rows"], LOGGER)

    # Prepend VALVE metadata rows to mapped schema tables
    schema_tables = prepend_valve_tables(schema_tables, output_dir, LOGGER)

    # Write schema/meta "config" table TSVs (with VALVE metadata rows prepended), ex. table, column, datatype
    serialize_schema_tables(schema_tables)

    # Map LinkML yaml data and serialize to VALVE data TSVs
    if data_dir is not None:
        map_data(yaml_schema_path, data_dir)

    return schema_tables


def serialize_schema_tables(schema_tables: List[dict]):
    # Serialize the combined VALVE schema and mapped LinkML schema to VALVE "config" TSVs, ex. table, column, datatype
    for schema_table_name in schema_tables:
        schema_table_dict = schema_tables[schema_table_name]
        schema_table_path = schema_table_dict["path"]
        write_dicts2tsv(schema_table_path, schema_table_dict["rows"], VALVE_SCHEMA["tables"][schema_table_name]["headers"])
        LOGGER.debug(f"Wrote schema table {len(schema_table_dict['rows'])} rows to '{schema_table_path}'")


def serialize_data_tables(schema_tables: List[dict], data_tables: List[dict]):
    # Serialize data tables listed in the Table table, and add data to them
    for schema_table_name in schema_tables:
        if schema_table_name == "table":
            schema_table_dict = schema_tables[schema_table_name]
            for table_row in schema_table_dict["rows"]:
                table_name = table_row["table"]
                table_path = table_row["path"]
                table_headers = [c["column"] for c in schema_tables["column"]["rows"] if c["table"] == table_name]
                table_data_rows = next((t for t in data_tables if t["table"] == table_name), {}).get("rows")
                write_dicts2tsv(table_path, table_data_rows, table_headers)
                if table_data_rows:
                    LOGGER.debug(f"Wrote data table {len(table_data_rows)} rows to '{table_path}'")


def map_schema(yaml_schema_path: str, output_dir: str) -> dict[str, dict[str, str]]:
    global SCHEMA_DEFAULT_RANGE
    # Data tables go in a subdirectory of the schema directory by default
    data_table_dir = os.path.join(output_dir, "data")
    if not os.path.isdir(data_table_dir):
        os.mkdir(data_table_dir)
        LOGGER.info(f"Created data directory '{data_table_dir}'")

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
        all_column_rows, all_datatype_rows = map_class_slots(linkml_schema, linkml_class, all_column_rows, all_datatype_rows, all_classes, all_enums)

    # Map multivalued slots to new columns in the class table of the multivalued slot's range. This has to be done after adding primary keys for missing class identifiers.
    all_column_rows = map_multivalued_slots(all_slots, all_classes, all_column_rows)

    # Map Enums
    all_table_rows, all_column_rows, data_tables = map_enums(all_enums, all_table_rows, all_column_rows, data_table_dir)

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
                    all_classes: List[dict], all_enums: List[dict]) -> List[dict]:
    
    new_column_rows = []
    new_datatype_rows = []
    class_has_primary_key = False

    # Map class slots and attributes, including inherited ones, to Column table rows. (Note: these include slots/attributes inherited from "mixins".)
    all_class_slots = get_all_class_slots_sorted(linkml_schema, linkml_class)
    for slot in all_class_slots:

        # Map slot to Column table row
        new_column_row = map_class_slot(linkml_schema, slot, linkml_class, all_classes, all_enums)
        if new_column_row is None: continue
        
        class_has_primary_key = class_has_primary_key or (new_column_row["structure"] == primary_structure())

        # Map the range of a class-specific slot_usage to a new Datatype row if the range is not a class or enum. This should be the dataype of the new Column row.
        slot_usage = linkml_class.slot_usage.get(slot.name)
        if slot_usage and is_datatype(slot_usage.range, all_enums, all_classes):
            # Create a new Datatype for this slot usage. Then set that as the "datatype" in the Column table.
            # Example: "primary_email" in Person uses a "person_primary_email" datatype.
            slot_usage_datatype = f"{linkml_class.name.lower()}_{slot_usage.name}"
            new_datatype_row =  datatype_row(slot_usage_datatype, None, slot_usage.pattern)
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
                                        SCHEMA_DEFAULT_RANGE, primary_structure(), is_required=True)
    table_index = column_rows.index(mapped_table_column_rows[0]) if len(mapped_table_column_rows) > 0 else len(column_rows)
    return column_rows[:table_index] + [new_primary_key_column] + column_rows[table_index:]
    

def map_multivalued_slots(all_slots: List[SlotDefinition], 
                          all_classes: List[ClassDefinition], 
                          all_column_rows: List[dict]) -> List[dict]:
    column_rows = []
    for slot in all_slots:
        if not slot.multivalued: continue
        slot_class = next((c for c in all_classes if slot.name in c.slots), None)
        if slot_class is None: continue
        if slot.range is None: continue # raise Exception(f"Error: No range found for multivalued slot '{slot.name}'.")
        if is_datatype(slot.range, [], all_classes):
            LOGGER.debug(f"Skipping multivalued '{slot.name}' with range '{slot.range}' because the range is a datatype")
            continue
        # Check if this table & column already exist - could have been generated from another class slot with the same range
        existing_column = next((c for c in column_rows if c["table"] == format_table_name(slot.range) and c["column"] == slot_class.name), None)
        if existing_column:
            LOGGER.warning(f"Skipping multivalued '{slot.name}' with range '{slot.range}' because {existing_column['table']}.{existing_column['column']} has already been generated from another slot with description: '{existing_column['description']}'")
            continue
        column_rows.append(map_multivalued_slot(slot, slot_class, all_column_rows))
    return all_column_rows + column_rows


def map_enums(all_enums: List[EnumDefinition], all_table_rows: List[dict], all_column_rows: List[dict], 
              output_dir: str) -> List[dict]:
    data_tables = []
    updated_table_rows = all_table_rows.copy()
    updated_column_rows = all_column_rows.copy()
    for enum in all_enums:
        # Map enum to Table table, with a Permissible Value column and IRI/meaning column
        updated_table_rows.append(table_row(enum.name, enum.description, output_dir))

        # Use the value as the primary key so it can serve as a foreign key
        updated_column_rows.append(column_row(ENUM_PRIMARY_KEY, enum.name, "Permissible Value", ENUM_PRIMARY_KEY_DATATYPE, primary_structure(), is_required=True))
        updated_column_rows.append(column_row("meaning", enum.name, "CURIE meaning", ENUM_MEANING_DATATYPE, None, is_required=False))

        # Map enum values to rows in the enum table
        permissible_values = enum.permissible_values
        data_tables.append({
            "table": enum.name,
            "rows": [ {ENUM_PRIMARY_KEY: v, "meaning": permissible_values[v].meaning} for v in permissible_values ],
        })
    return [updated_table_rows, updated_column_rows, data_tables]


def map_class_slot(schemaView: SchemaView, slot: SlotDefinition, slot_class: ClassDefinition, 
                   all_classes: List[ClassDefinition], all_enums: List[EnumDefinition]) -> Optional[dict]:
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
        column_structure = primary_structure()

    # Map slot range to the Column's datatype and structure
    elif slot.range is not None:
        # Note: This includes slot_usage ranges for this class slot
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
    # Ex. Person                        =>    table: "MedicalEvent", 
    #       - has_medical_history:      =>    column: "person",
    #           multivalued: true       =>    structure: from(Person.id)
    #           range: MedicalEvent     =>    "has_medical_history" is NOT added as column in the "Patient" table

    # Ex. association                           => table: ontology_class
    #       - subject category:                 => column: association
    #               range: ontology class       => structure: from(association.id)
    #       - subject category closure:
    #               multivalued: true
    #               range: ontology class

    # Get the primary key of the table that serves as the range of this slot
    slot_class_table_rows = [c for c in all_column_rows if c["table"] == format_table_name(slot_class.name)]
    slot_range_class_primary_key_column = next((c for c in slot_class_table_rows if c["structure"] == primary_structure()), None)

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


def get_all_class_slots_sorted(schemaView: SchemaView, linkml_class: ClassDefinition) -> List[SlotDefinition]:
    """Get all slots of a class, including inherited slots, sorted by inherited slots first"""
    sorted_slots = []
    slots = schemaView.class_induced_slots(linkml_class.name)
    for slot in slots:
        if is_slot_inherited(linkml_class, slot):
            sorted_slots.append(slot)
    for slot in slots:
        if not is_slot_inherited(linkml_class, slot):
            sorted_slots.append(slot)
    return sorted_slots


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


def is_slot_inherited(linkml_class: ClassDefinition, slot: SlotDefinition) -> bool:
    return (slot.name not in linkml_class.slots) and (slot.name not in linkml_class.attributes)

def is_enum_table(table_name, enum_primary_key, column_dicts):
    return any(c for c in column_dicts if c["table"] == table_name and c["column"] == enum_primary_key)

def is_datatype(slot_range, all_enums, all_classes):
    return not any(e for e in all_enums if e.name == slot_range) and not any(e for e in all_classes if e.name == slot_range)

def validate_schema(classes: List[ClassDefinition], slots: List[SlotDefinition], enums: List[EnumDefinition]):
    # Check for slots not associated to a class
    class_slots = [slot for c in classes for slot in c.slots] + [slot for c in classes for slot in c.attributes]
    classless_slots = [s.name for s in slots if s.name not in class_slots]
    if classless_slots:
        LOGGER.warning(f"Slots not associated to a class won't be mapped: {', '.join(classless_slots)}")


if __name__ == "__main__":
    main()