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

    # Create the actual data table files with some generated data (exclude VALVE metadata rows)
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

    # Map LinkML yaml data and serialize to VALVE data TSVs
    if data_dir is not None:
        map_data(yaml_schema_path, data_dir)



def class2table_row(class_name: str, class_description: str, table_dir: str):
    # A class is a Table row
    return {
        "table": class_name,
        "path": f'{table_dir}/{class_name}.tsv',
        "description": class_description,
        "type": None,
    }

def slot2column_row(slot_name: str,
                slot_class_name: str, 
                slot_description: str, 
                slot_range_name: str, 
                slot_range_identifier_name: str,
                slot_range_identifer_type: str,
                slot_usage_datatype: str,
                is_slot_range_a_class: bool, 
                is_primary_key: bool,
                is_required: bool):
    # A slot/attribute is a Column row
    return {
        "table": slot_class_name,
        "column": slot_name,
        "nulltype": 'empty' if not is_required else None,
        "datatype": slot2column_datatype(slot_usage_datatype, slot_range_name, is_slot_range_a_class, slot_range_identifer_type),
        "structure": slot2column_structure(slot_range_name, slot_range_identifier_name, is_slot_range_a_class, is_primary_key),
        "description": slot_description,
    }

def slot2column_datatype(slot_usage_datatype: str, slot_range_name: str, is_slot_range_a_class: bool, slot_range_identifier_type: str):
    # These map to a Column's "datatype":
        # - a slot's class-specific, slot_usage datatype
        # - a primitive type if the range is not a class
        # - a slot's range class's primary key if the range is a class
        # - the default datatype
    if slot_usage_datatype is not None:
        return slot_usage_datatype
    elif slot_range_name is not None:
        if not is_slot_range_a_class:
            # Range is not a class, e.g. a default/scalar type, so map to its name
            return slot_range_name
        elif slot_range_identifier_type is not None:
            # Range is a class AND has an identifier, so map to the datatype of the class's identifier (primary key)
            return slot_range_identifier_type
        else:
            # Range is a class but doesn't have an identifier, so map to the default datatype
            return SCHEMA_DEFAULT_RANGE
    else:
        return SCHEMA_DEFAULT_RANGE


def slot2column_structure(slot_range_name: str, slot_range_identifier_name: str, is_slot_range_a_class: bool, is_primary_key: bool):
    # These map to a Column's "structure":
        # - 'primary' if the slot is an identifier or key
        # - a 'from()' foreign key constraint if the slot has a range that's a class AND that class has an identifier/primary key
    if is_primary_key:
        return 'primary'
    elif (slot_range_name is not None) and is_slot_range_a_class and (slot_range_identifier_name is not None):
        # from() represents a foreign key constraint. The foreign key table is the class, the foreign key column is the identifier for that class.
        # TODO: if the range class doesn't have an identifier, we'll need to create one and also generate identifier values for its data
        return f'from({slot_range_name}.{slot_range_identifier_name})'
    else:
        return None

def slot2datatype_row(slot_name: str, slot_description, slot_pattern: str, slot_enum_values: List[str] = None):
    # A class-specific slot_usage maps to a Datatype row
    # TODO Map slot minimum_value & maximum_value to ...?
    return {
        "datatype": slot_name,
        "parent": DEFAULT_PARENT_DATATYPE,
        "transform": None,
        # A slot's pattern, or the permissible values of a slot's enum, maps to a Datatype "condition"
        "condition": (f"match(/{slot_pattern}/)" if slot_pattern else None) or (f"in({format_enum_str(slot_enum_values)})" if slot_enum_values else None),
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
    SCHEMA_DEFAULT_RANGE = linkml_schema.schema.default_range
    LOGGER.debug(f"{len(all_classes)} classes, {len(all_slots)} slots, {len(all_enums)} enums parsed from '{yaml_schema_path}'")
    validate_schema(all_classes, all_slots, all_enums)

    table_dicts = []
    column_dicts = []
    datatype_dicts = []

    # Map classes to Table table
    for linkml_class in all_classes:
        table_dicts.append(class2table_row(linkml_class.name, linkml_class.description, data_table_dir))

        # Map slots of (transitively) inherited classes to additional columns for this class in the Column table. (Note: these include slots/attributes inherited from "mixins".)
        inherited_slots = get_inherited_class_slots(linkml_schema, linkml_class)
        for inherited_slot in inherited_slots:
            map_class_slot(linkml_schema, inherited_slot, linkml_class, column_dicts, datatype_dicts, all_classes, all_enums)

        # Map class slots to Column table
        for slot_name in linkml_class.slots:
            slot = next((s for s in all_slots if s.name == slot_name), None) # TODO check if specified class slot isn't in the list of all slots ...
            map_class_slot(linkml_schema, slot, linkml_class, column_dicts, datatype_dicts, all_classes, all_enums)
            
        # Map attributes to the Column table
        for attribute in linkml_class.attributes:
            attribute_slot = next((s for s in all_slots if s.name == attribute), None)
            map_class_slot(linkml_schema, attribute_slot, linkml_class, column_dicts, datatype_dicts, all_classes, all_enums)

        # If no identifier slot was found and used as a primary key, we need to make one for this class and add it to the Column table
        class_primary_key = next((c for c in column_dicts if c["table"] == linkml_class.name and c["structure"] == "primary"), None)
        if class_primary_key is None:
            LOGGER.info(f"Class '{linkml_class.name}' has no identifier slot. Creating primary key '{DEFAULT_PRIMARY_KEY}' and adding it to the Column table.")
            # TODO: maybe add this as the first column...
            column_dicts.append(slot2column_row(DEFAULT_PRIMARY_KEY, linkml_class.name, "generated column", None, None, None, None, is_slot_range_a_class=False, is_primary_key=True, is_required=True))

    # Map multivalued slots to new columns in the class table of the multivalued slot's range. This has to be done after adding primary keys for missing class identifiers.
    for slot in all_slots:
        if not slot.multivalued: continue
        slot_class = next((c for c in all_classes if slot.name in c.slots), None)
        if slot_class is None: continue
        map_multivalued_slot(slot, slot_class, column_dicts)

    # Map enums
    for enum in all_enums:
        map_enum(enum, column_dicts, datatype_dicts, table_dicts, output_dir)

    # Add "default_range" as a datatype if needed
    if not SCHEMA_DEFAULT_RANGE in [d["datatype"] for d in datatype_dicts]:
        datatype_dicts.append(slot2datatype_row(SCHEMA_DEFAULT_RANGE, None, None))

    return {
        "table": {"rows":table_dicts, "path": output_dir + '/table.tsv'},
        "column": {"rows": column_dicts, "path": output_dir + '/column.tsv'}, 
        "datatype": {"rows": datatype_dicts, "path": output_dir + '/datatype.tsv'}
    }


def map_class_slot(schemaView: SchemaView, slot: SlotDefinition, slot_class: ClassDefinition, 
                   column_dicts: List[dict], datatype_dicts: List[dict],
                   all_classes: List[ClassDefinition], all_enums: List[EnumDefinition]):
    
    # Slot properties that are relevant to its corresponding Column table mapping
    is_slot_required = slot.required
    is_slot_range_a_class = False
    is_slot_primary_key = slot.identifier or slot.key

    # If the slot is multivalued, don't add it as a column for this table (effectively removing it as a column). 
    # This will be mapped to another table after all class slots have been mapped and primary keys are generated.
    if slot.multivalued:
        LOGGER.info(f"Skipping multivalued slot '{slot.name}' in class '{slot_class.name}' for now.")
        return

    # Map the range of a class-specific slot_usage to a new Datatype row. Example: "primary_email" in Person uses a "person_primary_email" datatype.
    # If slot_usage has a class as its range, then we map that to a from() foreign key "structure" in the Column table.
    # TODO: If slot_usage has an enum as its range, then we map that to a from() foreign key "structure" in the Column table.
    slot_usage = slot_class.slot_usage.get(slot.name)
    slot_usage_datatype = None
    if slot_usage is not None:
        # If the slot usage range is a class, then we only need the identifier of that class as the datatype
        slot_usage_class = next((c for c in all_classes if c.name == slot_usage.range), None)
        # If the slot_usage range is an Enum, save it for later when adding all Enums
        slot_usage_enum = next((e for e in all_enums if e.name == slot_usage.range), None)
        if slot_usage_class is not None:
            # TODO get datatype of identifier of range class of slot usage
            # slot_usage_datatype = ...
            pass
        elif slot_usage_enum is None:
            # Create a new Datatype for this slot usage. Then set that as the "datatype" in the Column table.
            slot_usage_datatype = f"{slot_class.name.lower()}_{slot_usage.name}"
            datatype_dicts.append(slot2datatype_row(slot_usage_datatype, None, slot_usage.pattern, None))
    
        # If slot usage is required, then the slot is required in the context of this class
        is_slot_required = slot_usage.required

    # If this slot's range is a class, then we need that class's identifier (slot) and datatype of that identifier
    range_class = next((c for c in all_classes if c.name == slot.range), None)
    range_class_identifier_name = None
    range_class_identifier_datatype = None
    if range_class is not None:
        is_slot_range_a_class = True
        range_class_identifier = get_identifier_or_key_slot(schemaView, range_class.name)
        if range_class_identifier is not None:
            range_class_identifier_name = range_class_identifier.name
            # TODO: Does this identifier have a slot_usage?
            slot_range_class_identifier_usage_datatype = None
            # Finally get the datatype. Note: the range of the class identifier will be the "default_range" in the schema if not specified otherwise
            range_class_identifier_datatype = slot2column_datatype(slot_range_class_identifier_usage_datatype, range_class_identifier.range, False, None)
        else:
            LOGGER.info(f"Slot '{slot.name}' has range '{range_class.name}', but '{range_class.name}' has no identifier. Using {range_class.name}.{DEFAULT_PRIMARY_KEY} as the foreign key.")
            range_class_identifier_name = DEFAULT_PRIMARY_KEY
            range_class_identifier_datatype = DEFAULT_PRIMARY_KEY_DATATYPE

    # Map slot range to a datatype only if it's not a class or enum, and we haven't already added it from somewhere else (ex. slot usage)
    is_slot_range_an_enum = slot.range in [e.name for e in all_enums]
    if (slot.range is not None) and (not is_slot_range_a_class) and (not is_slot_range_an_enum) and (not slot.range in [d["datatype"] for d in datatype_dicts]):
        datatype_dicts.append(slot2datatype_row(slot.range, None, None, None))
        
    # Finally map slot to column
    column_dicts.append(slot2column_row(slot.name, 
                                        slot_class.name, 
                                        slot.description, 
                                        slot.range, 
                                        range_class_identifier_name,
                                        range_class_identifier_datatype,
                                        slot_usage_datatype, 
                                        is_slot_range_a_class,
                                        is_slot_primary_key,
                                        is_slot_required))


def map_multivalued_slot(slot: SlotDefinition, slot_class: ClassDefinition, column_dicts: List[dict]):
    """Add a new column to the range class of this slot, where the "structure" is from(<range_clas>.<identifier>)
    """
    # Ex. Person
    #       - has_medical_history:
    #           multivalued: true
    #           range: MedicalEvent
    #  => MedicalEvent
    #       - person:
    #           range: Person

    new_column_table_name = slot.range # ex. MedicalEvent
    new_column_name = slot_class.name.lower() # ex. person   
    new_column_fk_table = slot_class.name # ex. Person

    # Get the primary key of the table that serves as the range of this slot
    slot_class_table_dicts = [c for c in column_dicts if c["table"] == slot_class.name]
    slot_range_class_primary_key_column = next((c for c in slot_class_table_dicts if c["structure"] == "primary"), None)
    new_column_fk_column = slot_range_class_primary_key_column["column"] # ex. Person.id
    new_column_datatype = slot_range_class_primary_key_column["datatype"] # ex. string

    LOGGER.info(f"Mapping multivalued slot '{slot.name}' with range '{slot.range}' in class '{slot_class.name}' => to new column '{new_column_name}' in '{new_column_table_name}'")

    column_dicts.append(slot2column_row(new_column_name, new_column_table_name, f"generated column from multivalued slot {slot_class.name}.{slot.name}", 
                                        new_column_fk_table, new_column_fk_column, new_column_datatype,
                                        slot_usage_datatype=None, 
                                        is_slot_range_a_class=True, is_primary_key=False, is_required=slot.required))


def map_enum(enum: EnumDefinition, column_dicts: List[dict], datatype_dicts: List[dict], table_dicts: List[dict], valve_dir: str):
    # Map permissible enum values to "condition" in Datatype table
    datatype_dicts.append(slot2datatype_row(enum.name, enum.description, None, enum.permissible_values))

    # Map enum to Table table, with a Permissible Value column and IRI/meaning column
    # table_dicts.append(class2table_row(enum.name, enum.description, valve_dir))

    # TODO: designate enum table "type" as "enum table" ?
    # column_dicts.append(slot2column_row("permissible_value", enum.name, "Permissible Value", None, None, None, None, False, False, is_required=True))
    # column_dicts.append(slot2column_row("meaning", enum.name, "IRI Meaning", None, None, None, None, False, False, is_required=True))

    # Need to create a primary key so these permissible values can serve as foreign
    # column_dicts.append(slot2column_row(DEFAULT_PRIMARY_KEY, enum.name, None, None, None, None, None, None, is_primary_key=True, is_required=True))

    # TODO: actually create the enum table with permissible values + IRIs as rows


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

def format_enum_str(enum_values: List[str]):
    return ','.join([f"'{v}'" for v in enum_values])

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