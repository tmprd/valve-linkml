import os
import csv
import logging
from typing import List, Optional
from argparse import ArgumentParser

from linkml_runtime.utils.schemaview import SchemaView, SlotDefinition, ClassDefinition, ClassDefinitionName, EnumDefinition
import linkml.utils.converter

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

def main():
    # CLI
    parser = ArgumentParser()
    parser.add_argument('yaml_schema_path', type=str)
    parser.add_argument("-d", "--data-dir", help="Directory of LinkML YAML data files. These are NOT schemas!")
    args = parser.parse_args()

    # Logging
    LOGGER.setLevel(level=logging.WARNING)

    # Run
    map_schema(args.yaml_schema_path)
    if args.data_dir:
        map_data(args.yaml_schema_path, args.data_dir)


def class2table_row(class_name: str, class_description: str, table_path: str):
    # A class is a Table row
    return {
        "table": class_name,
        "path": f'{table_path}/{class_name}.tsv',
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
            # Range is not a class, e.g. a default/scalar type
            return slot_range_name
        elif slot_range_identifier_type is not None:
            # Range is a class AND has an identifier, so map to the datatype of the class's identifier (primary key)
            return slot_range_identifier_type
        else:
            # Range is a class but doesn't have an identifier, so map to the default datatype
            return DEFAULT_DATATYPE
    else:
        return DEFAULT_DATATYPE


def slot2column_structure(slot_range_name: str, slot_range_identifier_name: str, is_slot_range_a_class: bool, is_primary_key: bool):
    # These map to a Column's "structure":
        # - 'primary' if the slot is an identifier or key
        # - a 'from()' foreign key constraint if the slot has a range that's a class AND that class has an identifier/primary key
    if is_primary_key:
        return 'primary'
    elif (slot_range_name is not None) and is_slot_range_a_class: #and (slot_range_identifier_name is not None):
        # from() represents a foreign key constraint. The foreign key table is the class, the foreign key column is the identifier for that class.
        return f'from({slot_range_name}.{slot_range_identifier_name or "?"})'
    else:
        return None

def slot2datatype_row(slot_name: str, slot_description, slot_pattern: str, slot_enum_values: List[str] = None):
    # A class-specific slot_usage maps to a Datatype row
    # TODO Map slot minimum_value & maximum_value to ...?
    return {
        "datatype": slot_name,
        "parent": None, # TODO choose from [nonspace, trimmed_line, line, word, trimmed_text, text] ?
        "transform": None,
        # A slot's pattern, or the permissible values of a slot's enum, maps to a Datatype "condition"
        "condition": (f"match(/{slot_pattern}/)" if slot_pattern else None) or (f"in({format_enum_str(slot_enum_values)})" if slot_enum_values else None),
        "structure": None, 
        "description": slot_description,
        "SQLite type": None, # TODO
        "PostgreSQL type": None, # TODO
        "RDF type": None, # TODO
        "HTML type": None, # TODO
    }

def map_schema(yaml_schema_path: str):
    valve_dir = os.path.dirname(yaml_schema_path)
    linkml_schema = SchemaView(yaml_schema_path)
    
    all_classes = linkml_schema.all_classes().values()
    all_slots = linkml_schema.all_slots().values()
    all_enums = linkml_schema.all_enums().values()

    validate_schema(all_classes, all_slots, all_enums)

    table_dicts = []
    column_dicts = []
    datatype_dicts = []

    # Map classes to Table table
    for linkml_class in all_classes:
        table_dicts.append(class2table_row(linkml_class.name, linkml_class.description, valve_dir))

        # Map slots of (transitively) inherited classes to additional columns for this class in the Column table (note: these may include slots/attributes inherited from "mixins")
        inherited_slots = get_inherited_class_slots(linkml_schema, linkml_class)
        for inherited_slot in inherited_slots:
            map_class_slot(linkml_schema, inherited_slot, linkml_class, column_dicts, datatype_dicts, all_classes)

        # Map class slots to Column table
        for slot_name in linkml_class.slots:
            slot = next((s for s in all_slots if s.name == slot_name), None) # TODO check if specified class slot isn't in the list of all slots ...
            map_class_slot(linkml_schema, slot, linkml_class, column_dicts, datatype_dicts, all_classes)
            
        # Map attributes to the Column table
        for attribute in linkml_class.attributes:
            attribute_slot = next((s for s in all_slots if s.name == attribute), None)
            map_class_slot(linkml_schema, attribute_slot, linkml_class, column_dicts, datatype_dicts, all_classes)

    # Map enums
    for enum in all_enums:
        map_enum(enum, column_dicts, datatype_dicts, table_dicts, valve_dir)

    # Serialize to VALVE TSVs
    write_dicts2tsv(valve_dir + '/column.tsv', column_dicts, VALVE_SCHEMA["column"]["headers"])
    write_dicts2tsv(valve_dir + '/table.tsv', table_dicts, VALVE_SCHEMA["table"]["headers"])
    write_dicts2tsv(valve_dir + '/datatype.tsv', datatype_dicts, VALVE_SCHEMA["datatype"]["headers"])

def map_class_slot(schemaView: SchemaView, slot: SlotDefinition, slot_class: ClassDefinition, 
                   column_dicts: List[dict], datatype_dicts: List[dict],
                   all_classes: List[ClassDefinition]):
    # Slot properties that are relevant to its corresponding Column table mapping
    is_slot_required = slot.required
    is_slot_range_a_class = False
    is_slot_primary_key = slot.identifier or slot.key

    # Map class-specific slot_usage to a new Datatype row
    slot_usage = slot_class.slot_usage.get(slot.name)
    slot_usage_datatype = None
    if slot_usage:
        # Create a new Datatype for this slot usage and set that as the "datatype" in the Column table.
        ### Example: "primary_email" in Person uses a "person_primary_email" datatype
        slot_usage_datatype = f"{slot_class.name.lower()}_{slot_usage.name}"
        datatype_dicts.append(slot2datatype_row(slot_usage_datatype, None, slot_usage.pattern, None))
    
        # If slot usage is required, then the slot is required in the context of this class
        is_slot_required = slot_usage.required

    # If this slot's range is a class, then we need that class's identifier (slot)... so we need all the slots of THAT class
    range_class = next((c for c in all_classes if c.name == slot.range), None)
    range_class_identifier_name = None
    range_class_identifier_datatype = None
    if range_class:
        is_slot_range_a_class = True
        # TODO: Get other imported identifiers from, e.g. Address's class_uri: schema:PostalAddress
        range_class_identifier = get_identifier_or_key_slot(schemaView, range_class.name)
        if range_class_identifier:
            range_class_identifier_name = range_class_identifier.name
            # TODO: Does this identifier have a slot_usage?
            slot_range_class_identifier_usage_datatype = None
            # Finally get the datatype
            # Note: the range of the class identifier will be the "default_range" in the schema if not specified otherwise
            range_class_identifier_datatype = slot2column_datatype(slot_range_class_identifier_usage_datatype, range_class_identifier.range, False, None)

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

def map_enum(enum: EnumDefinition, column_dicts: List[dict], datatype_dicts: List[dict], table_dicts: List[dict], valve_dir: str):
     # Map permissible enum values to "condition" in Datatype table
    datatype_dicts.append(slot2datatype_row(enum.name, enum.description, None, enum.permissible_values))

    # TODO: Map enums to Table table, with a Permissible Value column and IRI/meaning column
    # TODO: designate enum table "type" as "enum table" ?
    # table_dicts.append(class2table(enum.name, enum.description, valve_dir))
    # column_dicts.append(slot2column("permissible_value", enum.name, "Permissible Value", None, None, False))
    # column_dicts.append(slot2column("meaning", enum.name, "IRI Meaning", None, None, False))

    # TODO: actually create the enum table with permissible values + IRIs as rows


def map_data(yaml_schema_path: str, yaml_data_dir: str):
    for file in os.listdir(yaml_data_dir):
        if not file.endswith(".yaml"): continue
        # `linkml-convert -t tsv --index-slot persons -o test/linkml2valve/data/personinfo_data_valid.tsv -s test/linkml2valve/schema/personinfo.yaml test/linkml2valve/data/personinfo_data_valid.yaml`
        # Problem: this LinkML function should be available as undecorated to call programatically
        # Problem: this LinkML function fails to convert if the data is invalid
        # Problem: need to know "index-slot" of the data for outputting TSVs
        linkml.utils.converter.cli.callback(input=os.path.join(yaml_data_dir, file), schema=yaml_schema_path, output_format="tsv", module=None, target_class=None)

def get_inherited_class_slots(schemaView: SchemaView, linkml_class: ClassDefinition) -> List[SlotDefinition]:
    """Get only slots of this class that are inherited from some ancestor class, so we can track these separately from the non-inherited slots/attributes"""
    return [slot for slot in schemaView.class_induced_slots(linkml_class.name) if (slot.name not in linkml_class.slots) and (slot.name not in linkml_class.attributes)]

# Copied from https://github.com/linkml/linkml/blob/c933c7c0c82e3eaa48d815f9cae033360626438e/linkml/generators/typescriptgen.py#L140
def get_identifier_or_key_slot(sv: SchemaView, cn: ClassDefinitionName) -> Optional[SlotDefinition]:
    """Get class's identifier slot, or a slot that's an identifier from some (transitively) inherited class"""
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

def write_dicts2tsv(filepath: str, rendered_data: list, headers: list) -> str:
    with open(filepath, "w") as file:
        writer = csv.DictWriter(file, delimiter="\t", fieldnames=headers, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rendered_data)
    print(f"Wrote to '{filepath}'")

def validate_schema(classes: List[ClassDefinition], slots: List[SlotDefinition], enums: List[EnumDefinition]):
    # Check for slots not associated to a class
    class_slots = [slot for c in classes for slot in c.slots] + [slot for c in classes for slot in c.attributes]
    classless_slots = [s.name for s in slots if s.name not in class_slots]
    if classless_slots:
        LOGGER.warning(f"Slots not associated to a class won't be mapped: {', '.join(classless_slots)}")


if __name__ == "__main__":
    main()