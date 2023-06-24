import os
import valve_linkml.linkml2valve
from valve_linkml.valve_schema import primary_structure, from_structure2table_column, is_from_structure
from linkml_runtime.utils.schemaview import SchemaView

def test_schema_mapping(yaml_schema_path: str, mapped_schema_tables: dict):
    linkml_schema = SchemaView(yaml_schema_path)
    all_classes = linkml_schema.all_classes().values()
    all_enums = linkml_schema.all_enums().values()
    all_slots = linkml_schema.all_slots().values()
    all_types = linkml_schema.all_types().values()

    all_table_names = [t["table"] for t in mapped_schema_tables["table"]["rows"]]
    all_columns = mapped_schema_tables["column"]["rows"]
    all_column_names = []
    all_column_table_names = []
    for c in all_columns:
        all_column_names.append(c["column"])
        all_column_table_names.append(c["table"])
    all_datatype_names = [c["datatype"] for c in mapped_schema_tables["datatype"]["rows"]]


    # Every LinkML class name should be the value of some "table" column in the Table table
    for linkml_class in all_classes:
        if not linkml_class.name in all_table_names:
            raise Exception(f"LinkML class {linkml_class.name} not found in Table table")

    # Every LinkML enum name should be the value of some "table" column in the Table table
    for linkml_enum in all_enums:
        if not linkml_enum.name in all_table_names:
            raise Exception(f"LinkML enum {linkml_enum.name} not found in Table table")

    # Every non-multivalued LinkML slot name should be the value of some "column" column in the Column table
    for linkml_slot in all_slots:
        if not linkml_slot.multivalued and not linkml_slot.name in all_column_names:
            raise Exception(f"LinkML slot {linkml_slot.name} not found in Column table")

    # Every LinkML type name should be the value of some "datatype" column in the Datatype table
    for linkml_type in all_types:
        if not linkml_type.name in all_datatype_names:
            raise Exception(f"LinkML type {linkml_type.name} not found in Datatype table")

    # TODO: Every LinkML slot_usage should be the transformed value of some "datatype" column in the Datatype table, and some "datatype" column in the Column table

    # TODO: Every LinkML multivalued slot range should be the transformed value of some foreign key "structure" in the Column table

    # TODO: Every LinkML slot range that is a LinkML class should be the transformed value of some foreign key "structure" in the Column table

    # Table table should include VALVE metadata rows
    valve_metadata_table_names = ["table", "column", "datatype"]
    for table_name in valve_metadata_table_names:
        if not table_name in all_table_names:
            raise Exception(f"VALVE metadata table '{table_name}' not found in Table table")
        
        # Column table should include VALVE metadata rows (Each valve metadata table name is the value of some "table" column in the Column table)
        if not table_name in all_column_table_names:
            raise Exception(f"VALVE metadata '{table_name}' columns not found in Column table")
        
    # If a Column table row has a foreign key "structure", then that should correspond to exactly 1 other Column table row whose "structure" is a primary key
    for column in all_columns:
        if column.get("structure") and is_from_structure(column["structure"]):
            fk_table_name, fk_column_name = from_structure2table_column(column["structure"])
            matching_fk_columns = [c for c in all_columns if c["table"] == fk_table_name and c["column"] == fk_column_name]
            if not len(matching_fk_columns):
                raise Exception(f"No Column table row found corresponding to foreign key 'structure' {column['structure']}")
            if len(matching_fk_columns) > 1:
                raise Exception(f"More than 1 Column table row found corresponding to foreign key 'structure' {column['structure']}")
            fk_column = matching_fk_columns[0]
            if not fk_column["structure"] == primary_structure():
                raise Exception(f"Column table row {fk_column['table']}.{fk_column['column']} corresponding to foreign key 'structure' {column['structure']} does not have primary key 'structure'")

    # TODO: Datatype table should include VALVE metadata rows


if __name__ == "__main__":
    base_dir = "test/"
    input_file = os.path.join(base_dir, "linkml_input", "personinfo.yaml")
    output_dir = os.path.join(base_dir, "valve_output")

    mapped_schema_tables = valve_linkml.linkml2valve.linkml2valve(input_file, output_dir, data_dir=None, generate_data=True, log_verbosely=True)

    test_schema_mapping(input_file, mapped_schema_tables)