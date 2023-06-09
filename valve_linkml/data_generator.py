#!/usr/bin/env python3
import os
import csv
from typing import List

def generate_schema_data(table_dicts: List[dict], column_dicts: List[dict]):
    print("Generating data tables...")

    # Map some pre-generated data to our data tables if we have the right kind
    table_names = [t["table"] for t in table_dicts]
    if "Person" in table_names and "Address" in table_names:
        pregenerated_table_data: dict = generate_tables_from_fhir_mapping(table_dicts, column_dicts)

    # Create the data tables themselves
    for table_dict in table_dicts:
        table_name = table_dict["table"]
        table_path = table_dict["path"]

        # Create the directory if needed
        os.makedirs(os.path.dirname(table_path), exist_ok=True)
        
        # if os.path.exists(table_path): continue
        with open(table_path, 'w') as table_file:
            table_columns = [c for c in column_dicts if c["table"] == table_dict["table"]]
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
                
            print(f"Wrote to '{table_path}'")

            # Experimental generation
            # create_generation_prompt(table_name, table_columns)


def generate_tables_from_fhir_mapping() -> dict:
    # Read the pre-generated data
    TEST_PATIENT_DATA_FILE_PATH = "test/linkml2valve/data/synthea_patients.csv"
    with open(TEST_PATIENT_DATA_FILE_PATH, "r") as synthea_patients_file:
        reader = csv.DictReader(synthea_patients_file, delimiter=",")
        # Map the columns we want
        person_dicts = []
        address_dicts = []
        for patient_row in reader:
            person_dicts.append(map_fhirpatient2person(patient_row))
            address_dicts.append(map_fhirpatient2address(patient_row))
        return {"Person":person_dicts, "Address":address_dicts}


def map_fhirpatient2person(fhir_patient: dict):
    # person_columns = ['Id', 'BIRTHDATE', 'FIRST', 'LAST', 'GENDER']
    # aliases	id	name	description	image	primary_email	birth_date	age_in_years	gender	current_address	has_employment_history	has_familial_relationships	has_medical_history
    return {
        "id": fhir_patient["Id"],
        "birth_date": fhir_patient["BIRTHDATE"],
        "name": f'{fhir_patient["FIRST"]} {fhir_patient["LAST"]}',
        "gender": fhir_patient["GENDER"]
    }

def map_fhirpatient2address(fhir_patient: dict):
    # address_columns = ['ADDRESS', 'CITY', 'STATE', 'ZIP']
    # street	city	postal_code
    return {
        "street": fhir_patient["ADDRESS"],
        "city": fhir_patient["CITY"],
        "postal_code": fhir_patient["ZIP"]
    }



def create_generation_prompt(table_name: str, table_column_dicts: List[str]):
    """Warning: experimental!"""

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
