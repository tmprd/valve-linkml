import csv
import random

def generate_tables_from_fhir_mapping() -> dict:
    # Read the pre-generated data
    TEST_PATIENT_DATA_FILE_PATH = "test/linkml2valve/synthea/patients.csv"
    TEST_ENCOUNTER_DATA_FILE_PATH = "test/linkml2valve/synthea/encounters.csv"
    TEST_PROCEDURE_DATA_FILE_PATH = "test/linkml2valve/synthea/procedures.csv"
    with open(TEST_PATIENT_DATA_FILE_PATH, "r") as patients_file, open(TEST_ENCOUNTER_DATA_FILE_PATH, "r") as encounters_file, open(TEST_PROCEDURE_DATA_FILE_PATH, "r") as procedures_file:
        patients = csv.DictReader(patients_file, delimiter=",")
        person_dicts = []
        address_dicts = []
        medicalevents_dicts = []
        procedure_dicts = []
        for patient_row in patients:
            person_dicts.append(map_fhir_patient2person(patient_row))
            address_dicts.append(map_fhir_patient2address(patient_row))

        procedures = csv.DictReader(procedures_file, delimiter=",")
        unique_procedures = list({procedure["CODE"]: procedure for procedure in procedures}.values())
        for p in unique_procedures:
            procedure_dicts.append(map_fhir_procedure2procedure_concept(p))

        encounters = csv.DictReader(encounters_file, delimiter=",")
        procedures_list = list(unique_procedures)
        for e in encounters:
            procedure = random.choice(procedures_list)
            medicalevents_dicts.append(map_fhir_encounter2medical_event(e, procedure["CODE"]))

        return {"Person":person_dicts, "Address":address_dicts, "MedicalEvent": medicalevents_dicts, "ProcedureConcept": procedure_dicts}
    

def map_fhir_patient2person(fhir_patient: dict):
    # person_columns = ['Id', 'BIRTHDATE', 'FIRST', 'LAST', 'GENDER']
    # aliases	id	name	description	image	primary_email	birth_date	age_in_years	gender	current_address	has_employment_history	has_familial_relationships	has_medical_history
    return {
        "id": fhir_patient["Id"],
        "birth_date": fhir_patient["BIRTHDATE"],
        "name": f'{fhir_patient["FIRST"]} {fhir_patient["LAST"]}',
        "gender": fhir_patient["GENDER"],
        # TODO create address ID foreign key? Need to add to Address schema too
        "current_address": fhir_patient["ADDRESS"],
    }

def map_fhir_patient2address(fhir_patient: dict):
    # address_columns = ['ADDRESS', 'CITY', 'STATE', 'ZIP']
    # street	city	postal_code
    return {
        "street": fhir_patient["ADDRESS"],
        "city": fhir_patient["CITY"],
        "postal_code": fhir_patient["ZIP"]
    }

def map_fhir_encounter2medical_event(fhir_encounter: dict, procedure_code: str):
    # Id, START, STOP, PATIENT, ORGANIZATION, PROVIDER, PAYER, ENCOUNTERCLASS, CODE, DESCRIPTION, BASE_ENCOUNTER_COST, TOTAL_CLAIM_COST, PAYER_COVERAGE, REASONCODE, REASONDESCRIPTION
    # started_at_time	ended_at_time	duration	is_current	in_location	diagnosis	procedure
    return {
        "started_at_time": fhir_encounter["START"],
        "ended_at_time": fhir_encounter["STOP"],
        "procedure": procedure_code,
    }

def map_fhir_procedure2procedure_concept(fhir_procedure: dict):
    # DATE,PATIENT,ENCOUNTER,CODE,DESCRIPTION,BASE_COST,REASONCODE,REASONDESCRIPTION
    # id	name	description	image
    return {
        "id": fhir_procedure["CODE"],
        "name": fhir_procedure["DESCRIPTION"],
    }