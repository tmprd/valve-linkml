import csv
import random
from datetime import date, datetime
from logging import Logger

from .utils import generate_error_set

def generate_tables_from_fhir_mapping(logger: Logger) -> dict:
    # Read the pre-generated data
    TEST_PATIENT_DATA_FILE_PATH = "test/synthea/patients.csv"
    TEST_ENCOUNTER_DATA_FILE_PATH = "test/synthea/encounters.csv"
    TEST_PROCEDURE_DATA_FILE_PATH = "test/synthea/procedures.csv"
    with open(TEST_PATIENT_DATA_FILE_PATH, "r") as patients_file, open(TEST_ENCOUNTER_DATA_FILE_PATH, "r") as encounters_file, open(TEST_PROCEDURE_DATA_FILE_PATH, "r") as procedures_file:
        patients = csv.DictReader(patients_file, delimiter=",")
        encounters = csv.DictReader(encounters_file, delimiter=",")
        procedures = csv.DictReader(procedures_file, delimiter=",")

        # Add some errors to some of the data
        person_error_set = generate_error_set(sum(1 for row in patients), 0.01)
        medicalevent_error_set = generate_error_set(sum(1 for row in encounters), 0.0001)
        print(f"Adding {len(person_error_set)} errors to Person table")
        print(f"Adding {len(person_error_set)} errors to Address table")
        print(f"Adding {len(medicalevent_error_set)} errors to MedicalEvent table")
        # Reset to beginning of files after counting rows
        patients_file.seek(0)
        encounters_file.seek(0)

        person_dicts = []
        address_dicts = []
        medicalevents_dicts = []
        procedure_dicts = []

        for index, patient in enumerate(patients):
            if index == 0: continue # skip header row because we've enumerated it
            person_address = map_fhir_patient2address(patient, index)            
            person = map_fhir_patient2person(patient, index, person_address)
            if index in person_error_set:
                logger.info(f"Adding fake invalid data to Person {person['id']}")
                person["gender"] = "invalid-example"

                logger.info(f"Adding fake invalid data to Address {person_address['id']}")
                person_address["id"] = "invalid-example"

            person_dicts.append(person)
            address_dicts.append(person_address)

        unique_procedures = list({procedure["CODE"]: procedure for procedure in procedures}.values())
        for index, procedure in enumerate(unique_procedures):
            if index == 0: continue # skip header row 
            procedure_dicts.append(map_fhir_procedure2procedure_concept(procedure, index))

        for index, event in enumerate(encounters):
            if index == 0: continue # skip header row
            procedure = random.choice(procedure_dicts)
            medicalevent = map_fhir_encounter2medical_event(event, index, procedure["id"])
            if index in medicalevent_error_set:
                logger.info(f"Adding fake invalid data to MedicalEvent {medicalevent['id']}")
                medicalevent["procedure"] = "invalid-example"
            medicalevents_dicts.append(medicalevent)

        return {"Person":person_dicts, "Address":address_dicts, "MedicalEvent": medicalevents_dicts, "ProcedureConcept": procedure_dicts}
    

def map_fhir_patient2person(fhir_patient: dict, index: str, person_address: dict):
    """Depends on a mapped person address id because of foreign key"""
    # person_columns = ['Id', 'BIRTHDATE', 'FIRST', 'LAST', 'GENDER']
    # aliases	id	name	description	image	primary_email	birth_date	age_in_years	gender	current_address	has_employment_history	has_familial_relationships	has_medical_history
    return {
        "id": index, # generated int ID instead of using uuid for performance
        "birth_date": fhir_patient["BIRTHDATE"],
        "age_in_years": calculate_age(datetime.strptime(fhir_patient["BIRTHDATE"], "%Y-%m-%d")),
        "name": f'{fhir_patient["FIRST"]} {fhir_patient["LAST"]}',
        "gender": random.choice(['cisgender woman', 'nonbinary woman', 'transgender woman']) if (fhir_patient["GENDER"] == "F") else random.choice(['cisgender man', 'nonbinary man', 'transgender man']),
        "current_address": person_address["id"] # foreign key
    }

def map_fhir_patient2address(fhir_patient: dict, index: str):
    # address_columns = ['ADDRESS', 'CITY', 'STATE', 'ZIP']
    # street	city	postal_code
    return {
        "street": fhir_patient["ADDRESS"],
        "city": fhir_patient["CITY"],
        "postal_code": fhir_patient["ZIP"],
        "id": index # generated ID
    }

def map_fhir_encounter2medical_event(fhir_encounter: dict, index: str, procedure_fk: str):
    # Id, START, STOP, PATIENT, ORGANIZATION, PROVIDER, PAYER, ENCOUNTERCLASS, CODE, DESCRIPTION, BASE_ENCOUNTER_COST, TOTAL_CLAIM_COST, PAYER_COVERAGE, REASONCODE, REASONDESCRIPTION
    # started_at_time	ended_at_time	duration	is_current	in_location	diagnosis	procedure
    return {
        "started_at_time": fhir_encounter["START"],
        "ended_at_time": fhir_encounter["STOP"],
        "duration": (datetime.strptime(fhir_encounter["STOP"], "%Y-%m-%dT%H:%M:%SZ") - datetime.strptime(fhir_encounter["START"], "%Y-%m-%dT%H:%M:%SZ")).total_seconds() / 60,
        "procedure": procedure_fk,
        "id": index, # generated ID
        # Add a link to person id as part of multivalued mapping. Just use random for now
        # "person": 
    }

def map_fhir_procedure2procedure_concept(fhir_procedure: dict, index: str):
    # DATE,PATIENT,ENCOUNTER,CODE,DESCRIPTION,BASE_COST,REASONCODE,REASONDESCRIPTION
    # id	name	description	image
    return {
        "id": index, # generated ID, use index instead of procedure code for faster loading
        "name": fhir_procedure["CODE"],
        "description": fhir_procedure["DESCRIPTION"],
    }


def calculate_age(born):
    today = date.today()
    return today.year - born.year - ((today.month, today.day) < (born.month, born.day))