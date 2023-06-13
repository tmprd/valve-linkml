# VALVE - LinkML
Convert LinkML schema + data to VALVE tables and vice versa.

## Setup
`python3 -m pip install .`

### Test schema conversion
```shell
python3 -m test.test_linkml2valve
# This will generate VALVE tables in the `test/valve_output` folder.
```

### Test VALVE validation
```shell
rm test/valve_output/personinfo.db
ontodev_valve test/valve_output/table.tsv test/valve_output/personinfo.db --verbose
# This will generate a personinfo.db in the `test/valve_output` folder.
```

### Test LinkML validation
```shell
# Validate TSV
linkml-validate --schema test/linkml_input/personinfo.yaml --target-class Person --index-slot id test/valve_output/data/Person.tsv
linkml-validate --schema test/linkml_input/personinfo.yaml --target-class Address --index-slot street test/valve_output/data/Address.tsv

linkml-validate --schema test/linkml_input/personinfo.yaml --target-class ProcedureConcept --index-slot id test/valve_output/data/ProcedureConcept.tsv
```

LinkML conversion for validating JSON
```shell
# Convert TSV to JSON
linkml-convert --schema test/linkml_input/personinfo.yaml --target-class Person --index-slot id test/valve_output/data/Person.tsv -o test/valve_output/data/Person.json
linkml-convert --schema test/linkml_input/personinfo.yaml --target-class Address --index-slot street test/valve_output/data/Address.tsv -o test/valve_output/data/Address.json

# Validate JSON
linkml-validate --schema test/linkml_input/personinfo.yaml --target-class Person test/valve_output/data/Person.json
linkml-validate --schema test/linkml_input/personinfo.yaml --target-class Address test/valve_output/data/Address.json
```