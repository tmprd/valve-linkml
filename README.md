# VALVE - LinkML
Convert [LinkML](https://github.com/linkml/linkml) schemas + data to [VALVE](https://github.com/ontodev/valve.py) tables

## Requirements
* [LinkML](requirements.txt)
* [VALVE.py v0.2.1](https://github.com/ontodev/valve.py/releases/tag/v0.2.1) (requires Python 3.8) or [VALVE.rs v0.2.1](https://github.com/ontodev/valve.rs/releases/tag/v0.2.1)

## Setup
`python3 -m pip install .`

## Usage
`python3 -m valve_linkml.linkml2valve <linkml-yaml-schema-path> -o <output-directory> -d <linkml-yaml-data-directory>`

### Test schema conversion
```shell
python3 -m test.test_linkml2valve
# This will generate VALVE tables in the `test/valve_output` folder.
```

### Test VALVE validation
```shell
# PersonInfo - this will generate a VALVE sqlite db in the specified location
rm test/valve_output/personinfo/personinfo.db
ontodev_valve test/valve_output/personinfo/table.tsv test/valve_output/personinfo/personinfo.db --verbose
# 

# Biolink
rm test/valve_output/biolink/biolink.db
ontodev_valve test/valve_output/biolink/table.tsv test/valve_output/biolink/biolink.db --verbose
```

### Test LinkML validation
```shell
# Validate TSV
linkml-validate --schema test/linkml_input/personinfo/personinfo.yaml --target-class Person --index-slot id test/valve_output/personinfo/data/Person.tsv
linkml-validate --schema test/linkml_input/personinfo/personinfo.yaml --target-class Address --index-slot street test/valve_output/personinfo/data/Address.tsv
linkml-validate --schema test/linkml_input/personinfo/personinfo.yaml --target-class ProcedureConcept --index-slot id test/valve_output/personinfo/data/ProcedureConcept.tsv
```

LinkML conversion for validating JSON
```shell
# Convert TSV to JSON
linkml-convert --schema test/linkml_input/personinfo/personinfo.yaml --target-class Person --index-slot id test/valve_output/personinfo/data/Person.tsv -o test/valve_output/personinfo/data/Person.json
linkml-convert --schema test/linkml_input/personinfo/personinfo.yaml --target-class Address --index-slot street test/valve_output/personinfo/data/Address.tsv -o test/valve_output/personinfo/data/Address.json

# Validate JSON
linkml-validate --schema test/linkml_input/personinfo/personinfo.yaml --target-class Person test/valve_output/personinfo/data/Person.json
linkml-validate --schema test/linkml_input/personinfo/personinfo.yaml --target-class Address test/valve_output/personinfo/data/Address.json
```