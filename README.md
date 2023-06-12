# VALVE - LinkML
Convert LinkML schema + data to VALVE tables and vice versa.

## Setup
`python3 -m pip install .`

### Test schema conversion
```shell
python3 -m test.test_linkml2valve
```

### Test VALVE validation
```shell
valve.py test/linkml2valve/valve/table.tsv test/linkml2valve/valve/personinfo.db
```

### Test LinkML validation
```shell
# Validate TSV
linkml-validate --schema test/linkml2valve/linkml/personinfo.yaml --target-class Person --index-slot id test/linkml2valve/valve/data/Person.tsv
linkml-validate --schema test/linkml2valve/linkml/personinfo.yaml --target-class Address --index-slot street test/linkml2valve/valve/data/Address.tsv
```

LinkML conversion for validating JSON
```shell
# Convert TSV to JSON
linkml-convert --schema test/linkml2valve/linkml/personinfo.yaml --target-class Person --index-slot id test/linkml2valve/valve/data/Person.tsv -o test/linkml2valve/valve/data/Person.json
linkml-convert --schema test/linkml2valve/linkml/personinfo.yaml --target-class Address --index-slot street test/linkml2valve/valve/data/Address.tsv -o test/linkml2valve/valve/data/Address.json

# Validate JSON
linkml-validate --schema test/linkml2valve/linkml/personinfo.yaml --target-class Person test/linkml2valve/valve/data/Person.json
linkml-validate --schema test/linkml2valve/linkml/personinfo.yaml --target-class Address test/linkml2valve/valve/data/Address.json
```