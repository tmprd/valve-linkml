# VALVE - LinkML
Convert LinkML schema + data to VALVE tables and vice versa.

## Setup
`python3 -m pip install .`

### Test conversion
```shell
python3 -m test.test_linkml2valve
```

### Test validation
```shell
linkml-validate -f tsv -s test/linkml2valve/linkml/personinfo.yaml --index-slot=id test/linkml2valve/valve/data/Person.tsv
```