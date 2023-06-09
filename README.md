# VALVE - LinkML
Convert LinkML schema + data to VALVE tables and vice versa.

## Setup
`python3 -m pip install .`

## Usage
```shell
python3 -m valve_linkml.linkml2valve test/linkml2valve/schema/personinfo.yaml -d test/linkml2valve/data
```

### Test
```shell
python3 -m test.test_linkml2valve
```