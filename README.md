# Relatable - LinkML
Convert [LinkML](https://github.com/linkml/linkml) schemas + data to [Relatable](https://github.com/rltbl/relatable) tables

## Requirements
* [Relatable](https://github.com/rltbl/relatable)
* [LinkML](requirements.txt)

## Relatable setup

**TODO.**


## LinkML Setup

1. Clone the repository.

        git clone git@github.com:lmcmicu/rltbl_linkml.git

2. Navigate to the just-cloned repository's root directory.

        cd rltbl_linkml

3. Create a python virtual environment in the root directory of the repository.

        python3 -m venv _venv

4. Activate the newly-created virtual environment.

        . _venv/bin/activate

5. Install the python requirements to the virtual environment (this might take a few minutes).

        pip3 install -r requirements.txt

## Show usage

    python3 -m rltbl_linkml.linkml2rltbl --help

### Test schema conversion
```shell
python3 -m test.test_linkml2rltbl
# This will generate rltbl tables in the `test/rltbl_output` folder.
```

### Test `rltbl` validation
```shell
# PersonInfo - this will generate a rltbl sqlite db in the specified location
rm test/rltbl_output/personinfo/personinfo.db
./rltbl test/rltbl_output/personinfo/table.tsv test/rltbl_output/personinfo/personinfo.db --verbose
# 

# Biolink
rm test/rltbl_output/biolink/biolink.db
./rltbl test/rltbl_output/biolink/table.tsv test/rltbl_output/biolink/biolink.db --verbose
```

### Test LinkML validation
TODO: The --index-slot argument no longer exists in the new version of linkml. Is it needed below? What's the equivalent?
Note that it may be a typo, since it is a valid argument to linkml-convert (see below).

```shell
# Validate TSV
linkml-validate --schema test/linkml_input/personinfo/personinfo.yaml --target-class Person --index-slot id test/rltbl_output/personinfo/data/Person.tsv
linkml-validate --schema test/linkml_input/personinfo/personinfo.yaml --target-class Address --index-slot street test/rltbl_output/personinfo/data/Address.tsv
linkml-validate --schema test/linkml_input/personinfo/personinfo.yaml --target-class ProcedureConcept --index-slot id test/rltbl_output/personinfo/data/ProcedureConcept.tsv
```

#### LinkML conversion for validating JSON
```shell
# Convert TSV to JSON
linkml-convert --schema test/linkml_input/personinfo/personinfo.yaml --target-class Person --index-slot id test/rltbl_output/personinfo/data/Person.tsv -o test/rltbl_output/personinfo/data/Person.json
linkml-convert --schema test/linkml_input/personinfo/personinfo.yaml --target-class Address --index-slot street test/rltbl_output/personinfo/data/Address.tsv -o test/rltbl_output/personinfo/data/Address.json

# Validate JSON
linkml-validate --schema test/linkml_input/personinfo/personinfo.yaml --target-class Person test/rltbl_output/personinfo/data/Person.json
linkml-validate --schema test/linkml_input/personinfo/personinfo.yaml --target-class Address test/rltbl_output/personinfo/data/Address.json
```
