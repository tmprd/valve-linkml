MAKEFLAGS += --warn-undefined-variables
SHELL := bash
.DEFAULT_GOAL := example_target
.DELETE_ON_ERROR:
.SUFFIXES:

.PHONY: clean example_target test_linkml2valve test_linkml_validation test_linkml_json_validation

example_target:
	python3 -m valve_linkml.linkml2valve "test/linkml_input/personinfo/personinfo.yaml" \
		-o test/valve_output

# If this doesn't work, don't worry ...
test_linkml_validation: test/valve_output/personinfo/data/Person.tsv
	linkml-validate --schema test/linkml_input/personinfo/personinfo.yaml \
		--target-class Person $<

test_linkml_json_validation: test/valve_output/personinfo/data/Person.json
	linkml-validate --schema test/linkml_input/personinfo/personinfo.yaml --target-class Person $<

test_linkml2valve:
	python3 -m test.test_linkml2valve

test/valve_output/personinfo/data/Person.json: test/valve_output/personinfo/data/Person.tsv
	linkml-convert --schema test/linkml_input/personinfo/personinfo.yaml \
		--target-class Person --index-slot id -o $@ \
		$<

test/valve_output/personinfo/data/Person.tsv: test/valve_output/personinfo/data/Person.json
	linkml-convert --schema test/linkml_input/personinfo/personinfo.yaml \
		--target-class Person --index-slot id -o $@ \
		$<

test/valve_output/personinfo/personinfo.db: test/valve_output/personinfo/data/Person.tsv
	RLTBL_CONNECTION=$@ ./rltbl --verbose init --force
	RLTBL_CONNECTION=$@ ./rltbl --verbose load table $< --force

clean:
	git checkout test
	rm -f test/valve_output/*.tsv
	rm -f test/valve_output/personinfo/data/Person.json
	rm -f test/valve_output/personinfo/data/Person_no_id.tsv
	rm -Rf test/valve_output/data
