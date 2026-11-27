MAKEFLAGS += --warn-undefined-variables
SHELL := bash
.DEFAULT_GOAL := test_schema_load
.DELETE_ON_ERROR:
.SUFFIXES:

.PHONY: test_schema_load

test_schema_load:
	python3 -m test.test_linkml2rltbl
	cd ../relatable && cargo build && cd -
	RUST_BACKTRACE=1 ./rltbl --verbose init --force test/rltbl_output/personinfo/table.tsv
	@echo -n "Database initialized. Sleeping for 3s before loading personinfo data ..."
	@sleep 3
	@echo
	RUST_BACKTRACE=1 ./rltbl --verbose load table --force test/rltbl_output/personinfo/data/Address.tsv
	RUST_BACKTRACE=1 ./rltbl --verbose load table --force test/rltbl_output/personinfo/data/MedicalEvent.tsv
	RUST_BACKTRACE=1 ./rltbl --verbose load table --force test/rltbl_output/personinfo/data/Person.tsv
	sqlite3 -header .relatable/relatable.db \
	"SELECT \
		m.procedure, \
		m.started_at_time, \
		m.ended_at_time, \
		p.name, \
		p.primary_email, \
		a.street, \
		a.city, \
		a.postal_code \
	FROM MedicalEvent m, Person p, Address a \
	WHERE \
		m.person = p.id \
		AND p.current_address = a.id \
		AND m.started_at_time >= '2020-01-01' \
		AND m.ended_at_time <= '2020-12-31' \
	ORDER BY \
		m.procedure, \
		m.started_at_time, \
		m.ended_at_time, \
		p.name, \
		p.primary_email, \
		a.street, \
		a.city, \
		a.postal_code" | tee /tmp/test_schema_load.out
	diff --strip-trailing-cr -q test/expected/test_schema_load.out /tmp/test_schema_load.out


#####################
# Linkml-specific targets.
#####################

.PHONY: test_linkml_validation test_linkml_json_validation

test_linkml_validation: test/rltbl_output/personinfo/data/Person.tsv
	linkml-validate --schema test/linkml_input/personinfo/personinfo.yaml \
		--target-class Person $<

test_linkml_json_validation: test/rltbl_output/personinfo/data/Person.json
	linkml-validate --schema test/linkml_input/personinfo/personinfo.yaml --target-class Person $<

test/rltbl_output/personinfo/data/Person.json: test/rltbl_output/personinfo/data/Person.tsv
	linkml-convert --schema test/linkml_input/personinfo/personinfo.yaml \
		--target-class Person --index-slot id -o $@ \
		$<

test/rltbl_output/personinfo/data/Person.tsv: test/rltbl_output/personinfo/data/Person.json
	linkml-convert --schema test/linkml_input/personinfo/personinfo.yaml \
		--target-class Person --index-slot id -o $@ \
		$<

.PHONY: clean

clean:
	git checkout test/rltbl_output
	rm -f test/rltbl_output/*.tsv .relatable/relatable.db
	rm -f test/rltbl_output/personinfo/data/Person.json
	rm -Rf test/rltbl_output/data
