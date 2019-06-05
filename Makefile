.PHONY: default

default: init ;

setup:
	mkdir -p ./data
	mkdir -p ./data/mongo
	mkdir -p ./data/es
	mkdir -p ./data/tmp
	mkdir -p ./data/tmp/ml
	python setup.py develop

init: setup
	pip install -r requirements.txt

run:
	pecan serve config.py
