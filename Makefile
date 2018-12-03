.PHONY: default

default: init ;

setup:
	mkdir -p ./data
	mkdir -p ./data/mongo
	mkdir -p ./data/es

init:
	pip install -r requirements.txt

run:
	cd app; python main.py
