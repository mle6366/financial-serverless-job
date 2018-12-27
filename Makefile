all: clean init test

clean:
	rm -rf .pytest_cache/
	rm -rf .venv/

init:
	python3 -m venv .venv
	.venv/bin/pip3 install -r requirements.txt
	.venv/bin/pip3 install pytest boto3==1.7.48

test:
ifneq ($(wildcard tests),)
	.venv/bin/python3 -m pytest tests/
else
	@echo "Did not find any tests . . ."
endif
	cd py_utils/; \
	../.venv/bin/python3 -m pytest --capture=no tests

deploy: test
	npm install
	serverless deploy
