all: clean init

clean:
	rm -rf .venv/

init:
	python3 -m venv .venv
	.venv/bin/pip3 install -r requirements.txt
