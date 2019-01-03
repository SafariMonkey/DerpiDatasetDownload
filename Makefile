setup:
	python3 -m venv devenv
	# requirements to install requirements
	devenv/bin/pip install requests wheel
	devenv/bin/pip install -r requirements.txt
fetch:
	devenv/bin/python3 fetch.py