clean:
	rm -rf devenv
setup:
	python3 -m venv devenv
	# requirements to install requirements
	devenv/bin/pip install requests wheel
	devenv/bin/pip install -r requirements.txt
fetch:
	devenv/bin/python3 fetch.py
fetch-debug:
	DEBUG=true devenv/bin/python3 fetch.py
fetch-scratch:
	DERPIDL_DATA_PATH=./scratch-data devenv/bin/python3 fetch.py
fetch-scratch-debug:
	DERPIDL_DATA_PATH=./scratch-data DEBUG=true devenv/bin/python3 fetch.py
delete-scratch-data:
	rm -rf scratch-data/*