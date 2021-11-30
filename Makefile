PY_SRC:=$(wildcard src/*.py)
SHELL_HELPERS:=$(wildcard shell/docker*.sh)

usage:
	@echo "Targets:"
	@echo
	@echo "senprep : python preprocessing flows for Sentinel ARD"
	@echo "helpers : copy shell helper scripts to /usr/local/bin"

senprep: Dockerfile-senprep ${PY_SRC}
	docker build --network=host -f Dockerfile-senprep -t senprep:latest --force-rm=True .

helpers: ${SHELL_HELPERS}
	@echo "Copying scripts from `pwd`/shell into /usr/local/bin"
	cp -i $^ /usr/local/bin/
