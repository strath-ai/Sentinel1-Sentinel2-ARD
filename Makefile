PY_SRC:=$(wildcard src/*.py)
SHELL_HELPERS:=$(wildcard shell/docker*.sh)

usage:
	@echo "Targets:"
	@echo
	@echo "build : Build docker image for running snap download and/or processing"
	@echo "helpers : copy shell helper scripts to /usr/local/bin"

build: Dockerfile ${PY_SRC}
	docker build --network=host -f Dockerfile -t senprep:latest --force-rm=True .

helpers: ${SHELL_HELPERS}
	@echo "Copying scripts from `pwd`/shell into /usr/local/bin"
	cp -i $^ /usr/local/bin/
