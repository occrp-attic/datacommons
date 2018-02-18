MAPPINGS_DIR:=$(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))

all: \
	crawl.cd \
	crawl.ci \
	crawl.gn \
	crawl.ke \
	crawl.lr \
	crawl.mw \
	crawl.mz \
	crawl.na \
	crawl.pg \
	crawl.rw \
	crawl.ss \
	crawl.ug \
	crawl.zm

crawl.%:
	memorious run $*_flexicadastre