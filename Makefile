NAME=tendrl-gluster-integration
VERSION := $(shell PYTHONPATH=. python -c \
             'import version; print version.__version__' \
             | sed 's/\.dev[0-9]*//')
RELEASE=1
COMMIT := $(shell git rev-parse HEAD)
SHORTCOMMIT := $(shell echo $(COMMIT) | cut -c1-7)

all: srpm

clean:
	rm -rf dist/
	rm -rf $(NAME)-$(VERSION).tar.gz
	rm -rf $(NAME)-$(VERSION)-$(RELEASE).el7.src.rpm

dist:
	python setup.py sdist \
	  && mv dist/$(NAME)-$(VERSION).tar.gz .

srpm: dist
	fedpkg --dist epel7 srpm

rpm: dist
	mock -r epel-7-x86_64 rebuild $(NAME)-$(VERSION)-*.src.rpm --resultdir=. --define "dist .el7"

bzip-selinux-policy:
	@cd selinux; \
	rm -f *.pp.bz2 tmp; \
	make -f /usr/share/selinux/devel/Makefile; \
	bzip2 -9 tendrl.pp

gitversion:
	# Set version and release to the latest values from Git
	sed -i $(NAME).spec \
	  -e "/^Release:/cRelease: $(shell date +"%Y%m%dT%H%M%S").$(SHORTCOMMIT)"

snapshot: gitversion srpm


.PHONY: dist rpm srpm gitversion snapshot
