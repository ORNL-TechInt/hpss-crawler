WWW = $(HOME)/www/hpssic
PYFILES = $(shell find . -name "*.py")
YMDHM = $(shell date +"%Y.%m%d.%H%M")
help:
	@echo ""
	@echo "Targets in this Makefile"
	@echo "    clean        Remove *.pyc, *~, test.d, MANIFEST, README"
	@echo "    cov          show and capture coverage report"
	@echo "    cron         run tests in a cronjob"
	@echo "    doc          Generate the documentation"
	@echo "    install      Install the package locally"
	@echo "    precommit    tests to run just before committing"
	@echo "    pristine     clean + rm dist/*"
	@echo "    readme       Generate the readme in HTML"
	@echo "    refman       Generate the Reference Manual in HTML"
	@echo "    sdist        Generate the source distribution"
	@echo "    TAGS         Navigation tags for emacs"
	@echo "    tests        Run and log the fast tests"
	@echo "    alltests     Run and log all the tests"
	@echo "    testcov      Run tests, show & capture coverage report"
	@echo "    uguide       Generate the User Guide in HTML"
	@echo "    uninstall    Remove hpssic from the current environment"
	@echo "    up           Re-install hpssic with the --upgrade flag"
	@echo ""

doc: readme refman uguide

pristine: clean
	rm dist/*

clean:
	find . -name "*.pyc" | xargs rm -f
	find . -name "*~" | xargs rm -f
	rm -rf test/test.d MANIFEST README

TAGS: hpssic/*.py hpssic/plugins/*.py hpssic/test/*.py
	find . -name "*.py" | xargs etags

TEST_OPTS=-x
TESTLOG="hpssic_test.log"
tests:
	py.test $(TEST_OPTS) 2>&1

alltests:
	py.test --all $(TEST_OPTS) 2>&1

testcov:
	py.test --cov hpssic --all 2>&1
	coverage report -m > coverage/$(YMDHM)

cov:
	coverage report -m > coverage/$(YMDHM)

cron:
	echo cronjob | at now

pep8:
	py.test -k pep8

precommit: alltests

readme: $(WWW)/README.html

refman: $(WWW)/ReferenceManual.html

uguide: $(WWW)/UserGuide.html

$(WWW)/README.html: README.md
	Markdown.pl $< > $@

$(WWW)/ReferenceManual.html: RefMan.md
	Markdown.pl $< > $@

$(WWW)/UserGuide.html: UGuide.md
	Markdown.pl $< > $@

sdist: $(PYFILES)
	python setup.py sdist

install:
	@if [[ `whoami` == "root" ]]; then \
        pip install .; \
    elif [[ `which python` == "/usr/bin/python" ]]; then \
		echo "Do '. ~/venv/hpssic/bin/activate' first"; \
	else \
		pip install .; \
	fi

up:
	@if [[ `whoami` == "root" ]]; then \
        pip install --upgrade .; \
    elif [[ `which python` == "/usr/bin/python" ]]; then \
		echo "Do '. ~/venv/hpssic/bin/activate' first"; \
	else \
		pip install --upgrade .; \
	fi

uninstall:
	@if [[ `whoami` == "root" ]]; then \
        pip uninstall hpssic; \
    elif [[ `which python` == "/usr/bin/python" ]]; then \
		echo "Do '. ~/venv/hpssic/bin/activate' first"; \
	else \
		pip uninstall hpssic; \
	fi
