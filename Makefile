WWW = $(HOME)/www/hpssic
PYFILES = $(shell find . -name "*.py")

help:
	@echo ""
	@echo "Targets in this Makefile"
	@echo "    clean        Remove *.pyc, *~, test.d, MANIFEST, README"
	@echo "    doc          Generate the documentation"
	@echo "    install      Install the package locally"
	@echo "    pristine     clean + rm dist/*"
	@echo "    readme       Generate the readme in HTML"
	@echo "    refman       Generate the Reference Manual in HTML"
	@echo "    refresh      Regenerate the dist and install/upgrade the local copy"
	@echo "    sdist        Generate the source distribution"
	@echo "    TAGS         Navigation tags for emacs"
	@echo "    tests        Run and log the fast tests"
	@echo "    alltests     Run and log all the tests"
	@echo "    uguide       Generate the User Guide in HTML"
	@echo ""

doc: readme refman uguide

pristine: clean
	rm dist/*

clean:
	find . -name "*.pyc" | xargs rm -f
	find . -name "*~" | xargs rm -f
	rm -rf test/test.d MANIFEST README

TAGS: 
	find . -name "*.py" | xargs etags

TESTLOG=test/nosetests.log
NOSE_WHICH=test
tests:
	@echo "--------------------------------------------" >> $(TESTLOG)
	@date "+%Y.%m%d %H:%M:%S" >> $(TESTLOG)
	nosetests -c test/nose.cfg $(NOSE_WHICH) 2>&1 | tee -a $(TESTLOG)
	@date "+%Y.%m%d %H:%M:%S" >> $(TESTLOG)

pep8:
	nosetests $(TEST_D)/test_script.py:Test_PEP8.test_pep8

precommit: tests pep8

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
	@if [[ `which python` == "/usr/bin/python" ]]; then \
		echo "Do '. ~/venv/hpssic/bin/activate' first"; \
	else \
		pip install --upgrade dist/hpssic-2014.0725dev.tar.gz; \
	fi

refresh: sdist install

