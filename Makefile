WWW = $(HOME)/www/hpssic
PYFILES = $(shell find . -name "*.py")

doc: readme refman uguide

pristine: clean
	rm dist/*

clean:
	find . -name "*.pyc" | xargs rm -f
	find . -name "*~" | xargs rm -f
	rm -rf test/test.d MANIFEST

TAGS: 
	find . -name "*.py" | xargs etags

alltests:
	@echo "--------------------------------------------" >> $(TESTLOG)
	@date "+%Y.%m%d %H:%M:%S" >> $(TESTLOG)
	nosetests -c $(TEST_D)/nosecron.cfg $(TEST_D) 2>&1 | tee -a $(TESTLOG)
	@date "+%Y.%m%d %H:%M:%S" >> $(TESTLOG)

pep8:
	nosetests $(TEST_D)/test_script.py:Test_PEP8.test_pep8

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

