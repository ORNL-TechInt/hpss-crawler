WWW = $(HOME)/www/hpssic

doc: readme refman uguide

clean:
	find . -name "*.pyc" | xargs rm
	find . -name "*~" | xargs rm
	rm -rf test.d

TAGS: *.py tests/*.py
	etags *.py

TESTLOG=tests/nosetests.log
test:
	@echo "--------------------------------------------" >> $(TESTLOG)
	@date "+%Y.%m%d %H:%M:%S" >> $(TESTLOG)
	nosetests -c tests/nose.cfg 2>&1 | tee -a $(TESTLOG)
	@date "+%Y.%m%d %H:%M:%S" >> $(TESTLOG)


readme: $(WWW)/README.html

refman: $(WWW)/ReferenceManual.html

uguide: $(WWW)/UserGuide.html

$(WWW)/README.html: README.md
	Markdown.pl $< > $@

$(WWW)/ReferenceManual.html: RefMan.md
	Markdown.pl $< > $@

$(WWW)/UserGuide.html: UGuide.md
	Markdown.pl $< > $@
