WWW = $(HOME)/www/hpss/hpssic

clean:
	find . -name "*.pyc" | xargs rm
	find . -name "*~" | xargs rm
	rm -rf test.d

TAGS: *.py tests/*.py
	etags *.py

doc: readme refman uguide

readme: $(WWW)/README.html

refman: $(WWW)/ReferenceManual.html

uguide: $(WWW)/UserGuide.html

$(WWW)/README.html: README.md
	Markdown.pl $< > $@

$(WWW)/ReferenceManual.html: RefMan.md
	Markdown.pl $< > $@

$(WWW)/UserGuide.html: UGuide.md
	Markdown.pl $< > $@
