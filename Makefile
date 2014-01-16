WWW = $(HOME)/www/hpss/hpssic

doc: readme refman uguide

clean:
	find . -name "*.pyc" | xargs rm
	find . -name "*~" | xargs rm
	rm -rf test.d

TAGS: *.py tests/*.py
	etags *.py

readme: $(WWW)/README.html

refman: $(WWW)/ReferenceManual.html

uguide: $(WWW)/UserGuide.html

$(WWW)/README.html: README.md
	Markdown.pl $< > $@

$(WWW)/ReferenceManual.html: RefMan.md
	Markdown.pl $< > $@

$(WWW)/UserGuide.html: UGuide.md
	Markdown.pl $< > $@
