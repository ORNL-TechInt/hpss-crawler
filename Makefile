clean:
	find . -name "*.pyc" | xargs rm
	find . -name "*~" | xargs rm
	rm -rf test.d

