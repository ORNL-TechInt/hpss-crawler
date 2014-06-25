# - sitting a dir A, creating symlink in A/B for A/B/foo.py, symlink should be
#   A/B/foo -> foo.py, not A/B/foo -> B/foo.py

# running 'foo.py' should run the tests, whether or not the symlink exists.
# both ez_launch() and tf_launch() need a test for this

# running 'foo.py -L' should create the symlink. Both ez_launch() and
# tf_launch() need a test for this
