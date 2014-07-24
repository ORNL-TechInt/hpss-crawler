CWD=`pwd`
echo $CWD
echo $HOME
#
# This is where we're going to put our local (non-system) stuff.
# First, we need to get virtualenv.
LOCAL=${CWD}/local
LOCAL_SITE_PKG=${LOCAL}/lib/python2.6/site-packages
export PYTHONPATH=".:${LOCAL_SITE_PKG}"
mkdir -p $LOCAL_SITE_PKG
easy_install --prefix ${CWD}/local virtualenv
export PATH=${PATH}:${LOCAL}/bin

#
# Next, we set up a virtual environment where we can install all our
# dependencies. We put it in local, where easy_install just put
# virtualenv itself, so we keep everything together.
VENV=${CWD}/local
virtualenv ${VENV}

#
# and activate it
set +x
. ${VENV}/bin/activate
set -x

#
# Install the dependencies. We don't install ibm_db or mysql-python
# since they wouldn't do us any good anyhow. They depend on other
# pieces of db2 and mysql that are not on this machine. Our tests are
# decorated so that tests that depend on db2 or mysql will be skipped.
pip install -r jenkins_requirements.txt

#
# crawl.cfg.sample is set up with sqlite specified in its [dbi]
# section, so deploying it will allow us to run the sqlite DBI tests.
cp crawl.cfg.sample crawl.cfg

#
# And run the tests! 
nosetests -v -c hpssic/test/nose_jenkins.cfg hpssic/test
