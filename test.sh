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
# hpssic needs a default place to put log files and such like
export CRAWL=${CWD}
mkdir -p ${CRAWL}/work

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
pip install .

#
# crawl.cfg.sample is set up with sqlite specified in its [dbi]
# section, so deploying it will allow us to run the sqlite DBI tests.
cp crawl.cfg.sample crawl.cfg

#
# Touch jenkins to let the tests know that's where we're running so
# py.test will skip tests marked '@jenkins_fail'
touch jenkins

#
# look and see what's in the current dir
ls -al
py.test --version

#
# And run the tests! 
py.test hpssic --skip mysql --skip db2 --cov hpssic
