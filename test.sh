CWD=`pwd`
echo $CWD
LOCAL=${CWD}/local
LOCAL_SITE_PKG=${LOCAL}/lib/python2.6/site-packages
export PYTHONPATH=".:${LOCAL_SITE_PKG}"
mkdir -p $LOCAL_SITE_PKG
easy_install --prefix ${CWD}/local virtualenv
export PATH=${PATH}:${LOCAL}/bin
VENV=${CWD}/hpssic
virtualenv ${VENV}
export PATH=${PATH}:${VENV}/bin
set +x
. ${VENV}/bin/activate
set -x
pip install -r jenkins_requirements.txt
which nosetests
nosetests -v -c test/nosecron.cfg hpssic/test

