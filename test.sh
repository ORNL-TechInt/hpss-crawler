CWD=`pwd`
echo $CWD
echo $PYTHONPATH
export PYTHONPATH=${PYTHONPATH}:${CWD}/local/lib/python2.6/site-packages
easy_install --prefix ${CWD}/local virtualenv
export PATH=${PATH}:${CWD}/local/bin
virtualenv ${CWD}/hpssic
export PATH=${PATH}:${CWD}/hpssic/bin
. ${CWD}/hpssic/bin/activate
pip install pexpect
nosetests1.1 -c test/nosecron.cfg hpssic/test

