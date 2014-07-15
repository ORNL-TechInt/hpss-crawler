CWD=`pwd`
echo $CWD
export PYTHONPATH=${CWD}/local/lib/python2.6/site-packages
mkdir -p $PYTHONPATH
easy_install --prefix ${CWD}/local virtualenv
export PATH=${PATH}:${CWD}/local/bin
virtualenv ${CWD}/hpssic
export PATH=${PATH}:${CWD}/hpssic/bin
. ${CWD}/hpssic/bin/activate
pip install pexpect
nosetests1.1 -c test/nosecron.cfg hpssic/test

