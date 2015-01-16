python setup.py bdist_rpm
mkdir RPMS
cp dist/*.rpm RPMS
ls -l ..
# REPO_DESTINATION=jenkins-built-for-testing
# REPO_DISTRO=generic6-x86_64
# copied to /ccs/siterepos/$REPO_DESTINATION/$REP_DISTRO
ls -al /ccs/siterepos/*
export REPO_DESTINATTION=hpss-generic6-x86_64
