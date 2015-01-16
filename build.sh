set -x
python setup.py bdist_rpm

mkdir RPMS
cp dist/*.rpm RPMS

export REPO_DIR=hpss
export REPO_ARCH=generic6-x86_64
cd RPMS; export RPM_NAME=`ls *noarch.rpm | head -1`; cd ..

env

if [[ "$GIT_BRANCH" == "master" ]]; then
    echo "$RPM_NAME $REPO_DIR $REPO_ARCH" > destinations.txt
fi
