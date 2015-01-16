python setup.py bdist_rpm

mkdir RPMS
cp dist/*.rpm RPMS

export REPO_DIR=hpss
export REPO_ARCH=generic6-x86_64
cd RPMS; export RPM_NAME=`ls *noarch.rpm | head -1`; cd ..

echo "$RPM_NAME $REPO_DIR $REPO_ARCH" > destinations.txt
