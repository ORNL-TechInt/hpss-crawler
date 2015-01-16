python setup.py bdist_rpm

mkdir RPMS
cp dist/*.rpm RPMS

cd RPMS; export RPM_NAME=`ls *noarch.rpm | head -1`; cd ..
export REPO_DIR=hpss
export REPO_ARCH=generic6-x86_64
export RPM_NAME=`ls RPMS/*noarch.rpm`
echo "$RPM_NAME $REPO_DIR $REPO_ARCH" > destinations.txt
