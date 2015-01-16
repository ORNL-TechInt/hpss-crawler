python setup.py bdist_rpm
mkdir RPMS
cp dist/*.rpm RPMS
echo "hpssic-2015.0113.hf.6-1.noarch.rpm hpss generic6-x86_64" > destinations.txt
