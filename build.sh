pwd
env
echo "This is where we build the rpm"
python setup.py bdist_rpm
ls -al dist
mkdir artifact
cp dist/hpssic-2014.1217.4-1.noarch.rpm artifact
exit 0
