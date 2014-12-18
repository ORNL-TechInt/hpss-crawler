pwd
env
echo "This is where we build the rpm"
python setup.py bdist_rpm
ls -al dist
exit 1
