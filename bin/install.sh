mkdir -p env
virtualenv --no-site-packages env
source env/bin/activate
pip install numpy
pip install -r requirements.pip

echo -e "\nvirtual environment successfully created\n"
echo -e "to start server: $python bamboo.py\n"
