mkdir -p env
virtualenv --no-site-packages env
source env/bin/activate
echo -e "\nvirtual environment successfully created\n"
pip install numpy
pip install -r requirements.pip
echo -e "requirements successfully installed\n"
