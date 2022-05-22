sudo apt-get update
sudo apt install python3-pip
pip3 install jupyter
sudo apt-get install default.jre
python3 -m pip install --upgrade pip
sudo apt-get install scala
pip3 install py4j
wget https://archive.apache.org/dist/spark/spark-3.0.0/spark-3.0.0-bin-hadoop3.2.tgz
sudo tar -xzvf spark-3.0.0-bin-hadoop3.2.tgz
pip3 install findspark
mkdir certs
cd certs
sudo openssl req -x509 -nodes -days 365 -newkey rsa:2048 -keyout mycert.pem -out mycert.pem
(Press enter for every question that is asked)
sudo chown ubuntu:ubuntu mycert.pem (Replace with your user)
vi /home/ubuntu/.jupyter/jupyter_notebook_config.py 
Opens up VI editor) Add the following at the top & save the file 
c = get_config()
c.NotebookApp.certfile = u'/home/ubuntu/certs/mycert.pem' 
c.NotebookApp.ip = '*'
c.NotebookApp.open_browser = False
c.NotebookApp.port = 8888

pip install pandas
pip install matplotlib
vi ~/.bashrc
(Opens up VI editor) Add the following at the bottom and save
export SPARK_HOME=/home/ubuntu/spark-3.0.0-bin-hadoop3.2
export PATH=$SPARK_HOME/bin:$PATH
export PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH
export PATH=/usr/bin/python3:$PATH
export PYSPARK_PYTHON=/usr/bin/python3
source ~/.bashrc
