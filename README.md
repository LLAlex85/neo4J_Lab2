# iteso-o2022-bda-neo4j

A place to share neo4j app code

### Setup a python virtual env with python neo4j installed
```
# If pip is not present in you system
sudo apt update
sudo apt install python3-pip

# Install and activate virtual env
python3 -m pip install virtualenv
virtualenv -p python3 ./venv
source ./venv/bin/activate

# Install project python requirements
python3 -m pip install -r requirements.txt
```

### To load data
Ensure you have a running neo4j instance
i.e.:
```
docker run -d --name=neo4j --publish=7474:7474 --publish=7687:7687 neo4j
```
Run main.py
i.e.:
```
python3 main.py
```
