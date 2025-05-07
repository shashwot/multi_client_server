# Run Server

python3 main.py

# Run Client

#### Syntax
python3 client.py $host $port <file_or_folder_path>

#### File
python3 client.py 127.0.0.1 51234 test-workload/client_1.txt

#### Folder
python3 client.py 127.0.0.1 51234 test-workload/

OR

for i in {1..10}; do python3 client.py localhost 51234 test-workload/client_$i.txt &; done
