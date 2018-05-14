* Simple Google File System *

Designed a distributed system with multiple clients and server, and a metadata server. Metadata server maintains metadata about files in the file system and is also responsible to keep track of all alive servers. A file is made up of multiple chunks of fixed size and each chunk has 3 replicas in 3 different servers. 
Clients can perform following operations:
1. Read from a chunk to be performed from any alive replicas.
2. Append data to all live replicas of the chunks using two-phase commit protocol.
3. Create new file in any 3 servers selected by metadata server at random.

## Running the Code:  
Before running the code, please update the below mentioned details in configuration file:
* serverport (port for running the server)
* clientport (port for running the client)
* noofrequests ( # of operation to perform by a single)
* server address and it’s directory
* clients address

**Required Argument:** <configuration_file_path> <metadata_server/server/client> <server (1-3) or client (1-5) ID> <br />

**Metadata Server:** java -jar aos_project.jar config.properties metadata_server 1 <br />
**Server:** java -jar aos_project.jar config.properties server 1<br />
**Client:** java -jar aos_project.jar config.properties client 1
