1. Testing the local wt implementation
    1. Clone the project repo and build with the back up and copy
    
    3. 
2. Testing with the remote wt
    1. Install the dockerfile and the key pair for auth into the remote machine
    2. 
3. set up chron jobs for 
4. Agent harness connection and flow control 

* Session id is per harness started with a wt command
* Group id is managing the group of harness that could communicate with each other

* Consider the message queue:
1. The agent md to teach how the harness should use wt as the command line tool
2. WT send message should run for a long time and expect a response without needing to call recv again, claude code should monitor it in a subprocess until the response is ready
3. regarding how the messages are stored and fetched in the db
