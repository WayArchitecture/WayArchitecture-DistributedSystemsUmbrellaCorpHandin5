# WayArchitecture-DistributedSystemsUmbrellaCorpHandin5
Mandatory Activity 5

Repository: https://github.com/WayArchitecture/WayArchitecture-DistributedSystemsUmbrellaCorpHandin5

How to run:
To start, first run the action servers. To do this, go to the folder "auction_system\node" and run "node.exe". This starts 1 server node. 
To start multiple nodes, simply run "node.exe" again. The node cap is hard coded at 5, meaning that a maximum of 5 nodes can be created at the same time.

When an auction server has started, clients can connect to the auction. 
To create a client, simply go to the folder "auction_system\client" and run "client.exe". This starts 1 client.
To start multiple clients, simply run "client.exe" again.

A client can perform 2 different actions, these being bidding and showing the results.
These are called by writting either 'bid ' followed by an amount or 'result'.