# CRDTs for limited Resources

This project realizes a hybrid CRDT system that handels both monotonic CRDTs and non-monotonic limited resource CRDTs across multiple nodes.
Limited resource CRDTs can be used to manage limited stock in e-commerce systems. They provide the eventual consistency benefits of CRDTs while ensuring that the system does not give away more resources than are available.

To ensure safty for the limited resource CRDTs a coordination phase is is implemented that redistributes resources across nodes.
A general system execution can be found here:
![crdt-limited-resources-coordination_clean drawio (3)](https://github.com/kolya-krafeld/crdt_limited_resources/assets/91055239/ec32e9ac-a428-4915-aa03-4d346b8fd61c)


## Run the system

Check the `Client.java` class to see how to run Nodes and the Client locally and send requests from the client.
Run the `Client::main` to execute a test setup of the system.

## Work split

Leon: Ballot Leader Ellection, Failure Detection, Testing
Kolya: CRDT coordination phase, Monotonic CRDTs, Benchmarking

Project for Distributed Sytems, Advanced course (ID2203) by: Leon Moll & Kolya Krafeld
