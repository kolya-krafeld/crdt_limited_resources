# CRDTs for limited Resources

This project realizes a hybrid CRDT system that handels both monotonic CRDTs and non-monotonic limited resource CRDTs across multiple nodes.
Limited resource CRDTs can be used to manage limited stock in e-commerce systems. They provide the eventual consistency benefits of CRDTs while ensuring that the system does not give away more resources than are available.

To ensure safty for the limited resource CRDTs a coordination phase is is implemented that redistributes resources across nodes.
A general system execution can be found here:
![crdt-limited-resources-coordination_clean drawio (3)](https://github.com/kolya-krafeld/crdt_limited_resources/assets/91055239/ec32e9ac-a428-4915-aa03-4d346b8fd61c)


## Run the system

There are 3 seperate ways to check the systems execution:
1. **BenchmarkTests**: Checkout the `BenchmarkTests.java` class and run the JUnit tests in it. They cover the **random**, **one node** & **always coordiantion** scenarios mentioned in the report. You can change the globale cariables at the top of the class to change the number of resources and nodes. The tests need to be run independantly because we are using the same socket ports in all tests.
2. **SystemTests**: Checkout the `SystemTests.java` class and run the JUnit tests in it. The test cover different scenarios of the system (failed follower nodes, failed leader nodes,  delayed messages in the coordianation phase etc). The tests need to be run independantly because we are using the same socket ports in all tests.
3. **Client**: Checkout the `Client.java` class and run the `main()` method in it. It spawns a specified number of nodes and allocates resources to the nodes. The client sends requests to the nodes regularly to reqeust resources. You can change the settings in the main-method to kill nodes during the execution or add message delay.

## Work split

Leon: Ballot Leader Ellection, Failure Detection, Testing
Kolya: CRDT coordination phase, Monotonic CRDTs, Benchmarking

Project for Distributed Sytems, Advanced course (ID2203) by: Leon Moll & Kolya Krafeld
