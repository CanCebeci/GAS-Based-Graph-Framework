This repository aims to be a simplified, non-distributed implementation of GraphLab PowerGraph's GAS abstraction for vertex-centric graph applications.

---
# Current Status
* The simple pagerank program runs correctly.

* The execution scheme of `async_engine` is currently very naive and inefficient. It is to be changed fundamentally soon.

* Many features (such as message passing and gather caching are missing).
---
A makefile is to be added in upcoming commits. For now, you can simply compile and run the sample program with
```
g++ -o main src/main.cpp 
./main
```
---
