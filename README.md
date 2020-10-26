This repository aims to be a simplified, non-distributed implementation of GraphLab PowerGraph's GAS abstraction for vertex-centric graph applications.

---
# Current Status
* The simple pagerank program runs correctly.

* The execution scheme of `async_engine` is currently very naive. It may be changed fundamentally soon.

* Some features (such as message passing) are missing.
---
A makefile is to be added in upcoming commits. For now, you can simply compile and run the sample program with
```
g++ -o main src/main.cpp 
./main
```
---
