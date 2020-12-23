This repository aims to be a simplified, non-distributed implementation of GraphLab PowerGraph's GAS abstraction for vertex-centric graph applications.

---
A makefile is to be added soon. For now, you can test the framework with the following commands:

Compile and run the sample PageRank program with
```
g++ -o input_gen_pr src/sample_programs/testing_tools/input_generator_pagerank.cpp 
g++ -pthread -o pagerank src/pagerank.cpp
./input_gen_pr
./pagerank
```

Similarly, compile and run the sample Single Source Shortest Path program with
```
g++ -o input_gen_SSSP src/sample_programs/testing_tools/input_generator_SSSP.cpp 
g++ -pthread -o SSSP src/SSSP.cpp
./input_gen_SSSP
./SSSP
```
---
