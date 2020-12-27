#include <iostream>
#include <fstream>
#include <unordered_set>
#include <cstdlib>
#include <ctime>
using namespace std;

const int max_nodes = 200;
const int min_nodes = 199;
const double max_neighs_proportion = 0.4;
const int max_weight = 50;

int main() {
    ofstream graph_file("generated_graph_SSSP.txt");
    if (!graph_file.is_open()) {
        cout << "Unable to open file" << endl;
        return -1;
    }


    srand(time(NULL));    // seed rand()
    const int num_nodes = min_nodes + (rand() % (max_nodes - min_nodes));
    cout << "num nodes: " << num_nodes << endl;
    const int max_neighs = max_nodes * max_neighs_proportion;

    long int num_edges = 0;

    for (int i = 0; i < num_nodes; i++) {
        graph_file << i;

        const int num_neighs = 1 + (rand() % max_neighs);   // a node never has 0 out-neighbours
        unordered_set<int> neighs;
        for (int j = 0; j < num_neighs; j++) {
            int roll = rand() % num_nodes;
            // re-roll if the roll is already a neighbour or i itself.
            while (roll == i || neighs.count(roll) > 0) {
                roll = rand() % num_nodes;
            }
            neighs.insert(roll);
            int weight = (rand() % max_weight) + 1;
            graph_file << " " << roll << " " << weight;
            num_edges++;
        }
        graph_file << endl;
    }
    cout << "num edges: " << num_edges << endl;
    graph_file.close();
}