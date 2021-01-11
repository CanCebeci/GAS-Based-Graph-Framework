#include <iostream>
#include <fstream>
#include <sstream>
#include "../GAS_framework/simple_graph.hpp"
#include "../GAS_framework/async_engine.hpp"
#include "../graphlab/graphlab.hpp"

using namespace std;

const string in_graph_filename = "generated_graph_SSSP.txt";
const string out_filename = "SSSP_output.txt";


typedef long int vertex_data;
typedef long int edge_data;
typedef Graph<vertex_data, edge_data> graph_type;

struct min_container {
    int min;
    min_container(): min(0) { }
    min_container(int min): min(min) { }
    min_container &operator+=(const min_container &right) {
        if (min < 0 || (right.min > 0 && right.min < min)) {
            min = right.min;
        } 
        return *this;
    }
};

typedef min_container gather_type;

class SSSP_program :
             public graphlab::ivertex_program<graph_type, gather_type> {
private:
    bool do_scatter;
public:
    edge_dir_type gather_edges(icontext_type& context,
                             const vertex_type& vertex) const {
        return graphlab::IN_EDGES;
    }

    gather_type gather(icontext_type& context, const vertex_type& vertex,
               edge_type& edge) const {
	if (edge.source().data() >= 0) {
		return min_container(edge.source().data() + edge.data());
	}
	    return min_container(-1);
    }

    void apply(icontext_type& context, vertex_type& vertex,
             const gather_type& total) {
        if (total.min > 0 && (vertex.data() < 0 || vertex.data() > total.min)) {
            do_scatter = true;
            vertex.data() = total.min;
        } else {
            do_scatter = false;
        }
        
    }

    edge_dir_type scatter_edges(icontext_type& context,
                             const vertex_type& vertex) const {
        if (do_scatter) {
            return graphlab::OUT_EDGES;
        } else {
            return graphlab::NO_EDGES;
        }
    }

    void scatter(icontext_type& context, const vertex_type& vertex,
               edge_type& edge) const {
         context.signal(edge.target());
    }
};

int main(int argc, char** argv) {
    if (argc != 3) {
        cerr << "Wrong number of arguments" << endl;
        return -1;
    }

    int load_ahead_distance = atoi(argv[1]);
    int num_threads = atoi(argv[2]);


    graph_type graph;

    /**
     *  ---- Parse the input file ----
     * The parsing method used here is very specific. It only works with input
     * files that have the format used in the test I've run.
     */
    string line;
    ifstream input_graph(in_graph_filename);
    if (!input_graph.is_open()) {
      cout << "Can not open input file" << endl;
      return -1;
    }
    int line_no = 0;
    while (getline(input_graph, line)) {
      stringstream ss(line);
      int cur_vid;
      int neigh_vid;
      int edge_weight;
      ss >> cur_vid;
      graph.add_vertex(cur_vid, cur_vid == 0 ? 0 : -1);
      while (ss >> neigh_vid) {
        if (line_no < neigh_vid) {
          // need to add neighbour here so that add_edge does not fail.
          graph.add_vertex(neigh_vid, -1);
        }
        ss >> edge_weight;
        graph.add_edge(cur_vid, neigh_vid, edge_weight);
      }
      line_no++;
    }
    input_graph.close();

    // --- execute program
    async_engine<SSSP_program> engine(graph, load_ahead_distance, num_threads, false);
    engine.signal_all();
    engine.start();

    // --- write the output file
    ofstream out_file(out_filename);
    if (!out_file.is_open()) {
        cout << "Unable to open output file" << endl;
        return -1;
    }

    for (int i = 0; i < graph.num_vertices(); i++) {
        out_file << i << "\t" << graph.vertex(i).data() << endl;
    }
    out_file.close();

    cout << "SPM hits: " << engine.spm_hits << endl;
    cout << "SPM misses: " << engine.spm_misses << endl;
}