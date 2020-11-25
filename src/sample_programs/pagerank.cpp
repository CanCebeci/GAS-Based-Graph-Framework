#include <iostream>
#include <cmath>
#include <fstream>
#include <sstream>
#include "../GAS_framework/simple_graph.hpp"
#include "../GAS_framework/async_engine.hpp"
#include "../graphlab/graphlab.hpp"

using namespace std;

typedef Graph<double, graphlab::empty> graph_type;

const string in_graph_filename = "generated_graph_pagerank.txt";
const string out_filename = "pagerank_output.txt";

class pagerank_program :
             public graphlab::ivertex_program<graph_type, double> {

private:
  // a variable local to this program
  double delta;
public:
  edge_dir_type gather_edges(icontext_type& context, const vertex_type& vertex) const {
    return graphlab::IN_EDGES;
  }
  double gather(icontext_type& context, const vertex_type& vertex,
               edge_type& edge) const {
    return edge.source().data() / edge.source().num_out_edges();
  }

  // Use the total rank of adjacent pages to update this page 
  void apply(icontext_type& context, vertex_type& vertex,
             const gather_type& total) {
    double newval = total * 0.85 + 0.15;
    double prevval = vertex.data();
    vertex.data() = newval;
    delta = newval - prevval;
  }
  
  edge_dir_type scatter_edges(icontext_type& context, const vertex_type& vertex) const {
    return graphlab::OUT_EDGES;
  }

  void scatter(icontext_type& context, const vertex_type& vertex,
               edge_type& edge) const {
    context.post_delta(edge.target(), delta / vertex.num_out_edges());
    if ((std::fabs(delta) > 1E-3)) {
      context.signal(edge.target());
    }
  }
};

int main() { 
    graph_type g;

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
      ss >> cur_vid;
      g.add_vertex(cur_vid, 1.0);
      while (ss >> neigh_vid) {
        if (line_no < neigh_vid) {
          // need to add neighbour here so that add_edge does not fail.
          g.add_vertex(neigh_vid, 1.0);
        }
        g.add_edge(cur_vid, neigh_vid);
      }
      line_no++;
    }
    input_graph.close();

    // --- execute program
    async_engine<pagerank_program> engine(g, true); // the second argument enables gather caching.
    engine.signal_all();
    engine.start();

    // --- write the output file
    ofstream out_file(out_filename);
    if (!out_file.is_open()) {
        cout << "Unable to open output file" << endl;
        return -1;
    }

    for (int i = 0; i < g.num_vertices(); i++) {
        out_file << i << "\t" << g.vertex(i).data() << endl;
    }
    out_file.close();
}