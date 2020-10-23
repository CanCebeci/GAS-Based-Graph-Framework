#include <iostream>
#include <cmath>
#include "simple_graph.hpp"
#include "async_engine.hpp"

#include "graphlab/vertex_program/ivertex_program.hpp"

using namespace std;

typedef Graph<double, double> graph_type;

class pagerank_program :
             public graphlab::ivertex_program<graph_type, double> {

private:
  // a variable local to this program
  bool perform_scatter;

public:
  // no changes to gather_edges and gather
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
    perform_scatter = (std::fabs(prevval - newval) > 1E-3);
  }
  
  // The scatter edges depend on whether the pagerank has converged 
  edge_dir_type scatter_edges(icontext_type& context, const vertex_type& vertex) const {
    if (perform_scatter) return graphlab::OUT_EDGES;
    else return graphlab::NO_EDGES;
  }

  void scatter(icontext_type& context, const vertex_type& vertex,
               edge_type& edge) const {
    context.signal(edge.target());
  }
};

int main() { 
    graph_type g;
    g.add_vertex(1, 0.0);
    g.add_vertex(2, 0.0);
    g.add_vertex(3, 0.0);

    g.add_edge(1, 2, -1.0);
    g.add_edge(1, 3, -1.0);
    g.add_edge(2, 3, -1.0);
    g.add_edge(3, 2, -1.0);

    async_engine<pagerank_program> engine(g);
    engine.signal_all();
    engine.start();

    for (int i = 1; i <= 3; i++) {
      cout << "pagerank of vertex " << i << ": " << g.vertex(i).data() << endl;  
    }
}