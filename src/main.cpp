#include <iostream>
#include <cmath>
#include "simple_graph.hpp"
#include "async_engine.hpp"

#include "graphlab/graphlab.hpp"

using namespace std;

typedef Graph<double, graphlab::empty> graph_type;

class pagerank_program :
             public graphlab::ivertex_program<graph_type, double> {

private:
  // a variable local to this program
  double delta;
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
    delta = newval - prevval;
  }
  
  // The scatter edges depend on whether the pagerank has converged 
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
    g.add_vertex(1, 1.0);
    g.add_vertex(2, 1.0);
    g.add_vertex(3, 1.0);

    g.add_edge(1, 2);
    g.add_edge(1, 3);
    g.add_edge(2, 3);
    g.add_edge(3, 2);

    async_engine<pagerank_program> engine(g, true); // the second argument enables gather caching.
    engine.signal_all();
    engine.start();

    for (int i = 1; i <= 3; i++) {
      cout << "pagerank of vertex " << i << ": " << g.vertex(i).data() << endl;  
    }
}