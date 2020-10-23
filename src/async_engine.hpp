#ifndef __ASYNC_ENGINE_H
#define __ASYNC_ENGINE_H

#include "simple_graph.hpp"
#include "graphlab/vertex_program/ivertex_program.hpp"
#include "graphlab/vertex_program/context.hpp"

#include <unordered_set>
#include <type_traits>  //for is_base_of
#include <iostream>

template<typename VertexProgram>
class async_engine {
public:
    // -- below are typedefs borrowed from GraphLab's synchronous_engine.hpp
    typedef VertexProgram vertex_program_type;
    typedef typename VertexProgram::gather_type gather_type;
    typedef typename VertexProgram::message_type message_type;
    typedef typename VertexProgram::vertex_data_type vertex_data_type;
    typedef typename VertexProgram::edge_data_type edge_data_type;
    typedef typename VertexProgram::graph_type  graph_type;
    typedef typename graph_type::vertex_type vertex_type;
    typedef typename graph_type::edge_type edge_type;
    typedef typename graph_type::vertex_id_type vertex_id_type;
    typedef graphlab::context<async_engine> context_type;

    graph_type& g;

    std::unordered_set<vertex_id_type> activeList;  // possibly replace later with better data structure

    async_engine(graph_type& g): g(g) {
        if (!std::is_base_of<graphlab::ivertex_program<graph_type, gather_type>, VertexProgram>::value) {
            throw "type parameter for async egnine is not derived from graphlab::ivertex_program";
        }
    }

    void signal_all();
    void start();

    // called by context
    void internal_signal(const vertex_type& vertex);
};

/**
 * implementation file (previously async_engine.cpp) copy-pasted below. Otherwise, template functions do not work
 */

using namespace std;

template<typename VertexProgram>
void async_engine<VertexProgram>::internal_signal(const vertex_type& vertex) {
    if (activeList.count(vertex.id()) == 0) {
        activeList.insert(vertex.id());
    }
}

template<typename VertexProgram>
void async_engine<VertexProgram>::signal_all() {
    for (typename unordered_map<vertex_id_type, vertex_type*>::iterator iter = g.vertices.begin(); iter != g.vertices.end(); iter++) {
        if (activeList.count(iter->first) == 0) {
            activeList.insert(iter->first);
        }
    }
}

/**
 * Naive implementation. Executes each vertex program in isolation.
 */
template<typename VertexProgram>
void async_engine<VertexProgram>::start() {
    /**
     * TODO: not sure if this is the correct place to instantiate a context. Refer to graphlab.
     */
    context_type context(*this, g);

    while (!activeList.empty()) {
        // instantiate vertex program
        VertexProgram vprog;
        vertex_id_type cur_vid = *(activeList.begin());
        activeList.erase(cur_vid);
        vertex_type& cur = g.vertex(cur_vid);

        // init() phase is skipped for now along with anything message passsing-related.

        /**
         * -----  GATHER PHASE  -----  
         */
        list<edge_type *> gather_edges;

        /**
         *  adapter from the gather_edges enum to actual lists of edge pointers
         *  probably will not be needed in less naive implementationsi
         */
        switch (vprog.gather_edges(context, cur)) {
        case graphlab::NO_EDGES:
            gather_edges = list<edge_type*>();
            break;
        case graphlab::IN_EDGES:
            gather_edges = cur.in_edges;
            break;
        case graphlab::OUT_EDGES:
            gather_edges = cur.out_edges;
            break;
        case graphlab::ALL_EDGES:
            gather_edges = cur.in_edges;
            gather_edges.insert(gather_edges.end(), cur.in_edges.begin(), cur.in_edges.end());
            break;
        }

        /** TODO: replace with templatized gather type */
        gather_type acc = 0;    // will not work when gather type is not numerical.    
        
        for (edge_type* e : gather_edges) {
            acc += vprog.gather(context, cur, *e);
        }

        /**
         * -----  APPLY PHASE  -----
         */
        vprog.apply(context, cur, acc);

        /**
         * -----  SCATTER PHASE  -----
         */
        list<edge_type *> scatter_edges;

        /**
         *  adapter from the scatter_edges enum to actual lists of edge pointers
         *  probably will not be needed in less naive implementationsi
         */
        switch (vprog.scatter_edges(context, cur)) {
        case graphlab::NO_EDGES:
            scatter_edges = list<edge_type*>();
            break;
        case graphlab::IN_EDGES:
            scatter_edges = cur.in_edges;
            break;
        case graphlab::OUT_EDGES:
            scatter_edges = cur.out_edges;
            break;
        case graphlab::ALL_EDGES:
            scatter_edges = cur.in_edges;
            scatter_edges.insert(gather_edges.end(), cur.in_edges.begin(), cur.in_edges.end());
            break;
        }

        for (edge_type* e : scatter_edges) {
            vprog.scatter(context, cur, *e);
        }
    }

    cout << "Engine has finished running.." << endl;
}

#endif