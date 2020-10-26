#ifndef __ASYNC_ENGINE_H
#define __ASYNC_ENGINE_H

#include "simple_graph.hpp"
#include "graphlab/vertex_program/ivertex_program.hpp"
#include "graphlab/vertex_program/context.hpp"

#include <unordered_set>
#include <unordered_map>
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
    typedef graphlab::edge_dir_type edge_dir_type;

    graph_type& g;
    std::unordered_set<vertex_id_type> activeList;  // possibly replace later with better data structure

    bool caching_enabled;
    /**
     * GraphLab uses vectors instead of maps for gather_cache and has_cache,
     * which also enables the use of the special vector<graphlab::empty>
     * However, this implementation does not use local vertex id's and the global
     * vertex id's are not necessarily consecutive. Thus, a map is used instead.
     */
    std::unordered_map<vertex_id_type, gather_type> gather_cache;  
    std::unordered_map<vertex_id_type, bool> has_cache;             //          IMPORTANT: are bools default initialized to false?


    async_engine(graph_type& g, bool enable_caching = false): g(g), caching_enabled(enable_caching) {
        if (!std::is_base_of<graphlab::ivertex_program<graph_type, gather_type>, VertexProgram>::value) {
            throw "type parameter for async egnine is not derived from graphlab::ivertex_program";
        }
    }

    void signal_all();
    void start();

    // called by context
    void internal_signal(const vertex_type& vertex);
    void internal_post_delta(const vertex_type& vertex, const gather_type& delta);
    void internal_clear_gather_cache(const vertex_type& vertex);
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
void async_engine<VertexProgram>::
internal_post_delta(const vertex_type& vertex, const gather_type& delta) {
    if(caching_enabled && has_cache[vertex.id()]) {
        gather_cache[vertex.id()] += delta;
    }
}

template<typename VertexProgram>
void async_engine<VertexProgram>::
internal_clear_gather_cache(const vertex_type& vertex) {
    if(caching_enabled && has_cache[vertex.id()]) {
        has_cache[vertex.id()] == false;
    }
} // end of clear_gather_cache

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
        bool accum_is_set = false;
        gather_type accum = gather_type();  // imporant to explicitly call the default constructor for basic data types
                                            // when gather_type is double, int, bool etc...

        if (caching_enabled && has_cache[cur_vid]) {
            accum = gather_cache[cur_vid];
            accum_is_set = true;
        } else {
            const edge_dir_type gather_dir = vprog.gather_edges(context, cur);

            // Loop over in edges
            if (gather_dir == graphlab::IN_EDGES || gather_dir == graphlab::ALL_EDGES) {
                for (edge_type *e : cur.in_edges) {
                    if (accum_is_set) {
                        accum += vprog.gather(context, cur, *e);
                    } else {
                        accum = vprog.gather(context, cur, *e);
                        accum_is_set = true;
                    }
                }
            }
            // Loop over out edges
            if (gather_dir == graphlab::OUT_EDGES || gather_dir == graphlab::ALL_EDGES) {
                for (edge_type *e : cur.out_edges) {
                    if (accum_is_set) {
                        accum += vprog.gather(context, cur, *e);
                    } else {
                        accum = vprog.gather(context, cur, *e);
                        accum_is_set = true;
                    }
                }
            }
            // If caching is enabled then save the accumulator to the
            // cache for future iterations.  Note that it is possible
            // that the accumulator was never set in which case we are
            // effectively "zeroing out" the cache.
            if(caching_enabled && accum_is_set) {
                gather_cache[cur_vid] = accum; 
                has_cache[cur_vid] = true;
            }
        }

        /**
         * -----  APPLY PHASE  -----
         */
        vprog.apply(context, cur, accum);

        /**
         * -----  SCATTER PHASE  -----
         */
        const edge_dir_type scatter_dir = vprog.scatter_edges(context, cur);
        // Loop over in edges
        if(scatter_dir == graphlab::IN_EDGES || scatter_dir == graphlab::ALL_EDGES) {
            for (edge_type *e : cur.in_edges) {
            vprog.scatter(context, cur, *e);
            }
        }
        // Loop over out edges
        if(scatter_dir == graphlab::OUT_EDGES || scatter_dir == graphlab::ALL_EDGES) {
          for (edge_type *e : cur.out_edges) {
            vprog.scatter(context, cur, *e);
            }
        }
    }

    cout << "Engine has finished running.." << endl;
}

#endif