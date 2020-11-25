/**
 * TODO:
 *  - Reconsider the efficieny of the graph representation.
 *    using direct references instead of vertex id's might eliminate
 *    retrieval time from the map.
 * 
 *  - Force things to be default constructible as in GraphLab. 
 * 
 *  - Vertices and edges store data directly, not a pointer.
 *    might make sense to fix this to avert possible copying overhead.
 * 
 *  - Consider moving to GraphLab's representation, where the
 *    vertex_type objects do not store the edges and are used to store
 *    other vertex information.
 */

#ifndef __SIMPLE_GRAPH_H
#define __SIMPLE_GRAPH_H

#include <list> // adjancecy lists of vertices
#include <vector> // 

/** 
 * An adjacency list-based graph structure
 */
template<typename VertexData, typename EdgeData>
class Graph {
public:
    // ---------------------------------------- //
    // ---------------- TYPES ----------------- //
    // ---------------------------------------- //
    typedef int vertex_id_type;

    typedef VertexData vertex_data_type;    // used by ivertex_program
    typedef EdgeData edge_data_type;    // used by ivertex_program
    
    struct edge_type;

    struct vertex_type {
        vertex_id_type vid;     // it might be unnecessary to store this again.
        std::list<edge_type*> out_edges;
        std::list<edge_type*> in_edges;
        VertexData vdata;

        /**
         * Default constructor required to use vertex_type as the value type in
         * std::vector. A default-constructed vertex should never be processed.
         */
        vertex_type(): vid(-1) {}

        /**
         * TODO: the constructor copies the data. What is the better alternative?
         * hold a pointer to vertexdata instead? Is that unnecessary complication?
         */
        vertex_type(vertex_id_type vid, const VertexData& data): vid(vid), vdata(data) {}

        VertexData& data() { return vdata; }

        const VertexData& data() const { return vdata; }

        int num_in_edges() const {
            return in_edges.size();
        }

        int num_out_edges() const {
            return out_edges.size();
        }

        vertex_id_type id() const {
            return vid;
        }
    };

    struct edge_type {
        Graph& graph_ref;   // needed in order to access the source and target vertices.

        vertex_id_type source_vid;
        vertex_id_type target_vid; 
        EdgeData edata;

        /**
         * TODO: the constructor copies the data. What is the better alternative?
         * hold a pointer to vertexdata instead? Is that unnecessary complication?
         */
        edge_type(Graph& graph_ref, vertex_id_type source, vertex_id_type target, const EdgeData& data)
            : graph_ref(graph_ref), source_vid(source), target_vid(target), edata(data) {}

        vertex_type source() const {
            return *(graph_ref.vertices.at(source_vid));
        }

        vertex_type target() const {
            return *(graph_ref.vertices.at(target_vid));
        }

        const EdgeData& data() const { return edata; }

        EdgeData& data() { return edata; }
    };


    // ---------------------------------------- //
    // -------------- PROPERTIES -------------- //
    // ---------------------------------------- //

    std::vector<vertex_type*> vertices;


    // ---------------------------------------- //
    // --------------- METHODS ---------------- //
    // ---------------------------------------- // 

    /** 
     * TODO: provide a destructor that de-allocates dynamic memory. 
     */

    // returns true if vid is negative or occupied 
    bool add_vertex(vertex_id_type vid, const VertexData& vdata = VertexData()) {
        if (vid < 0) {
            return false;
        }
        if (vid >= vertices.size()) {
            vertices.resize(vid + 1, new vertex_type());    // fill gaps with default constructed 
                                                            // vertices. They have negative id's.
        }
        if (vertices[vid]->id() > 0) {    // the vid is occupied
            return false;
        }
        vertices[vid] = new vertex_type(vid, vdata);
        return true;
    }

    // returns false if self edge or vid's are invalid.
    bool add_edge(vertex_id_type source, vertex_id_type target,
                  const EdgeData& edata = EdgeData()) {
        if (source == target    // self edge
            || source < 0 || source >= vertices.size() || vertices[source]->id() < 0     // source invalid
            || source < 0 || source >= vertices.size() || vertices[source]->id() < 0) {  // target invalid
            return false;
        }

        edge_type *e = new edge_type(*this, source ,target, edata);

        vertices[source]->out_edges.push_back(e);
        vertices[target]->in_edges.push_back(e);
        return true;
    }

    vertex_type& vertex(vertex_id_type vid) {
        return *(vertices.at(vid));
    }

    int num_vertices() {
        return vertices.size();
    }
};

#endif