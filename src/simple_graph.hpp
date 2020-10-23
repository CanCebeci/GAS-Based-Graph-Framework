/**
 * TODO:
 *  - Reconsider the efficieny of the graph representation.
 *    using direct references instead of vertex id's might eliminate
 *    retrieval time from the map.
 * 
 *  - Make things default constructible as in GraphLab. 
 * 
 *  - Vertices and edges store data directly, not a pointer.
 *    might make sense to fix this to avert possible copying overhead.
 * 
 *  - Consider moving to GraphLab's representation, where the
 *    vertex_type objects do not store the edges and are used to store
 *    other vertex information.
 */

#ifndef __NONTEMP_GRAPH_H
#define __NONTEMP_GRAPH_H

#include <list>
#include <unordered_map>

/** 
 * An adjacency list-based graph structure
 */
template<typename VertexData, typename EdgeData>
class Graph {
public:
    typedef int vertex_id_type; // should this be statically declared here?

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
         * unordered_map. A default-constructed vertex should never be processed.
         */
        vertex_type(): vid(-1) {}

        /**
         * TODO: the constructor copies the data. What is the better alternative?
         * hold a pointer to vertexdata instead? Is that unnecessary complication?
         */
        vertex_type(vertex_id_type vid, const VertexData& data): vid(vid), vdata(data) {}

        const VertexData& data() const { return vdata; }

        VertexData& data() { return vdata; }

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
        Graph& graph_ref;

        vertex_id_type source_vid;
        vertex_id_type target_vid; 
        EdgeData edata;

        /**
         * TODO: the constructor copies the data. What is the better alternative?
         * hold a pointer to vertexdata instead? Is that unnecessary complication?
         *
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

    std::unordered_map<vertex_id_type, vertex_type*> vertices; 
    
    Graph() {vertices = std::unordered_map<vertex_id_type, vertex_type *>();}

    // return false if vid already exists
    bool add_vertex(const vertex_id_type& vid, const VertexData& vdata) {
        // TODO: VertexData must be default constructable.
        if (vertices.count(vid) == 0) {
            vertices[vid] = new vertex_type(vid, vdata);
            return true;
        } else {
            return false;
        }
    }

    // returns false if self-edge or vid's don't exist.
    bool add_edge(vertex_id_type source, vertex_id_type target,
                  const EdgeData& edata) {
        if (source == target || vertices.count(source) == 0 || vertices.count(target) == 0) {
            return false;
        }
        // TODO: EdgeData must be default constructable.

        edge_type *e = new edge_type(*this, source ,target, edata);

        vertices[source]->out_edges.push_back(e);
        vertices[target]->in_edges.push_back(e);

        return true;
    }

    vertex_type& vertex(vertex_id_type vid) {
        return *(vertices.at(vid));
    }
};

#endif