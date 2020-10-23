/**  
 * Copyright (c) 2009 Carnegie Mellon University. 
 *     All rights reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS
 *  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied.  See the License for the specific language
 *  governing permissions and limitations under the License.
 *
 * For more about this software visit:
 *
 *      http://www.graphlab.ml.cmu.edu
 *
 * Also contains code that is Copyright 2011 Yahoo! Inc.  All rights
 * reserved. 
 *
 */

#ifndef GRAPHLAB_IVERTEX_PROGRAM_HPP
#define GRAPHLAB_IVERTEX_PROGRAM_HPP


#include "context.hpp"
#include "../graph/graph_basic_types.hpp"
#include "../../simple_graph.hpp"  // new graph structure

#include <iostream>   // added to report errors where needed

namespace graphlab {
  template<typename GraphType,
           typename GatherType,
           typename MessageType = int>  /** TODO: this should not default to int */
  class ivertex_program {    
  public:

    // User defined type members ==============================================
    /**
     * \brief The user defined vertex data associated with each vertex
     * in the graph (see \ref distributed_graph::vertex_data_type).
     *
     * The vertex data is the data associated with each vertex in the
     * graph.  Unlike the vertex-program the vertex data of adjacent
     * vertices is visible to other vertex programs during the gather
     * and scatter phases and persists between executions of the
     * vertex-program.
     *
     * The vertex data type must be serializable 
     * (see \ref sec_serializable)
     */
    typedef typename GraphType::vertex_data_type vertex_data_type;
    /**
     * \brief The user defined edge data associated with each edge in
     * the graph.
     *
     * The edge data type must be serializable 
     * (see \ref sec_serializable)
     *
     */
    typedef typename GraphType::edge_data_type edge_data_type;

    /**
     * \brief The user defined gather type is used to accumulate the
     * results of the gather function during the gather phase and must
     * implement the operator += operation.
     *
     * The gather type plays the following role in the vertex program:
     * 
     * \code
     * gather_type sum = EMPTY;
     * for(edges in vprog.gather_edges()) {
     *   if(sum == EMPTY) sum = vprog.gather(...);
     *   else sum += vprog.gather( ... );
     * }
     * vprog.apply(..., sum);
     * \endcode
     *
     * In addition to implementing the operator+= operation the gather
     * type must also be serializable (see \ref sec_serializable).
     */
    typedef GatherType gather_type;

    /**
     * The message type which must be provided by the vertex_program
     */
    typedef MessageType message_type;

    // Graph specific type members ============================================
    /**
     * \brief The graph type associative with this vertex program. 
     *
     * The graph type is specified as a template argument and will
     * usually be \ref distributed_graph.
     */
    typedef GraphType graph_type;

    typedef typename GraphType::vertex_type vertex_type;

    typedef typename GraphType::edge_type edge_type;

    /**
     * \brief The unique integer id used to reference vertices in the graph.
     * 
     * See \ref graphlab::vertex_id_type for details.
     */
    typedef typename graph_type::vertex_id_type vertex_id_type;

    /**
     * \brief The type used to define the direction of edges used in
     * gather and scatter.
     * 
     * Possible values include:
     *
     * \li graphlab::NO_EDGES : Do not process any edges
     * 
     * \li graphlab::IN_EDGES : Process only inbound edges to this
     * vertex
     * 
     * \li graphlab::OUT_EDGES : Process only outbound edges to this
     * vertex
     *
     * \li graphlab::ALL_EDGES : Process both inbound and outbound
     * edges on this vertes.
     * 
     * See \ref graphlab::edge_dir_type for details.
     */
    typedef graphlab::edge_dir_type edge_dir_type;

    // Additional Types =======================================================
    
    /**
     * \brief The context type is used by the vertex program to
     * communicate with the engine.
     *
     * The context and provides facilities for signaling adjacent
     * vertices (sending messages), interacting with the GraphLab
     * gather cache (posting deltas), and accessing engine state.
     *
     */
    typedef icontext<graph_type, gather_type, message_type> icontext_type;
   
   
     // Functions ==============================================================
    /**
     * \brief Standard virtual destructor for an abstract class.
     */
    virtual ~ivertex_program() { }

    /**
     * \brief This called by the engine to receive a message to this
     * vertex program.  The vertex program can use this to initialize
     * any state before entering the gather phase.  The init function
     * is invoked _once_ per execution of the vertex program.
     *
     * If the vertex program does not implement this function then the
     * default implementation (NOP) is used.
     *
     * \param [in,out] context The context is used to interact with the engine
     *
     * \param [in] vertex The vertex on which this vertex-program is
     * running. Note that the vertex is constant and its value should
     * not be modified within the init function.  If there is some
     * message state that is needed then it must be saved to the
     * vertex-program and not the vertex data.
     *
     * \param [in] message The sum of all the signal calls to this
     * vertex since it was last run.
     */
    virtual void init(icontext_type& context,
                      const vertex_type& vertex, 
                      const message_type& msg) { /** NOP */ }
    

    /**
     * \brief Returns the set of edges on which to run the gather
     * function.  The default edge direction is in edges.
     *
     * The gather_edges function is invoked after the init function
     * has completed.
     *
     * \warning The gather_edges function may be invoked multiple
     * times for the same execution of the vertex-program and should
     * return the same value.  In addition it cannot modify the
     * vertex-programs state or the vertex data.
     *
     * Possible return values include:
     *
     * \li graphlab::NO_EDGES : The gather phase is completely skipped
     * potentially reducing network communication.
     * 
     * \li graphlab::IN_EDGES : The gather function is only run on
     * inbound edges to this vertex.
     * 
     * \li graphlab::OUT_EDGES : The gather function is only run on
     * outbound edges to this vertex.
     *
     * \li graphlab::ALL_EDGES : The gather function is run on both
     * inbound and outbound edges to this vertes.
     * 
     * \param [in,out] context The context is used to interact with
     * the engine
     *
     * \param [in] vertex The vertex on which this vertex-program is
     * running. Note that the vertex is constant and its value should
     * not be modified.
     * 
     * \return One of graphlab::NO_EDGES, graphlab::IN_EDGES,
     * graphlab::OUT_EDGES, or graphlab::ALL_EDGES.
     * 
     */
    virtual edge_dir_type gather_edges(icontext_type& context,
                                       const vertex_type& vertex) const { 
      return IN_EDGES; 
    }


    /**
     * \brief The gather function is called on all the 
     * \ref ivertex_program::gather_edges in parallel and returns the 
     * \ref gather_type which are added to compute the final output of 
     * the gather phase.  
     *
     * The gather function is the core computational element of the
     * Gather phase and is responsible for collecting the information
     * about the state of adjacent vertices and edges.  
     *
     * \warning The gather function is executed in parallel on
     * multiple machines and therefore cannot modify the
     * vertex-program's state or the vertex data.
     *
     * A default implementation of the gather function is provided
     * which will fail if invoked. 
     *
     * \param [in,out] context The context is used to interact with
     * the engine
     *
     * \param [in] vertex The vertex on which this vertex-program is
     * running. Note that the vertex is constant and its value should
     * not be modified.
     *
     * \param [in,out] edge The adjacent edge to be processed.  The
     * edge is not constant and therefore the edge data can be
     * modified. 
     *
     * \return the result of the gather computation which will be
     * "summed" to produce the input to the apply operation.  The
     * behavior of the "sum" is defined by the \ref gather_type.
     * 
     */
    virtual gather_type gather(icontext_type& context, 
                               const vertex_type& vertex, 
                               edge_type& edge) const {
      std::cerr << "Gather not implemented!" << std::endl;
      return gather_type();
    };


    /**
     * \brief The apply function is called once the gather phase has
     * completed and must be implemented by all vertex programs.
     *
     * The apply function is responsible for modifying the vertex data
     * and is run only once per vertex per execution of a vertex
     * program.  In addition the apply function can modify the state
     * of the vertex program.
     *
     * If a vertex has no neighbors than the apply function is called
     * passing the default value for the gather_type.
     *
     * \param [in,out] context The context is used to interact with
     * the engine
     *
     * \param [in,out] vertex The vertex on which this vertex-program is
     * running. 
     *
     * \param [in] total The result of the gather phase.  If a vertex
     * has no neighbors then the total is the default value (i.e.,
     * gather_type()) of the gather type.
     * 
     */
    virtual void apply(icontext_type& context, 
                       vertex_type& vertex, 
                       const gather_type& total) = 0;

    /**
     * \brief Returns the set of edges on which to run the scatter
     * function.  The default edge direction is out edges.
     *
     * The scatter_edges function is invoked after the apply function
     * has completed.
     *
     * \warning The scatter_edges function may be invoked multiple
     * times for the same execution of the vertex-program and should
     * return the same value.  In addition it cannot modify the
     * vertex-programs state or the vertex data.
     *
     * Possible return values include:
     *
     * \li graphlab::NO_EDGES : The scatter phase is completely
     * skipped potentially reducing network communication.
     * 
     * \li graphlab::IN_EDGE : The scatter function is only run on
     * inbound edges to this vertex.
     * 
     * \li graphlab::OUT_EDGES : The scatter function is only run on
     * outbound edges to this vertex.
     *
     * \li graphlab::ALL_EDGES : The scatter function is run on both
     * inbound and outbound edges to this vertes.
     * 
     * \param [in,out] context The context is used to interact with
     * the engine
     *
     * \param [in] vertex The vertex on which this vertex-program is
     * running. Note that the vertex is constant and its value should
     * not be modified.
     * 
     * \return One of graphlab::NO_EDGES, graphlab::IN_EDGES,
     * graphlab::OUT_EDGES, or graphlab::ALL_EDGES.
     * 
     */
    virtual edge_dir_type scatter_edges(icontext_type& context,
                                        const vertex_type& vertex) const { 
      return OUT_EDGES; 
    }

    /**
     * \brief Scatter is called on all scatter_edges() in parallel
     * after the apply function has completed and is typically
     * responsible for updating edge data, signaling (messaging)
     * adjacent vertices, and updating the gather cache state when
     * caching is enabled.
     *
     * The scatter function is almost identical to the gather function
     * except that nothing is returned. 
     *
     * \warning The scatter function is executed in parallel on
     * multiple machines and therefore cannot modify the
     * vertex-program's state or the vertex data.
     *
     * A default implementation of the gather function is provided
     * which will fail if invoked. 
     *
     * \param [in,out] context The context is used to interact with
     * the engine
     *
     * \param [in] vertex The vertex on which this vertex-program is
     * running. Note that the vertex is constant and its value should
     * not be modified.
     *
     * \param [in,out] edge The adjacent edge to be processed.  The
     * edge is not constant and therefore the edge data can be
     * modified. 
     *
     */
    virtual void scatter(icontext_type& context, const vertex_type& vertex, 
                         edge_type& edge) const { 
      std::cerr << "Scatter not implemented!" << std::endl;
    };

    // pre_local_gather() and post_local_gather are omitted.

  };  // end of ivertex_program
 
}; //end of namespace graphlab

#endif
