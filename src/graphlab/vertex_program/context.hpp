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
 */


/**
 *  ---- Notes by Can Cebeci ----
 * 
 *  This class has been simplified and reduced significanly.
 *  It currently only implements the signal() function.
 *  TODO: implement the rest of the relevant functionalities.
 */


#ifndef GRAPHLAB_CONTEXT_HPP
#define GRAPHLAB_CONTEXT_HPP

#include <set>
#include <vector>
#include <cassert>

#include "icontext.hpp"

namespace graphlab {



  /**
   * \brief The context object mediates the interaction between the
   * vertex program and the graphlab execution environment and
   * implements the \ref icontext interface.
   *
   * \tparam Engine the engine that is using this context.
   */
  template<typename Engine>
  class context : 
    public icontext<typename Engine::graph_type,
                    typename Engine::gather_type,
                    typename Engine::message_type> {
  public:
    // Type members ===========================================================
    /** The engine that created this context object */
    typedef Engine engine_type;

    /** The parent type */
    typedef icontext<typename Engine::graph_type,
                     typename Engine::gather_type,
                     typename Engine::message_type> icontext_type;
    typedef typename icontext_type::graph_type graph_type;
    typedef typename icontext_type::vertex_id_type vertex_id_type;
    typedef typename icontext_type::vertex_type vertex_type;   
    typedef typename icontext_type::message_type message_type;
    typedef typename icontext_type::gather_type gather_type;



  private:
    /** A reference to the engine that created this context */
    engine_type& engine;
    /** A reference to the graph that is being operated on by the engine */
    graph_type& graph;
       
  public:        
    /** 
     * \brief Construct a context for a particular engine and graph pair.
     */
    context(engine_type& engine, graph_type& graph) : 
      engine(engine), graph(graph) { }

    /**
     * Send a message to a vertex.
     */
    void signal(const vertex_type& vertex, 
                const message_type& message = message_type()) {
      engine.internal_signal(vertex/**, message*/);
    }                                              

  }; // end of context
  
} // end of namespace

#endif

