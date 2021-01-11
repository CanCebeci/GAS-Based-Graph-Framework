/**
 * Right now, access to vertex and edge data is synchronized with a mechanism
 * based on the monitor solution to the dining philosophers problem described 
 * in Operating System Concepts (9th Edition) by Silberschatz, Galvin & Gagne.
 * 
 * In the near future, it may be replaced with the Chandy-Misra solution, 
 * which is used by GraphLab as well.
 * 
 * ! For the sake of simplicity, everything that needs synchronization uses
 * ! the same mutex. They all access vertex_states so that is to some degreee 
 * ! necessary. Think about how this might be resolved. Using a lock for
 * ! each element in vertex_states might allow deadlocks. 
 * 
 * ! Think about limiting the duration of the locks to specific parts of the
 * ! functions.
 */

#ifndef __ASYNC_ENGINE_H
#define __ASYNC_ENGINE_H

#include "simple_graph.hpp"
#include "../graphlab/graphlab.hpp"
#include "spm_interface.hpp"

#include <unordered_set>
#include <vector>
#include <type_traits>  //for is_base_of
#include <iostream>
#include <algorithm>    //min()

#include <thread>
#include <mutex>
#include <condition_variable>

// #define load_ahead_distance 50
// #define NUM_THREADS 2

template<typename VertexProgram>
class async_engine {
    // ---------------------------------------- //
    // --------------- TYPEDEFS --------------- //
    // ---------------------------------------- //
    // -- mostly borrowed from GraphLab's synchronous_engine.hpp
public:
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

    // ---------------------------------------- //
    // -------------- FUNCTIONS --------------- //
    // ---------------------------------------- //

    // constructor
    async_engine(graph_type& g, int load_ahead_distance, int num_threads, bool enable_caching = false): g(g), 
                                                                caching_enabled(enable_caching),
                                                                context(*this, g),
                                                                num_threads(num_threads),
                                                                load_ahead_distance(load_ahead_distance), 
                                                                num_idle_threads(0) {
        if (!std::is_base_of<graphlab::ivertex_program<graph_type, gather_type>, VertexProgram>::value) {
            throw "type parameter for async egnine is not derived from graphlab::ivertex_program";
        }
        vertex_states.resize(g.num_vertices(), vertex_state_type::FREE);
        in_use.resize(g.num_vertices(), false);
        gather_cache.resize(g.num_vertices());
        has_cache.resize(g.num_vertices(), false);

        // This cannot be resized since the copy constructor for std::condition_variable is deleted.
        cv_exclusive_access = std::vector<std::condition_variable>(g.num_vertices());

        spm_hits = 0;
        spm_misses = 0;
    }

    // called by the application programmer
    void signal_all();
    void start();

    // called by the context
    void internal_signal(const vertex_type& vertex);
    void internal_post_delta(const vertex_type& vertex, const gather_type& delta);
    void internal_clear_gather_cache(const vertex_type& vertex);


    // ---------------------------------------- //
    // ------------- DATA MEMBERS ------------- //
    // ---------------------------------------- //

    // test-purpose counters
    long int spm_hits;
    long int spm_misses;

    spm_interface<graph_type> spmi;

private:
    graph_type& g;  // A reference to the input graph.

    /**  
     * TODO: Possibly replace later with better data structure.
     * * A bitset could be useful now that vid's are restricted to be consecutive.
     * * Though it would make an empty-check and the retrival of any acitve vertex slower.
     */
    std::unordered_set<vertex_id_type> active_list; // The collection of vertices that have not converged yet.

    /**
     * Indicates whether the application programmer has enabled gather caching.
     */
    bool caching_enabled;

    std::vector<gather_type> gather_cache;  
    std::vector<bool> has_cache;

    // creating a separate context for each vertex program is not necessary. This one is used by all of them.
    context_type context;



    // --------------------------------------------------------------------------- //
    // --- MULTITHREADING & SYNCHRONIZATION RELATED INTERNAL DATA & FUNCTIONS ---- //
    // --------------------------------------------------------------------------- //
    const int num_threads;
    int num_idle_threads;

    enum vertex_state_type {
        FREE,       // the vertex program for the vertex is not scheduled to any thread.
        SCHEDULED,  // the vprog is assigned to a thread, which is waiting to acquire lock to begin running
        RUNNING
    };
    std::vector<vertex_state_type> vertex_states;
    
    /**
     * ! See the comments at the very top of the file. Right now this mutex is used for all synchronization.
     */
    std::mutex scheduling_mutex;

    /**
     * When a thread tries to get a job, job_queue is empty, and there exists at least one
     * busy thread, the thread asking for a job waits on cv_no_jobs. It is waken up when 
     * a vertex is signalled. 
     * Alternatively, it may be waken up when the last busy thread finds active_list empty. 
     * In that case, no futher activation is possible. Each idle thread is waken up and they 
     * all fail to get jobs. After that they all quit. 
     */
    std::condition_variable cv_no_jobs;

    /** 
     * Used in the dining philosophers-based synchronization of vertex programs.
     * A thread that can not get the exclusive access required for its job (see 
     * get_exclusive_access below) will wait on cv_exclusive_access[vid] where vid 
     * is the id of the vertex whose program the thread is scheduled to execute.
     */
    std::vector<std::condition_variable> cv_exclusive_access;

    /**
     * Has an entry for each vertex that is true if a neighbour or the vertex itself
     * is currently executing. Analogous to the forks in the dining phil. analogy.
     */
    std::vector<bool> in_use;

    int load_ahead_distance;

    void thread_start();
    void execute_vprog(vertex_id_type vid);
    bool get_next_job(vertex_id_type& ret_vid);

    /**
     * Blocks current thread until it has exclusive access to the vertex at vid, all its
     * neighbours and the edges in between. Equiavalent to pickup(...) in the 
     * above-mentioned dining philosophers analogy.
     */
    void get_exclusive_access(vertex_id_type vid);

    /**
     * Equiavalent to test(...) in the above-mentioned dining philosophers analogy.
     * block is a return argument that holds a vid which can not be accesssed exclusively
     * if the return value is false.
     */
    bool exclusive_access_possible(vertex_id_type vid, vertex_id_type &block);

    /**
     * Equiavalent to putdown(...) in the above-mentioned dining philosophers analogy.
     */
    void release_exclusive_access(vertex_id_type vid);

    // a test-purpose function that increments hit & miss counts. 
    void check_spm_hit(const edge_type &e, const vertex_type &v);
};

/**
 * implementation file (previously async_engine.cpp) copy-pasted below. Otherwise, template functions do not work
 */

using namespace std;

template<typename VertexProgram>
void async_engine<VertexProgram>::get_exclusive_access(vertex_id_type vid) {
    std::unique_lock<std::mutex> lock(scheduling_mutex);

    vertex_id_type block;
    while (!exclusive_access_possible(vid, block)) {
        cv_exclusive_access[block].wait(lock);
    }

    in_use[vid] = true; // acquire vid
    vertex_type v = g.vertex(vid);
    for (edge_type *e : v.in_edges) {   // acquire in_neigbours
        in_use[e->source().id()] = true;
    }
    for (edge_type *e : v.out_edges) {   // acquire out_neigbours
        in_use[e->target().id()] = true;
    }


    vertex_states[vid] = vertex_state_type::RUNNING;
}

template<typename VertexProgram>
bool async_engine<VertexProgram>::exclusive_access_possible(vertex_id_type vid, vertex_id_type &block) {
    // always called by get_exclusive_access, which readily holds mutex_exclusive access
    
    vertex_type v = g.vertex(vid);
    if (in_use[v.id()]) {
        block = v.id();
        return false;
    }
    for (edge_type *e : v.in_edges) {   // can't get access if any in_neighbour is in use
        if (in_use[e->source().id()]) {
            block = e->source().id();
            return false;
        }
    }
    for (edge_type *e : v.out_edges) {   // can't get access if any out_neighbour is in use
        if (in_use[e->target().id()]) {
            block = e->target().id();
            return false;
        }
    }
    return true;
}

template<typename VertexProgram>
void async_engine<VertexProgram>::release_exclusive_access(vertex_id_type vid) {
    std::unique_lock<std::mutex> lock(scheduling_mutex);

    vertex_states[vid] = vertex_state_type::FREE;
    vertex_type v = g.vertex(vid);

    in_use[vid] = false;    // release executed vertex
    cv_exclusive_access[vid].notify_all();

    for (edge_type *e : v.in_edges) {   // release in neighbours
        in_use[e->source().id()] = false;
        cv_exclusive_access[e->source().id()].notify_all();
    }
    for (edge_type *e : v.out_edges) {   // release out neighbours
        in_use[e->target().id()] = false;
        cv_exclusive_access[e->target().id()].notify_all();
    }

}

/**
 * TODO: Might get rid of the need to check if the vertex is SCHEDUELED by 
 * TODO: moving removal from active_list to after exclusive_access is obtained.
 * TODO: then, internal_signal would not have to access vertex_states.
 */
template<typename VertexProgram>
void async_engine<VertexProgram>::internal_signal(const vertex_type& vertex) {
    // lock scheduling_mutex within the whole function body.
    std::unique_lock<std::mutex> lock_AL(scheduling_mutex);

    const vertex_id_type vid = vertex.id();

    if (active_list.count(vid) == 0) {  // if vertex is not already active
        if (vertex_states[vid] == vertex_state_type::FREE) {
            active_list.insert(vid);
            cv_no_jobs.notify_one();  // an idle thread may wake up and find a job.
        } else if (vertex_states[vid] == vertex_state_type::SCHEDULED) {
            // skip activation.. the thread that has the vertex will access the latest data anyway.
        } else {
            // TODO: throw a proper exception.
            throw std::runtime_error("running vertex signalled. A neighbour must have been running also");
        }
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
}


/**
 * singal_all() currently accesses active_list without synchronization.
 * ?: Does it need to?  Vertex programs can not call singal_all() anyway.
 */
template<typename VertexProgram>
void async_engine<VertexProgram>::signal_all() {
    for (int i = 0; i < g.vertices.size(); i++) {
        if (active_list.count(i) == 0) {
            active_list.insert(i);
        }
    }
}

/**
 * Naive implementation. Executes each vertex program in isolation.
 */
template<typename VertexProgram>
void async_engine<VertexProgram>::start() {
    thread threads[num_threads];
    for (int i = 0; i < num_threads; i++) {
        threads[i] = thread([&]{ this->thread_start(); });
    }

    for (int i = 0; i < num_threads; i++) {
        threads[i].join();
        cout << "Thread " << i << " is done" << endl;
    }
    cout << "Engine has finished running.." << endl;
}

template<typename VertexProgram>
bool async_engine<VertexProgram>::get_next_job(vertex_id_type& ret_vid) {
    // lock scheduling_mutex within the whole function body.
    std::unique_lock<std::mutex> lock_AL(scheduling_mutex);
    num_idle_threads++;
    while (active_list.empty() && (num_idle_threads < num_threads)) {
        // wait until some other thread, which may activate new vertices and create jobs, finishes running.
        cv_no_jobs.wait(lock_AL);
    }
    if (active_list.empty()) {   // all other threads are idle, no further activation is possible.
        cv_no_jobs.notify_all();  // all idle threads should wake up and fail to get a job.
        // num_idle_threads is not decremented so that the other threads can properly fail.
        return false;
    } else {
        ret_vid = *(active_list.begin());
        active_list.erase(ret_vid);
        num_idle_threads--; // the thread is no longer idle
        vertex_states[ret_vid] = vertex_state_type::SCHEDULED;
        return true;
    }
}

template<typename VertexProgram>
void async_engine<VertexProgram>::thread_start() {
    vertex_id_type job_vid;
    while (get_next_job(job_vid)) {
        get_exclusive_access(job_vid);
        //cerr << "getexclac done v: " << job_vid << endl;
        // --- vertex-program-level load ahead ---
        vertex_type& job_vertex = g.vertex(job_vid);
        for (int i = 0; i < min(load_ahead_distance, job_vertex.num_in_edges()); i++) {
            if (!is_same<edge_data_type, graphlab::empty>::value) {
                spmi.load_edata(*job_vertex.in_edges[i]);
            }
            if (!is_same<vertex_data_type, graphlab::empty>::value) {
                spmi.load_vdata((job_vertex.in_edges[i])->source());
            }
        }
        for (int i = 0;
             i < min(load_ahead_distance - job_vertex.num_in_edges(),
                     job_vertex.num_out_edges());
                     i++) {
            if (!is_same<edge_data_type, graphlab::empty>::value) {
                spmi.load_edata(*job_vertex.out_edges[i]);
            }
            if (!is_same<vertex_data_type, graphlab::empty>::value) {
                spmi.load_vdata((job_vertex.out_edges[i])->target());
            }
        }
        //cerr << "vprog preload done v: " << job_vid << endl;
        // vertex-program-level load ahead done
        // *** use the BARRIER INSTRUCTION here to suspend thread until vprog-level load ahead is done *** //
        execute_vprog(job_vid);
        //cerr << "excv done v: " << job_vid << endl;
        release_exclusive_access(job_vid);
        // TODO: right after releasing the mutex, get_next_job tries to acquire it again.
        // ?: can this be merged somehow to increase performance?
        //spmi.print_vslab_info();
        //spmi.print_eslab_info();
        //string in;
        //std::cin >> in;
    }
}

template<typename VertexProgram>
void async_engine<VertexProgram>::check_spm_hit(const edge_type &e, const vertex_type &v) {
    if (!is_same<edge_data_type, graphlab::empty>::value) {
        edge_data_type edata;
        if (spmi.read_edata(e, edata)) {
            spm_hits++;
            //cerr << "-> edge hit, ";
        } else {
            spm_misses++;
            //cerr << "-> edge miss, ";
        }
    }
    if (!is_same<vertex_data_type, graphlab::empty>::value) {
        vertex_data_type vdata;
        if (spmi.read_vdata(v, vdata)) {
            spm_hits++;
            //cerr << "vertex hit\n";
        } else {
            spm_misses++;
            //cerr << "vertex miss\n";
        }
    }
}


template<typename VertexProgram>
void async_engine<VertexProgram>::execute_vprog(vertex_id_type vid) {

    // instantiate vertex program
    VertexProgram vprog;
    vertex_type& cur = g.vertex(vid);

    // init() phase is skipped for now along with anything message passsing-related.

    /**
     * -----  GATHER PHASE  -----  
     */
    bool accum_is_set = false;
    gather_type accum = gather_type();  // imporant to explicitly call the default constructor for basic data types
                                        // when gather_type is double, int, bool etc...

    vector<vertex_type *> loaded_doubcon_neighs;  // used for the special treatment of doubly connected neighbours in SPM.

    if (caching_enabled && has_cache[vid]) {
        accum = gather_cache[vid];
        accum_is_set = true;
    } else {
        const edge_dir_type gather_dir = vprog.gather_edges(context, cur);

        /**
         * Gather is commonly done over in_edges.
         * Heuristically, in_edges are loaded ahead of the execution of the vertex program.
         * If unused, this pre-loaded data should be dumped from SPM.
         */

        // Loop over in edges
        if (gather_dir == graphlab::IN_EDGES || gather_dir == graphlab::ALL_EDGES) {
            for (int i = 0; i < cur.in_edges.size(); i++) {
                // -- load ahead into SPM --
                if (i + load_ahead_distance < cur.in_edges.size()) {
                    // load an in_edge
                    edge_type *load_ahead_edgeptr = cur.in_edges[i + load_ahead_distance];
                    if (!is_same<edge_data_type, graphlab::empty>::value) {
                        spmi.load_edata(*load_ahead_edgeptr);
                    }
                    if (!is_same<vertex_data_type, graphlab::empty>::value) {
                        spmi.load_vdata(load_ahead_edgeptr->source());
                    }
                } else if (i + load_ahead_distance - cur.in_edges.size() < cur.out_edges.size()) {
                    //  load an out_edge
                    /**
                     * Out edges are loaded even if gather_dir == graphlab::IN_EDGES. 
                     * Scatters begin with out edges so even if gather skips them, the loads
                     * will heuristically be used (since often scatter contains out edges)
                     */
                    edge_type *load_ahead_edgeptr = cur.out_edges[i + load_ahead_distance - cur.in_edges.size()];
                    if (!is_same<edge_data_type, graphlab::empty>::value) {
                        spmi.load_edata(*load_ahead_edgeptr);
                    }
                    if (!is_same<vertex_data_type, graphlab::empty>::value) {
                        spmi.load_vdata(load_ahead_edgeptr->target());
                    }
                }

                /**
                 * Need a compiler modification to replace gather's access to
                 * edge and vertex data with SPM instructions. For now, gather
                 * accesses data regularly.
                 * 
                 * Whether the data is present in SPM is still checked in order
                 * to analyse the SPM hit rate.
                 */
                //cerr << "gather in edges, v: " << cur.id() << " i: " << i << " s: " << (cur.in_edges[i])->source().id();
                check_spm_hit(*(cur.in_edges[i]), (cur.in_edges[i])->source());
                
                // execute the actual gather
                if (accum_is_set) {
                    accum += vprog.gather(context, cur, *(cur.in_edges[i]));
                } else {
                    accum = vprog.gather(context, cur, *(cur.in_edges[i]));
                    accum_is_set = true;
                }

                // -- remove from SPM --
                spmi.remove_edata(*(cur.in_edges[i]));
                // if there is the opposite edge, the neighbour may be needed as an out_neigh.
                if (!(cur.in_edges[i]->has_opposite)) {
                    spmi.remove_vdata((cur.in_edges[i])->source());
                } else {
                    //cerr << "has opp., not removed\n";
                    loaded_doubcon_neighs.push_back(&(cur.in_edges[i])->source());
                }
            }
        } else {
            // Gather does not include in_edges. Remove the data previously loaded for them.
            for (int i = 0; i < min(load_ahead_distance, (int) cur.in_edges.size()); i++) {
                spmi.remove_edata(*(cur.in_edges[i]));
                spmi.remove_vdata((cur.in_edges[i])->source());
            }
        }

        // << "gather_in done v: " << cur.id() << endl;
        // Loop over out edges
        if (gather_dir == graphlab::OUT_EDGES || gather_dir == graphlab::ALL_EDGES) {
            for (int i = 0; i < cur.out_edges.size(); i++) {
                // -- load ahead into SPM --
                if (i + load_ahead_distance < cur.out_edges.size()) {
                    // load an out_edge
                    edge_type *load_ahead_edgeptr = cur.out_edges[i + load_ahead_distance];
                    if (!is_same<edge_data_type, graphlab::empty>::value) {
                        spmi.load_edata(*load_ahead_edgeptr);
                    }
                    if (!is_same<vertex_data_type, graphlab::empty>::value) {
                        spmi.load_vdata(load_ahead_edgeptr->target());
                    }
                }

                check_spm_hit(*(cur.out_edges[i]), (cur.out_edges[i])->target());

                // execute the actual gather
                if (accum_is_set) {
                    accum += vprog.gather(context, cur, *(cur.out_edges[i]));
                } else {
                    accum = vprog.gather(context, cur, *(cur.out_edges[i]));
                    accum_is_set = true;
                }

                // -- remove from SPM --
                /**
                 * Do not remove the first load_ahead_distance pairs of data.
                 * These are likely to be used again at the beginning of the scatters.
                 */
                if (i >= load_ahead_distance) {
                    spmi.remove_edata(*(cur.out_edges[i]));
                    spmi.remove_vdata((cur.out_edges[i])->target());
                }
            }
        }
        // If caching is enabled then save the accumulator to the
        // cache for future iterations.  Note that it is possible
        // that the accumulator was never set in which case we are
        // effectively "zeroing out" the cache.
        if(caching_enabled && accum_is_set) {
            gather_cache[vid] = accum; 
            has_cache[vid] = true;
        }
    }

    /**
     * -----  APPLY PHASE  -----
     */
    vprog.apply(context, cur, accum);
    //cerr << "apply done v: " << cur.id() << endl;
    /**
     * -----  SCATTER PHASE  -----
     */
    const edge_dir_type scatter_dir = vprog.scatter_edges(context, cur);
    // Loop over out edges
    if (scatter_dir == graphlab::OUT_EDGES || scatter_dir == graphlab::ALL_EDGES) {
        for (int i = 0; i < cur.out_edges.size(); i++) {
            // -- load ahead into SPM --
            if (i + load_ahead_distance < cur.out_edges.size()) {
                // load an out_edge
                edge_type *load_ahead_edgeptr = cur.out_edges[i + load_ahead_distance];
                if (!is_same<edge_data_type, graphlab::empty>::value) {
                    spmi.load_edata(*load_ahead_edgeptr);
                }
                if (!is_same<vertex_data_type, graphlab::empty>::value) {
                    spmi.load_vdata(load_ahead_edgeptr->target());
                }
            } else if (scatter_dir == graphlab::ALL_EDGES &&    // stop loading if scatter will not include in_edges
                i + load_ahead_distance - cur.out_edges.size() < cur.in_edges.size()) {
                // load an in_edge
                edge_type *load_ahead_edgeptr = cur.in_edges[i + load_ahead_distance - cur.out_edges.size()];
                if (!is_same<edge_data_type, graphlab::empty>::value) {
                    spmi.load_edata(*load_ahead_edgeptr);
                }
                if (!is_same<vertex_data_type, graphlab::empty>::value) {
                    spmi.load_vdata(load_ahead_edgeptr->source());
                }
            }
            //cerr << "scatter out edges, v: " << cur.id() << " i: " << i;
            check_spm_hit(*(cur.out_edges[i]), (cur.out_edges[i])->target());

            vprog.scatter(context, cur, *(cur.out_edges[i]));
            
            // -- remove from SPM --
            spmi.remove_edata(*(cur.out_edges[i]));
            spmi.remove_vdata((cur.out_edges[i])->target());
        }
    } else {
        // Scatter does not include out_edges. Remove the data previously loaded for them.
        for (int i = 0; i < min(load_ahead_distance, (int) cur.out_edges.size()); i++) {
            spmi.remove_edata(*(cur.out_edges[i]));
            spmi.remove_vdata((cur.out_edges[i])->target());
        }
    }

    //cerr << "scatter_out done v: " << cur.id() << endl;

    // Loop over in edges
    if (scatter_dir == graphlab::IN_EDGES || scatter_dir == graphlab::ALL_EDGES) {
        for (int i = 0; i < cur.in_edges.size(); i++) {
            // -- load ahead into SPM --
            if (i + load_ahead_distance < cur.in_edges.size()) {
                // load an in_edge
                edge_type *load_ahead_edgeptr = cur.in_edges[i + load_ahead_distance];
                if (!is_same<edge_data_type, graphlab::empty>::value) {
                    spmi.load_edata(*load_ahead_edgeptr);
                }
                if (!is_same<vertex_data_type, graphlab::empty>::value) {
                    spmi.load_vdata(load_ahead_edgeptr->source());
                }
            }
            
            check_spm_hit(*(cur.in_edges[i]), (cur.in_edges[i])->source());

            vprog.scatter(context, cur, *(cur.in_edges[i]));
            
            // -- remove from SPM --
            spmi.remove_edata(*(cur.in_edges[i]));
            spmi.remove_vdata((cur.in_edges[i])->source());
        }
    }

    // Doubly-connected vertex data should be removed here
    for (int i = 0; i < loaded_doubcon_neighs.size(); i++) {
        spmi.remove_vdata(*loaded_doubcon_neighs[i]); 
    }
}

#endif