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
#include "../graphlab/vertex_program/ivertex_program.hpp"
#include "../graphlab/vertex_program/context.hpp"
#include "spm_interface.hpp"

#include <unordered_set>
#include <vector>
#include <type_traits>  //for is_base_of
#include <iostream>

#include <thread>
#include <mutex>
#include <condition_variable>

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
    async_engine(graph_type& g, bool enable_caching = false): g(g), 
                                                                caching_enabled(enable_caching),
                                                                context(*this, g), 
                                                                num_idle_threads(0) {
        if (!std::is_base_of<graphlab::ivertex_program<graph_type, gather_type>, VertexProgram>::value) {
            throw "type parameter for async egnine is not derived from graphlab::ivertex_program";
        }
        vertex_states.resize(g.num_vertices(), vertex_state_type::FREE);
        gather_cache.resize(g.num_vertices());
        has_cache.resize(g.num_vertices(), false);

        // This cannot be resized since the copy constructor for std::condition_variable is deleted.
        cv_exclusive_access = std::vector<std::condition_variable>(g.num_vertices());
    }

    // called by the application programmer
    void signal_all();
    void start();

    // called by the context
    void internal_signal(const vertex_type& vertex);
    void internal_post_delta(const vertex_type& vertex, const gather_type& delta);
    void internal_clear_gather_cache(const vertex_type& vertex);



private:
    // ---------------------------------------- //
    // ------------- DATA MEMBERS ------------- //
    // ---------------------------------------- //
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
    const int num_threads = 4;
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
     */
    bool exclusive_access_possible(vertex_id_type vid);

    /**
     * Equiavalent to putdown(...) in the above-mentioned dining philosophers analogy.
     */
    void release_exclusive_access(vertex_id_type vid);
};

/**
 * implementation file (previously async_engine.cpp) copy-pasted below. Otherwise, template functions do not work
 */

using namespace std;

template<typename VertexProgram>
void async_engine<VertexProgram>::get_exclusive_access(vertex_id_type vid) {
    std::unique_lock<std::mutex> lock(scheduling_mutex);

    while (!exclusive_access_possible(vid)) {
        cv_exclusive_access[vid].wait(lock);
    }

    vertex_states[vid] = vertex_state_type::RUNNING;
}

template<typename VertexProgram>
bool async_engine<VertexProgram>::exclusive_access_possible(vertex_id_type vid) {
    // always called by get_exclusive_access, which readily holds mutex_exclusive access

    vertex_type v = g.vertex(vid);
    for (edge_type *e : v.in_edges) {   // can't get access if any in_neighbour is running
        if (vertex_states[e->source().id()] == vertex_state_type::RUNNING) {
            return false;
        }
    }
    for (edge_type *e : v.out_edges) {   // can't get access if any out_neighbour is running
        if (vertex_states[e->target().id()] == vertex_state_type::RUNNING) {
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
    for (edge_type *e : v.in_edges) {   // an in_neigbour may possibly start running
        cv_exclusive_access[e->source().id()].notify_all(); // TODO: notify_one should be enough. think about it.
    }
    for (edge_type *e : v.out_edges) {   // an out_neigbour may possibly start running
        cv_exclusive_access[e->target().id()].notify_all(); // TODO: notify_one should be enough. think about it.
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
            cout << "ERROR: running vertex signalled. A neighbour must have been running also" << endl;
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
        execute_vprog(job_vid);
        release_exclusive_access(job_vid);
        // TODO: right after releasing the mutex, get_next_job tries to acquire it again.
        // ?: can this be merged somehow to increase performance?
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

    if (caching_enabled && has_cache[vid]) {
        accum = gather_cache[vid];
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
            gather_cache[vid] = accum; 
            has_cache[vid] = true;
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

#endif