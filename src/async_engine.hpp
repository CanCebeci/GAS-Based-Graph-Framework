/**
 * TODO: consider replacing active_list, job_queue and running_vertices
 *  with a more appropriate data structure.
 */

#ifndef __ASYNC_ENGINE_H
#define __ASYNC_ENGINE_H

#include "simple_graph.hpp"
#include "graphlab/vertex_program/ivertex_program.hpp"
#include "graphlab/vertex_program/context.hpp"

#include <unordered_set>
#include <unordered_map>
#include <type_traits>  //for is_base_of
#include <iostream>

#include <thread>
#include <mutex>
#include <condition_variable>

template<typename VertexProgram>
class async_engine {
public:
    // ---------------------------------------- //
    // --------------- TYPEDEFS --------------- //
    // ---------------------------------------- //
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

    // ---------------------------------------- //
    // ------------- DATA MEMBERS ------------- //
    // ---------------------------------------- //
    graph_type& g;
    std::unordered_set<vertex_id_type> active_list;  // possibly replace later with better data structure
                                                     // a bitset could be useful if vid's were consecutive.

    bool caching_enabled;
    /**
     * GraphLab uses vectors instead of maps for gather_cache and has_cache,
     * which also enables the use of the special vector<graphlab::empty>
     * However, this implementation does not use local vertex id's and the global
     * vertex id's are not necessarily consecutive. Thus, a map is used instead.
     * TODO: change this? (could result in faster retrieval)
     */
    std::unordered_map<vertex_id_type, gather_type> gather_cache;  
    std::unordered_map<vertex_id_type, bool> has_cache; // IMPORTANT: are bools default initialized to false?

    // creating a separate context for each veertex program is not necessary. This one is used by all of them.
    context_type context;   

    // ---------------------------------------- //
    // -------------- FUNCTIONS --------------- //
    // ---------------------------------------- //
    void signal_all();
    void start();

    // called by the context
    void internal_signal(const vertex_type& vertex);
    void internal_post_delta(const vertex_type& vertex, const gather_type& delta);
    void internal_clear_gather_cache(const vertex_type& vertex);

    
    async_engine(graph_type& g, bool enable_caching = false): g(g), 
                                                                caching_enabled(enable_caching),
                                                                context(*this, g), 
                                                                num_idle_threads(0) {
        if (!std::is_base_of<graphlab::ivertex_program<graph_type, gather_type>, VertexProgram>::value) {
            throw "type parameter for async egnine is not derived from graphlab::ivertex_program";
        }
    }

private:
    // ---------------------------------------- //
    // --- MULTITHREADING & SYNCHRONIZATION --- //
    // ---------------------------------------- //
    const int num_threads = 4;

    int num_idle_threads;

    /**
     * active_list, cond_no_jobs, running_vertices and deferred_activation_list
     * are accessed together when
     *  - singaling a vertex
     *  - a vertex program is finished
     *  - a thread needs a new job
     * 
     * scheduling_mutex regulates access to all four data members as a group to
     * avoid multiple locking overheads for each one. 
     * 
     * TODO: consider how much separation can be done more extensively.
     */
    std::mutex scheduling_mutex;

    /**
     * When a thread tries to get a job, job_queue is empty, and there exists at least one
     * busy thread, the thread asking for a job waits on cond_no_jobs. It is waken up when 
     * a vertex is signalled. 
     * Alternatively, it may be waken up when the last busy thread finds active_listy empty. 
     * In that case, no futher activation is possible. Each idle thread is waken up and they 
     * all fail to get jobs. After that they all quit. 
     */
    std::condition_variable cond_no_jobs;

    /**
     * A vertex is running if there is a thread currently executing its vertex program.
     * This set is used in order to ensure that if an running vertex is activated, it
     * does not get inserted into active_list until its execution is done.
     */
    std::unordered_set<vertex_id_type> running_vertices;

    /**
     * When an running vertex is signalled, it is placed into deferred_activation_set. When 
     * the vertex program finishes, it moves the vertex from this set into active_list. This 
     * ensures that the same vertex is not scheduled to two threads simultaneously.
     */
    std::unordered_set<vertex_id_type> deferred_activation_list;

    void thread_start();
    void execute_vprog(vertex_id_type vid);
    bool get_next_job(vertex_id_type& ret_vid);

};

/**
 * implementation file (previously async_engine.cpp) copy-pasted below. Otherwise, template functions do not work
 */

using namespace std;

template<typename VertexProgram>
void async_engine<VertexProgram>::internal_signal(const vertex_type& vertex) {
    // lock active list mutex within the whole function body.
    std::unique_lock<std::mutex> lock_AL(scheduling_mutex);

    const vertex_id_type vid = vertex.id();

    if (active_list.count(vid) == 0 && deferred_activation_list.count(vid) == 0) {  // if vertex is not already active
        printf("signaling %d\n", vid);
        if (running_vertices.count(vid) > 0) { 
            // if vertex is running, defer activation.
            deferred_activation_list.insert(vid);
        } else {
            active_list.insert(vid);
            cond_no_jobs.notify_one();  // an idle thread may wake up and find a job.
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
    for (typename unordered_map<vertex_id_type, vertex_type*>::iterator iter = g.vertices.begin(); 
                                                                iter != g.vertices.end(); iter++) {
        if (active_list.count(iter->first) == 0) {
            active_list.insert(iter->first);
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
    // lock active list mutex within the whole function body.
    std::unique_lock<std::mutex> lock_AL(scheduling_mutex);
    num_idle_threads++;
    while (active_list.empty() && (num_idle_threads < num_threads)) {
        // wait until some other thread, which may activate new vertices and create jobs, finishes running.
        printf("going to sleep\n");
        cond_no_jobs.wait(lock_AL);
    }
    if (active_list.empty()) {   // all other threads are idle, no further activation is possible.
        cond_no_jobs.notify_all();  // all idle threads should wake up and fail to get a job.
        // num_idle_threads is not decremented so that the other threads can properly fail.
        return false;
    } else {
        ret_vid = *(active_list.begin());
        active_list.erase(ret_vid);
        num_idle_threads--; // the thread is no longer idle
        running_vertices.insert(ret_vid);   // the vertex starts to run
        printf("scheduled vertex %d\n", ret_vid);
        return true;
    }
}

template<typename VertexProgram>
void async_engine<VertexProgram>::thread_start() {
    vertex_id_type job_vid;
    while (get_next_job(job_vid)) {
        execute_vprog(job_vid);

        {// start of mutex-protected block
            std::unique_lock<std::mutex> lock_AL(scheduling_mutex);
            // perform deferred activation if necessary.
            if (deferred_activation_list.count(job_vid) > 0) {  
                deferred_activation_list.erase(job_vid);
                active_list.insert(job_vid);
                cond_no_jobs.notify_one();  // an idle thread may wake up and find a job.
            }

            running_vertices.erase(job_vid);    // the vertex is no longer running
            printf("vertex done: %d\n", job_vid);
        }// end of mutex-protected block

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