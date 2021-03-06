/**
 * Improvements to be implemented
 * 
 * * Right now, the edge and vertex data and the main memory addresses
 * are assumed to be SPM-word-sized (currently 8 bytes).
 * 
 * * Shrinking of the opposite slab during load may not work if edge and
 * vertex data sizes are very different (if one's slot size is 2 times 
 * the other or more). Fix this when removing the above assumption.
 * 
 * * Implement dirty-checks when removing data instead of writing
 * back to the main memory all the time.
 */

#ifndef __SPM_INTERFACE_H
#define __SPM_INTERFACE_H

#include "new_arch.hpp"
#include "simple_graph.hpp"

#include <stdexcept>
#include <mutex>

using namespace new_arch;

#define SPM_POINTER_SZ      sizeof(spm_addr_type)
#define ADDR_VSLAB_END      0
#define ADDR_VEMPTY_HEAD    SPM_POINTER_SZ
#define ADDR_ESLAB_END      (2 * SPM_POINTER_SZ)
#define ADDR_EEMPTY_HEAD    (3 * SPM_POINTER_SZ)

#define VSLAB_START         (4 * SPM_POINTER_SZ)
#define SPM_NULL            spm_addr_type(0)    // 0 is reserved, can be considered as null

int num_esq = 0; // test 
int num_v = 0; // test 
int num_e = 0; // test

std::string in;

template<typename GraphType>
class spm_interface {

    typedef typename GraphType::vertex_id_type vertex_id_type;
    typedef typename GraphType::vertex_type vertex_type;
    typedef typename GraphType::edge_type edge_type;
    typedef typename GraphType::vertex_data_type vertex_data_type;
    typedef typename GraphType::edge_data_type edge_data_type;


    // ! turn these into macros so that additional memory access is not necessary.
    const new_arch::size_t v_slot_size = sizeof(vertex_data_type) + sizeof(vertex_data_type *);  
    const new_arch::size_t e_slot_size = sizeof(edge_data_type) + sizeof(edge_data_type *);

    /**
     * The mutexes below regulate insertion to and deletio from their respective slabs.
     * Access to individual slots is not regulated since it is the engine's responsiblity
     * to ensure that two threads never access the same edge or vertex data simultaneously
     */
    std::mutex vslab_mutex;
    std::mutex eslab_mutex;


    /**
     * read_x and write_x normally do not have to be executed in mutual exclusion with
     * load_x and remove_x since the latter two do not modify existing data in SPM.
     * However, load_x may result in the opposite being shrunk, in which case it may
     * cause errors in read_x and write_x. The two mutexes below are used to lock in that
     * specific case. 
     */
    std::mutex vslot_reloc_mutex;
    std::mutex eslot_reloc_mutex;

public:

    long int num_failed_loads = 0;  // test-purpose counter

    /**
     * constructor initializes the fixed-size metadata at the beginning of SPM.
     */
    spm_interface() {
        REG2SPM(ADDR_VSLAB_END, VSLAB_START);  // store v_slab_end
        REG2SPM(ADDR_VEMPTY_HEAD, SPM_NULL);  // store v_empty_head
        REG2SPM(ADDR_ESLAB_END, SPM_SIZE - e_slot_size); // store e_slab_end
        REG2SPM(ADDR_EEMPTY_HEAD, SPM_NULL); // store e_empty_head
    }

    // ------------------------------------------ //
    // ---- FUNCTIONS RELATED TO VERTEX DATA ---- //
    // ------------------------------------------ //
    /**
    void print_vslab_info() {
        std::cerr << "vslab_end..." << (spm_addr_type) SPM2REG(ADDR_VSLAB_END) << std::endl;

        int len = 0;
        spm_addr_type cur = SPM2REG(ADDR_VEMPTY_HEAD);
        while (cur != SPM_NULL) {
            len++;
            cur = SPM2REG(cur + sizeof(vertex_data_type *));
        }
        std::cerr << "vempty len..." << len << std::endl;
    }
    void print_eslab_info() {
        std::cerr << "eslab_end..." << (spm_addr_type) SPM2REG(ADDR_ESLAB_END) << std::endl;

        int len = 0;
        spm_addr_type cur = SPM2REG(ADDR_EEMPTY_HEAD);
        while (cur != SPM_NULL) {
            len++;
            cur = SPM2REG(cur + sizeof(edge_data_type *));
        }
        std::cerr << "eempty len..." << len << std::endl;
    }
    */
    /**
     * Brings vertex data from the main memory to SPM.
     * Returns false is SPM is full and data can not be loaded.
     * Returns false if vdata is already in SPM. Normally, load_vdata
     * is not called in such a case since neighboring vertices will not
     * execute simultaneously. May only happen when two vertices are
     * doubly-linked and one has too few neigbors (so that vdata is loaded
     * both as an in_neigh and out_neigh).
     */
    bool load_vdata(const vertex_type &v) {
        if (find_vdata(v) != SPM_NULL) {
            return false;
        }
        // ensure exclusive access to VEMPTY_HEAD and VSLAB_END
        std::unique_lock<std::mutex> lock(vslab_mutex);
        // if there is an empty slot in the vertex slab, store there
        spm_addr_type head = (spm_addr_type) SPM2REG(ADDR_VEMPTY_HEAD);
        if (head != SPM_NULL) {
            // advance VEMPTY_HEAD
            spm_addr_type tail = SPM2REG(head + sizeof(vertex_data_type *));
            REG2SPM(ADDR_VEMPTY_HEAD, tail);

            internal_load_vdata(v, head);
            return true;
        }

        // if extending the vertex slab will not cause collision with the edge slab
        spm_addr_type end = (spm_addr_type) SPM2REG(ADDR_VSLAB_END);
            // uppermost edge slab entry begins at ESLAB_END + e_slot_size
        if (end + v_slot_size <= SPM2REG(ADDR_ESLAB_END) + e_slot_size) {
            // extend vertex slab
            REG2SPM(ADDR_VSLAB_END, end + v_slot_size);

            internal_load_vdata(v, end);
            return true;
        }

        // if there are empty slots in the edge slab, compress it.
        {   // local scope for the locks

            std::unique_lock<std::mutex> lock2(eslab_mutex);
            std::unique_lock<std::mutex> lock3(eslot_reloc_mutex);

            spm_addr_type edge_head = (spm_addr_type) SPM2REG(ADDR_EEMPTY_HEAD);
            if (edge_head != SPM_NULL) {
                num_esq++;
                spm_addr_type edge_end = SPM2REG(ADDR_ESLAB_END);
                word end_mm_addr = SPM2REG(edge_end + e_slot_size);

                if (end_mm_addr == SPM_NULL) {  // if the last slot is an empty slot
                    // scan linked list until the parent of the last slot is found
                    // TODO: if two SPM pointers fit into a slot, use a doubly linked list to avoid linear scan
                    spm_addr_type cur = SPM2REG(ADDR_EEMPTY_HEAD);
                    if (cur == edge_end + e_slot_size) {  // last slot is head
                        REG2SPM(ADDR_EEMPTY_HEAD, SPM_NULL);
                    } else {
                        //cur = SPM2REG(cur + sizeof(edge_data_type *));  // advance cur
                        while (cur != SPM_NULL &&  SPM2REG(cur + sizeof(edge_data_type *)) != edge_end + e_slot_size) {
                            cur = SPM2REG(cur + sizeof(edge_data_type *));
                        }
                        if (cur != SPM_NULL) {  // cur is the parent of the last node
                            spm_addr_type grandchild = SPM2REG(edge_end + e_slot_size + sizeof(edge_data_type *));
                            REG2SPM(cur + sizeof(edge_data_type *), grandchild); // make cur point to grandchild
                        }
                    }
                } else {
                    // advance edge head
                    spm_addr_type edge_tail = SPM2REG(edge_head + sizeof(edge_data_type *));
                    REG2SPM(ADDR_EEMPTY_HEAD, edge_tail);

                    // move last edge slot to edge empty head
                    word end_data = SPM2REG(edge_end + e_slot_size + sizeof(edge_data_type *));
                    REG2SPM(edge_head, end_mm_addr);
                    REG2SPM(edge_head + sizeof(edge_data_type *), end_data);
                }
                // shrink edge slab
                REG2SPM(ADDR_ESLAB_END, edge_end + e_slot_size);

                // load to the end of v_slab
                REG2SPM(ADDR_VSLAB_END, end + v_slot_size);
 
                internal_load_vdata(v, end);    

                return true;
            }
        }
        num_failed_loads++;

        return false;
    }

    /**
     * Writes vertex data from SPM back to main memory and
     * de-allocates the slot occupied by it.
     * 
     * Returns false if vertex data is not in SPM.
     */
    bool remove_vdata(const vertex_type &v) {
        // ensure exclusive access to VEMPTY_HEAD and VSLAB_END
        std::unique_lock<std::mutex> lock(vslab_mutex);

        spm_addr_type rm_addr = find_vdata(v);
        if (rm_addr == SPM_NULL) {
            return false;
        }
        // store back to main memory before removal
        // ! a dirty check may be implemented in the future. Always write back for now.
        // !!!! write backs are eliminated for the hit/miss tests in order to run the tests faster
        // !!!! GAS functions operate purely on the main memory copy.
        SPM2MEM(&(v.data()), rm_addr + sizeof(vertex_data_type *), sizeof(vertex_data_type));

        if (rm_addr + v_slot_size == SPM2REG(ADDR_VSLAB_END)) {
            // removal from the end, shrink vertex slab.
            REG2SPM(ADDR_VSLAB_END, rm_addr);
        } else {
            // add the freed slot to the empty list
            spm_addr_type head = SPM2REG(ADDR_VEMPTY_HEAD);
            REG2SPM(rm_addr, SPM_NULL); // put empty marker into freed slot
            REG2SPM(rm_addr + sizeof(vertex_data_type *), head); // link head to freed slot
            REG2SPM(ADDR_VEMPTY_HEAD, rm_addr); // change head pointer to freed slot
        }
        return true;
    }

    /**
     * For vdata that fits into a word. Reads the value from SPM.
     * Returns false if vdata not present in SPM
     * The data read is returned in argument ret_data.
     */
    bool read_vdata(const vertex_type &v, vertex_data_type &ret_data) {
        std::unique_lock<std::mutex> lock(vslot_reloc_mutex);
        spm_addr_type addr = find_vdata(v);
        if (addr == SPM_NULL) {
            return false;
        } else {
            ret_data = vertex_data_type(SPM2REG(addr + sizeof(vertex_data_type *)));
            return true;
        }
    }

    /**
     * For vdata that fits into a word. Writes the value to SPM.
     * Returns false if vdata not present in SPM
     * The data read is returned in argument ret_data..
     */
    bool write_vdata(const vertex_type &v, const vertex_data_type &w_data) {
        std::unique_lock<std::mutex> lock(vslot_reloc_mutex);
        spm_addr_type addr = find_vdata(v);
        if (addr == SPM_NULL) {
            return false;
        } else {
            REG2SPM(addr + sizeof(vertex_data_type *), word(w_data));
            return true;
        }
    }

    // ---------------------------------------- //
    // ---- FUNCTIONS RELATED TO EDGE DATA ---- //
    // ---------------------------------------- //

    /**
     * Brings edge data from the main memory to SPM.
     * 
     * Returns false is SPM is full and data can not be loaded.
     * ! should not be called for edata already in SPM.
     * TODO: implement a check for the line above
     */
    bool load_edata(const edge_type &e) {
        {
            // ensure exclusive access to EEMPTY_HEAD and ESLAB_END
            std::unique_lock<std::mutex> lock(eslab_mutex);

            // if there is an empty slot in the edge slab, store there
            spm_addr_type head = (spm_addr_type) SPM2REG(ADDR_EEMPTY_HEAD);
            if (head != SPM_NULL) {
                // advance EEMPTY_HEAD
                spm_addr_type tail = SPM2REG(head + sizeof(edge_data_type *));
                REG2SPM(ADDR_EEMPTY_HEAD, tail);

                internal_load_edata(e, head);
                return true;
            }
            // if extending the edge slab will not cause collision with the vertex slab
            spm_addr_type end = (spm_addr_type) SPM2REG(ADDR_ESLAB_END);
            if (end - e_slot_size >= SPM2REG(ADDR_VSLAB_END)) {
                // extend edge slab
                REG2SPM(ADDR_ESLAB_END, end - e_slot_size);
                internal_load_edata(e, end);
                return true;
            }
        }   // end of local scope with e_slab_mutex. Need to release it and re-acqquire after
            // vslab_mutex in the next local scope to avoid deadlocks.

        // if there are empty slots in the vertex slab, compress it.
        {   // local scope for the lock on vslab_mutex
            std::unique_lock<std::mutex> lock1(vslab_mutex);
            std::unique_lock<std::mutex> lock2(eslab_mutex);
            std::unique_lock<std::mutex> lock3(vslot_reloc_mutex);
            spm_addr_type vertex_head = (spm_addr_type) SPM2REG(ADDR_VEMPTY_HEAD);
            if (vertex_head != SPM_NULL) {
                spm_addr_type vertex_end = SPM2REG(ADDR_VSLAB_END);
                word end_mm_addr = SPM2REG(vertex_end - v_slot_size);
                if (end_mm_addr == SPM_NULL) {  // if the last slot is an empty slot
                    // scan linked list until the parent of the last slot is found
                    // TODO: if two SPM pointers fit into a slot, use a doubly linked list to avoid linear scan
                    spm_addr_type cur = SPM2REG(ADDR_VEMPTY_HEAD);
                    if (cur == vertex_end - v_slot_size) {  // last slot is head
                        REG2SPM(ADDR_VEMPTY_HEAD, SPM_NULL);
                    } else {
                        //cur = SPM2REG(cur + sizeof(vertex_data_type *));  // advance cur
                        while (cur != SPM_NULL &&  SPM2REG(cur + sizeof(vertex_data_type *)) != vertex_end - v_slot_size) {
                            cur = SPM2REG(cur + sizeof(vertex_data_type *));
                        }
                        if (cur != SPM_NULL) {  // cur is the parent of the last node
                            spm_addr_type grandchild = SPM2REG(vertex_end - v_slot_size + sizeof(vertex_data_type *));
                            REG2SPM(cur + sizeof(vertex_data_type *), grandchild); // make cur point to grandchild
                        }
                    }
                } else {
                    // advance vertex head
                    spm_addr_type vertex_tail = SPM2REG(vertex_head + sizeof(vertex_data_type *));
                    REG2SPM(ADDR_VEMPTY_HEAD, vertex_tail);

                    // move last vertex slot to vertex empty head
                    word end_data = SPM2REG(vertex_end - v_slot_size + sizeof(edge_data_type *));
                    REG2SPM(vertex_head, end_mm_addr);
                    REG2SPM(vertex_head + sizeof(edge_data_type *), end_data);
                }
                // shrink vertex slab
                REG2SPM(ADDR_VSLAB_END, vertex_end - v_slot_size);

                // load to the end of e_slab
                spm_addr_type end = (spm_addr_type) SPM2REG(ADDR_ESLAB_END);
                REG2SPM(ADDR_ESLAB_END, end - e_slot_size);
                internal_load_edata(e, end);    

                return true;
            }
        }
        num_failed_loads++;
        return false;
    }

    /**
     * Writes edge data from SPM back to main memory and
     * de-allocates the slot occupied by it.
     * 
     * Returns false if edge data is not in SPM.
     */
    bool remove_edata(const edge_type &e) {
        // ensure exclusive access to EEMPTY_HEAD and ESLAB_END
        std::unique_lock<std::mutex> lock(eslab_mutex);

        spm_addr_type rm_addr = find_edata(e);
        if (rm_addr == SPM_NULL) {
            return false;
        }
        // store back to main memory before removal
        // ! a dirty check may be implemented in the future. Always write back for now.
        // !!!! write backs are eliminated for the hit/miss tests in order to run the tests faster
        // !!!! GAS functions operate purely on the main memory copy.
        //SPM2MEM(&(e.data()), rm_addr + sizeof(edge_data_type *), sizeof(edge_data_type));

        if (rm_addr - e_slot_size == SPM2REG(ADDR_ESLAB_END)) {
            // removal from the end, shrink edge slab.
            REG2SPM(ADDR_ESLAB_END, rm_addr);
        } else {
            // add the freed slot to the empty list
            spm_addr_type head = SPM2REG(ADDR_EEMPTY_HEAD);
            REG2SPM(rm_addr, SPM_NULL); // put empty marker into freed slot
            REG2SPM(rm_addr + sizeof(edge_data_type *), head); // link head to freed slot
            REG2SPM(ADDR_EEMPTY_HEAD, rm_addr); // change head pointer to freed slot
        }
        return true;
    }

    /**
     * For edata that fits into a word. Reads the value from SPM.
     * Returns false if edata not present in SPM
     * The data read is returned in argument ret_data.
     */
    bool read_edata(const edge_type &e, edge_data_type &ret_data) {
        std::unique_lock<std::mutex> lock(eslot_reloc_mutex);
        spm_addr_type addr = find_edata(e);
        if (addr == SPM_NULL) {
            return false;
        } else {
            ret_data = edge_data_type(SPM2REG(addr + sizeof(edge_data_type *)));
            return true;
        }
    }

    /**
     * For edata that fits into a word. Writes the value to SPM.
     * Returns false if edata not present in SPM
     * The data read is returned in argument ret_data..
     */
    bool write_edata(const edge_type &e, const edge_data_type &w_data) {
        std::unique_lock<std::mutex> lock(eslot_reloc_mutex);
        spm_addr_type addr = find_edata(e);
        if (addr == SPM_NULL) {
            return false;
        } else {
            REG2SPM(addr + sizeof(edge_data_type *), word(w_data));
            return true;
        }
    }

private:
    /**
     * Helper function that does a linear search overthe vertex slab 
     * and locates the slot which contains the data of argument vertex.
     * Returns SPM_NULL if vdata is not in SPM.
     */
    spm_addr_type find_vdata(const vertex_type &v) {
        spm_addr_type cur = VSLAB_START;
        spm_addr_type end = SPM2REG(ADDR_VSLAB_END);
        while (cur < end) {
            if (SPM2REG(cur) != SPM_NULL && SPM2REG(cur) == (word) &(v.data())) {
                return cur;
            }
            cur += v_slot_size;
        }
        return SPM_NULL;
    }

    /**
     * Helper function used by load_vdata once the spm_addr for the load
     * is determined.
     */
    void internal_load_vdata(const vertex_type &v, spm_addr_type addr) {
        REG2SPM(addr, (word) &(v.data())); // store mm_address to SPM
        NBL2SPM(&(v.data()), addr + sizeof(vertex_data_type *), sizeof(vertex_data_type)); // load vdata to SPM
    }

    /**
     * Helper function that does a linear search over the edge slab 
     * and locates the slot which contains the data of argument edge.
     * Returns SPM_NULL if edata is not in SPM.
     */
    spm_addr_type find_edata(const edge_type &e) {
        spm_addr_type cur = SPM_SIZE - e_slot_size;
        spm_addr_type end = SPM2REG(ADDR_ESLAB_END);
        while (cur > end) {
            if (SPM2REG(cur) != SPM_NULL && SPM2REG(cur) == (word) &(e.data())) {
                return cur;
            }
            cur -= e_slot_size;
        }
        return SPM_NULL;
    }
    /**
     * Helper function used by load_edata once the spm_addr for the load
     * is determined.
     */
    void internal_load_edata(const edge_type &e, spm_addr_type addr) {
        REG2SPM(addr, (word) &(e.data())); // store mm_address to SPM
        NBL2SPM(&(e.data()), addr + sizeof(edge_data_type *), sizeof(edge_data_type)); // load edata to SPM
    }
};

#endif