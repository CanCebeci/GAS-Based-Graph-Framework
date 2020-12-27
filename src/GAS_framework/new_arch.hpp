#ifndef __NEW_ARCH_H
#define __NEW_ARCH_H

#include <stdexcept>

namespace new_arch {
    typedef intptr_t size_t;
    typedef intptr_t spm_addr_type;
    typedef intptr_t word;

    const size_t SPM_SIZE = 256;    // in bytes

    // --- placeholder functions for special instructions ---
    // ! currently, these functions are not placeholders but implement access to an
    // ! in-memory simulation of SPM.
    // For simplicity, the memory is word-addressable right now. 
    // NBL2SPM and SPM2MEM work for sizes divisible by or less than 8 (word size)

    // a word is 8 bytes (intptr_t)
    word *SPM = new word[SPM_SIZE / sizeof(word)];
    
    // Non-blocking load to SPM
    void NBL2SPM(const void *mm_addr, spm_addr_type spm_addr, size_t size) {
        if (spm_addr % 8 > 0 || (size % 8 > 0 && size > 8)) {
            // not iplemented yet for simplicity
            throw std::runtime_error("NBL2SPM not word-aligned");
        }
        for (int i = 0; i < size / 8; i++) {
            SPM[spm_addr / 8 + i] = *(( word *)mm_addr + i);
        }
    }

    // Non-blocking store from SPM
    void SPM2MEM(const void *mm_addr, spm_addr_type spm_addr, size_t size) {
        if (spm_addr % 8 > 0 || (size % 8 > 0 && size > 8)) {
            // not iplemented yet for simplicity
            throw std::runtime_error("SPM2MEM not word-aligned");
        }

        for (int i = 0; i < size / 8; i++) {
            *((unsigned int *)mm_addr + i) = SPM[spm_addr / 8 + i] ;
        }
    }

    // Synchronous load to a register from SPM
    word SPM2REG(spm_addr_type spm_addr) {
        std::cerr << spm_addr << std::endl;
        // THIS ONE cAUSES SEGFALUT
        if (spm_addr % 8 > 0) {
            // not iplemented yet for simplicity
            throw std::runtime_error("SPM2REG not word-aligned");
        }
        return SPM[spm_addr / 8];
    }

    // Synchronous store to a SPM from a register
    void REG2SPM(spm_addr_type spm_addr, word value) {
        if (spm_addr == 1264) {
            std::cerr << "Write to 1264: " << value << std::endl; 
        }
        if (spm_addr % 8 > 0) {
            // not iplemented yet for simplicity
            throw std::runtime_error("REG2SPM not word-aligned");
        }
        SPM[spm_addr / 8] = value;
    }

    // Returns when all non-blocking memory requests are completed
    void BARRIER() { /* NOP */ }

    
}

#endif