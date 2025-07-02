#include <algorithm>
#include "include/neo_reader_trace.h"
#include "utils/c_art/include/art_node.h"
//#include "utils/art_new/include/art_node.h"

namespace container{
    ReaderTraceBlock* ActiveReaderTracer::reader_register() {
        for(auto & block: blocks) {
            if(block.get_status() == 0 && block.try_lock()) {
                block.set_status(1);   // acquiring
                return &block;
            }
        }
        return nullptr;
    }

    void ActiveReaderTracer::set_status(ReaderTraceBlock* block, uint64_t status) {
        block->set_status(1);
    }

    void ActiveReaderTracer::set_timestamp(ReaderTraceBlock* block, uint64_t timestamp) {
        block->set_timestamp(timestamp);
        block->unlock();
    }

    void ActiveReaderTracer::reader_unregister(ReaderTraceBlock* block) {
        block->lock();
        block->clear();
    }

    void ActiveReaderTracer::get_active_reader_info(std::vector<uint64_t>&readers) {
        for(auto &block: blocks) {
            if(block.get_status() == 1) {
                uint64_t timestamp;
                do {
                    timestamp = block.get_timestamp();
                } while(timestamp == 0);
                readers.push_back(timestamp);
            }
        }
        std::sort(readers.begin(), readers.end());
        readers.erase(std::unique(readers.begin(), readers.end()), readers.end());
    }

    uint64_t ActiveReaderTracer::get_min_timestamp() {
        uint64_t min_timestamp = std::numeric_limits<uint64_t>::max();
        for(auto &block: blocks) {
            if(block.get_status() == 1) {
                block.lock();
                auto timestamp = block.get_timestamp();
                block.unlock();
                if(timestamp < min_timestamp) {
                    min_timestamp = timestamp;
                }
            }
        }
        return min_timestamp;
    }

    RangeElementSegment_t* WriterTraceBlock::allocate_range_element_segment() {
        RangeElementSegment_t* res = nullptr;
        if(range_element_segments->empty()) {
            res = (RangeElementSegment_t*)malloc(sizeof(RangeElementSegment_t));
        } else {
            res = range_element_segments->top();
            range_element_segments->pop();
        }
        memset(res, 0, sizeof(RangeElementSegment_t));
        res->ref_cnt = 1;
        return res;
    }

//    InRangeElementSegment_t* WriterTraceBlock::allocate_inrange_element_segment() {
//        InRangeElementSegment_t* res = nullptr;
//        if(in_range_element_segments->empty()) {
//            res = new InRangeElementSegment_t();
//        } else {
//            res = in_range_element_segments->top();
//            in_range_element_segments->pop();
//        }
//        memset(res, 0, sizeof(InRangeElementSegment_t));
//        res->ref_cnt = 1;
//        return res;
//    }
    
    VertexMap_t* WriterTraceBlock::allocate_vertex_map() {
        VertexMap_t* res = nullptr;
        if(vertex_maps->empty()) {
            res = new VertexMap_t();
        } else {
            res = vertex_maps->top();
            vertex_maps->pop();
        }
        memset(res, 0, sizeof(VertexMap_t));
        return res;
    }

    std::array<uint32_t, ART_LEAF_SIZE>* WriterTraceBlock::allocate_art_leaf32() {
        std::array<uint32_t, ART_LEAF_SIZE> *res = nullptr;
        if(art_leaf32s->empty()) {
            res = new std::array<uint32_t, ART_LEAF_SIZE>();
        } else {
            res = art_leaf32s->top();
            art_leaf32s->pop();
        }
        memset(res, 0, sizeof(std::array<uint32_t, ART_LEAF_SIZE>));
        return res;
    }

    std::array<uint64_t, ART_LEAF_SIZE>* WriterTraceBlock::allocate_art_leaf64() {
        std::array<uint64_t, ART_LEAF_SIZE> *res = nullptr;
        if(art_leaf64s->empty()) {
            res = new std::array<uint64_t, ART_LEAF_SIZE>();
        } else {
            res = art_leaf64s->top();
            art_leaf64s->pop();
        }
        memset(res, 0, sizeof(std::array<uint64_t, ART_LEAF_SIZE>));
        return res;
    }

    ARTNode_48* WriterTraceBlock::allocate_art_node48() {
        ARTNode_48* res = nullptr;
        if(art_node48s->empty()) {
            res = new ARTNode_48();
        } else {
            res = art_node48s->top();
            art_node48s->pop();
        }
        memset(res, 0, sizeof(ARTNode_48));
        res->n.ref_cnt = 1;
        return res;
    }

    ARTNode_256* WriterTraceBlock::allocate_art_node256() {
        ARTNode_256* res = nullptr;
        if(art_node256s->empty()) {
            res = new ARTNode_256();
        } else {
            res = art_node256s->top();
            art_node256s->pop();
        }
        memset(res, 0, sizeof(ARTNode_256));
        res->n.ref_cnt = 1;
        return res;
    }

#if EDGE_PROPERTY_NUM > 0
    PropertyVec<RANGE_LEAF_SIZE>* WriterTraceBlock::allocate_range_prop_vec() {
        PropertyVec<RANGE_LEAF_SIZE>* res = nullptr;
        if(range_prop_vecs->empty()) {
            res = new PropertyVec<RANGE_LEAF_SIZE>();
        } else {
            res = range_prop_vecs->top();
            range_prop_vecs->pop();
        }
        return res;
    }

    PropertyVec<ART_LEAF_SIZE>* WriterTraceBlock::allocate_art_prop_vec() {
        PropertyVec<ART_LEAF_SIZE>* res = nullptr;
        if(art_prop_vecs->empty()) {
            res = new PropertyVec<ART_LEAF_SIZE>();
        } else {
            res = art_prop_vecs->top();
            art_prop_vecs->pop();
        }
        return res;
    }
#endif

    void WriterTraceBlock::deallocate_range_element_segment(RangeElementSegment_t *segment) {
        range_element_segments->push(segment);
    }

//    void WriterTraceBlock::deallocate_inrange_element_segment(InRangeElementSegment_t *segment) {
//        in_range_element_segments->push(segment);
//    }

    void WriterTraceBlock::deallocate_vertex_map(VertexMap_t *segment) {
        vertex_maps->push(segment);
    }

    void WriterTraceBlock::deallocate_art_leaf32(std::array<uint32_t, ART_LEAF_SIZE> *leaf) {
        art_leaf32s->push(leaf);
    }

    void WriterTraceBlock::deallocate_art_leaf64(std::array<uint64_t, ART_LEAF_SIZE> *leaf) {
        art_leaf64s->push(leaf);
    }

    void WriterTraceBlock::deallocate_art_node48(ARTNode_48 *node) {
        art_node48s->push(node);
    }

    void WriterTraceBlock::deallocate_art_node256(ARTNode_256 *node) {
        art_node256s->push(node);
    }

#if EDGE_PROPERTY_NUM > 0
    void WriterTraceBlock::deallocate_range_prop_vec(PropertyVec<RANGE_LEAF_SIZE> *vec) {
        range_prop_vecs->push(vec);
    }

    void WriterTraceBlock::deallocate_art_prop_vec(PropertyVec<ART_LEAF_SIZE> *vec) {
        art_prop_vecs->push(vec);
    }
#endif

    
    ActiveWriterTracer::ActiveWriterTracer() {
        for(uint64_t i = 0; i < INIT_WRITER_NUM; i++) {
            blocks[i] = new WriterTraceBlock();
        }
    }

    ActiveWriterTracer::~ActiveWriterTracer() {
        for(auto & block: blocks) {
            delete block;
        }
    }

    WriterTraceBlock* ActiveWriterTracer::writer_register() {
        while(true) {
            for (auto &block: blocks) {
                if (block->lock.try_lock()) {
                    block->range_element_segments = new std::stack<RangeElementSegment_t *>();
                    block->vertex_maps = new std::stack<VertexMap_t *>();
                    block->art_leaf32s = new std::stack<std::array<uint32_t, ART_LEAF_SIZE> *>();
                    block->art_leaf64s = new std::stack<std::array<uint64_t, ART_LEAF_SIZE> *>();
                    block->art_node48s = new std::stack<ARTNode_48 *>();
                    block->art_node256s = new std::stack<ARTNode_256 *>();
#if EDGE_PROPERTY_NUM > 0
                    block->range_prop_vecs = new std::stack<PropertyVec<RANGE_LEAF_SIZE> *>();
                    block->art_prop_vecs = new std::stack<PropertyVec<ART_LEAF_SIZE> *>();
#endif
                    return block;
                }
            }
        }
        return nullptr;
    }

    void ActiveWriterTracer::writer_batch_register(uint64_t writer_num) {

    }

    void ActiveWriterTracer::writer_unregister(WriterTraceBlock* block) {
        while(!block->range_element_segments->empty()) {
            free(block->range_element_segments->top());
            block->range_element_segments->pop();
        }
        delete block->range_element_segments;
//        while(!block->in_range_element_segments->empty()) {
//            delete block->in_range_element_segments->top();
//            block->in_range_element_segments->pop();
//        }
//        delete block->in_range_element_segments;
        while(!block->vertex_maps->empty()) {
            delete block->vertex_maps->top();
            block->vertex_maps->pop();
        }
        delete block->vertex_maps;
        while(!block->art_leaf32s->empty()) {
            delete block->art_leaf32s->top();
            block->art_leaf32s->pop();
        }
        delete block->art_leaf32s;
        while(!block->art_leaf64s->empty()) {
            delete block->art_leaf64s->top();
            block->art_leaf64s->pop();
        }
        delete block->art_leaf64s;
        while(!block->art_node48s->empty()) {
            delete block->art_node48s->top();
            block->art_node48s->pop();
        }
        delete block->art_node48s;
        while(!block->art_node256s->empty()) {
            delete block->art_node256s->top();
            block->art_node256s->pop();
        }
        delete block->art_node256s;
#if EDGE_PROPERTY_NUM > 0
        while(!block->range_prop_vecs->empty()) {
            delete block->range_prop_vecs->top();
            block->range_prop_vecs->pop();
        }
        delete block->range_prop_vecs;
        while(!block->art_prop_vecs->empty()) {
            delete block->art_prop_vecs->top();
            block->art_prop_vecs->pop();
        }
        delete block->art_prop_vecs;
#endif
        block->lock.unlock();
    }

    std::atomic<uint64_t> read_txn_num;

    void add_read_txn_num() {
        read_txn_num += 1;
    }

    void dec_read_txn_num() {
        read_txn_num -= 1;
    }

    uint64_t get_read_txn_num() {
        return read_txn_num;
    }

    ActiveReaderTracer global_tracer;

    ActiveWriterTracer global_writer_tracer;

    ReaderTraceBlock* reader_register() {
        return global_tracer.reader_register();
    }

    void reader_unregister(ReaderTraceBlock* block) {
        global_tracer.reader_unregister(block);
    }

    void set_status(ReaderTraceBlock* block, uint64_t status) {
        global_tracer.set_status(block, status);
    }

    void set_timestamp(ReaderTraceBlock* block, uint64_t timestamp) {
        global_tracer.set_timestamp(block, timestamp);
    }

    void get_active_reader_info(std::vector<uint64_t>&readers) {
        global_tracer.get_active_reader_info(readers);
    }

    uint64_t get_min_timestamp() {
        return global_tracer.get_min_timestamp();
    }

    WriterTraceBlock* writer_register() {
        return global_writer_tracer.writer_register();
    }

    void writer_batch_register(uint64_t writer_num) {
        return global_writer_tracer.writer_batch_register(writer_num);
    }

    WriterTraceBlock* get_trace_block(uint64_t idx) {
        return global_writer_tracer.blocks.at(idx);
    }

    void writer_unregister(WriterTraceBlock* block) {
        global_writer_tracer.writer_unregister(block);
    }
}