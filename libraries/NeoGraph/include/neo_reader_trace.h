#pragma once
#include "utils/config.h"
#include "utils/spin_lock.h"
#include "utils/types.h"
#include <vector>
#include <stack>

namespace container {
    struct ReaderTraceBlock {
    private:
        std::atomic<uint64_t> atomic_value;

        static constexpr uint64_t LOCK_BIT = 63;
        static constexpr uint64_t LOCK_MASK = 1ULL << LOCK_BIT;

        static constexpr uint64_t STATUS_SHIFT = 60;
        static constexpr uint64_t STATUS_MASK = 0x7ULL << STATUS_SHIFT; // 3 bits for status

        static constexpr uint64_t TIMESTAMP_MASK = (1ULL << 60) - 1; // 60 bits for timestamp

    public:
        ReaderTraceBlock() : atomic_value(0) {}

        // Lock the ReaderTraceBlock
        void lock() {
            uint64_t expected;
            uint64_t desired;
            while (true) {
                expected = atomic_value.load(std::memory_order_relaxed);
                if (expected & LOCK_MASK) {
                    // Lock is already held by another thread, continue spinning
                    continue;
                }
                desired = expected | LOCK_MASK;
                if (atomic_value.compare_exchange_weak(expected, desired, std::memory_order_acquire)) {
                    // Acquired the lock
                    break;
                }
                // Optionally, you can insert a pause or yield here to reduce CPU usage
            }
        }

        void unlock() {
            atomic_value.fetch_and(~LOCK_MASK, std::memory_order_release);
        }

        bool try_lock() {
            uint64_t expected = atomic_value.load(std::memory_order_relaxed);
            uint64_t desired = expected | LOCK_MASK;
            return ((expected & LOCK_MASK) == 0) &&
                   atomic_value.compare_exchange_strong(expected, desired, std::memory_order_acquire);
        }

        uint8_t get_status() const {
            uint64_t value = atomic_value.load(std::memory_order_relaxed);
            return static_cast<uint8_t>((value & STATUS_MASK) >> STATUS_SHIFT);
        }

        void set_status(uint8_t status) {
            assert(status < 8); // Ensure status fits in 3 bits
            uint64_t expected;
            uint64_t desired;
            do {
                expected = atomic_value.load(std::memory_order_relaxed);
                desired = (expected & ~STATUS_MASK) | (static_cast<uint64_t>(status) << STATUS_SHIFT);
            } while (!atomic_value.compare_exchange_weak(expected, desired, std::memory_order_relaxed));
        }

        uint64_t get_timestamp() const {
            uint64_t value = atomic_value.load(std::memory_order_relaxed);
            return value & TIMESTAMP_MASK;
        }

        void set_timestamp(uint64_t timestamp) {
            assert(timestamp < (1ULL << 60)); // Ensure timestamp fits in 60 bits
            uint64_t expected;
            uint64_t desired;
            do {
                expected = atomic_value.load(std::memory_order_relaxed);
                desired = (expected & ~TIMESTAMP_MASK) | (timestamp & TIMESTAMP_MASK);
            } while (!atomic_value.compare_exchange_weak(expected, desired, std::memory_order_relaxed));
        }

        void clear() {
            atomic_value.store(0, std::memory_order_relaxed);
        }
    };

    struct ActiveReaderTracer{
        std::array<ReaderTraceBlock, INIT_READER_NUM> blocks{};

        ReaderTraceBlock* reader_register();

        void set_status(ReaderTraceBlock* block, uint64_t status);

        void set_timestamp(ReaderTraceBlock* block, uint64_t timestamp);

        void reader_unregister(ReaderTraceBlock* block);

        void get_active_reader_info(std::vector<uint64_t>&readers);

        uint64_t get_min_timestamp();
    };

    struct ARTNode_48;
    struct ARTNode_256;

    struct WriterTraceBlock {
        SpinLock lock;
        std::stack<RangeElementSegment_t*>* range_element_segments;
//        std::stack<InRangeElementSegment_t*>* in_range_element_segments;
        std::stack<VertexMap_t*>* vertex_maps;
        std::stack<std::array<uint32_t, ART_LEAF_SIZE>*>* art_leaf32s;
        std::stack<std::array<uint64_t, ART_LEAF_SIZE>*>* art_leaf64s;
        std::stack<ARTNode_48*>* art_node48s;
        std::stack<ARTNode_256*>* art_node256s;
#if EDGE_PROPERTY_NUM > 0
        std::stack<PropertyVec<RANGE_LEAF_SIZE>*>* range_prop_vecs;
        std::stack<PropertyVec<ART_LEAF_SIZE>*>* art_prop_vecs;
#endif

        RangeElementSegment_t* allocate_range_element_segment();
//        InRangeElementSegment_t* allocate_inrange_element_segment();
        VertexMap_t* allocate_vertex_map();
        std::array<uint32_t, ART_LEAF_SIZE>* allocate_art_leaf32();
        std::array<uint64_t, ART_LEAF_SIZE>* allocate_art_leaf64();
        ARTNode_48* allocate_art_node48();
        ARTNode_256* allocate_art_node256();

        void deallocate_range_element_segment(RangeElementSegment_t *segment);
//        void deallocate_inrange_element_segment(InRangeElementSegment_t *segment);
        void deallocate_vertex_map(VertexMap_t *segment);
        void deallocate_art_leaf32(std::array<uint32_t, ART_LEAF_SIZE> *leaf);
        void deallocate_art_leaf64(std::array<uint64_t, ART_LEAF_SIZE> *leaf);
        void deallocate_art_node48(ARTNode_48 *node);
        void deallocate_art_node256(ARTNode_256 *node);


#if EDGE_PROPERTY_NUM > 0
        PropertyVec<RANGE_LEAF_SIZE>* allocate_range_prop_vec();
        PropertyVec<ART_LEAF_SIZE>* allocate_art_prop_vec();

        void deallocate_range_prop_vec(PropertyVec<RANGE_LEAF_SIZE> *vec);
        void deallocate_art_prop_vec(PropertyVec<ART_LEAF_SIZE> *vec);
#endif
    };

    struct ActiveWriterTracer{
        std::array<WriterTraceBlock*, INIT_WRITER_NUM> blocks{};

        ActiveWriterTracer();
        ~ActiveWriterTracer();

        WriterTraceBlock* writer_register();

        void writer_batch_register(uint64_t writer_num);

        void writer_unregister(WriterTraceBlock* block);
    };

    void add_read_txn_num();

    void dec_read_txn_num();

    uint64_t get_read_txn_num();

    // global tracer
    ReaderTraceBlock* reader_register();

    void reader_unregister(ReaderTraceBlock* block);

    void set_status(ReaderTraceBlock* block, uint64_t status);

    void set_timestamp(ReaderTraceBlock* block, uint64_t timestamp);

    void get_active_reader_info(std::vector<uint64_t>&readers);

    uint64_t get_min_timestamp();

    WriterTraceBlock* writer_register();

    void writer_batch_register(uint64_t writer_num);

    WriterTraceBlock* get_trace_block(uint64_t idx);

    void writer_unregister(WriterTraceBlock* block);
}