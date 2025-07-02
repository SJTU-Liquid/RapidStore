#include <memory>

#include "include/neo_index.h"
#include "utils/helper.h"
#include "utils/thread_pool.h"

namespace container {
    NeoGraphIndex::NeoGraphIndex() {
        forest = new std::vector<std::unique_ptr<NeoTree>>();
    }

    NeoGraphIndex::~NeoGraphIndex() {
        delete forest;
    }

    NeoTree* NeoGraphIndex::lock(uint64_t direction) {
        if(forest->size() <= direction) {
            forest->resize(direction + 1);
        }
        auto raw_direction = forest->at(direction).get();
        if(raw_direction == nullptr) {
            return nullptr;
        }
        raw_direction->writer_lock.lock();
        return raw_direction;
    }

    void NeoGraphIndex::unlock(uint64_t direction) {
        auto raw_direction = forest->at(direction).get();
        if(raw_direction == nullptr) {
            return;
        }
        raw_direction->writer_lock.unlock();
    }

    bool NeoGraphIndex::has_vertex(uint64_t vertex, uint64_t timestamp) const {
        auto direction = gen_tree_direction(vertex);
        if(forest->size() <= direction) {
            return false;
        }
        auto raw_direction = forest->at(gen_tree_direction(vertex)).get();
        if(raw_direction == nullptr) {
            return false;
        }
        return raw_direction->has_vertex(vertex, timestamp);
    }

    bool NeoGraphIndex::has_edge(uint64_t src, uint64_t dest, uint64_t timestamp) const {
        auto direction = gen_tree_direction(src);
        if(forest->size() <= direction) {
            return false;
        }
        auto raw_direction = forest->at(gen_tree_direction(src)).get();
        if(raw_direction == nullptr) {
            return false;
        }
        return raw_direction->has_edge(src, dest, timestamp);
    }

    uint64_t NeoGraphIndex::get_degree(uint64_t src, uint64_t timestamp) const {
        auto direction = gen_tree_direction(src);
        if(forest->size() <= direction) {
            return false;
        }
        auto raw_direction = forest->at(gen_tree_direction(src)).get();
        if(raw_direction == nullptr) {
            return 0;
        }
        return raw_direction->get_degree(src, timestamp);
    }

    RangeElement *NeoGraphIndex::get_neighbor_addr(uint64_t vertex, uint64_t timestamp) const {
        auto direction = gen_tree_direction(vertex);
        if(forest->size() <= direction) {
            return nullptr;
        }
        auto raw_direction = forest->at(gen_tree_direction(vertex)).get();
        if(raw_direction == nullptr) {
            return nullptr;
        }
        return raw_direction->get_neighbor_addr(vertex, timestamp);
    }

    std::pair<uint64_t, uint64_t> NeoGraphIndex::get_filling_info(uint64_t timestamp) const {
        std::pair<uint64_t, uint64_t> res = std::make_pair(0, 0);
//        for(auto & tree: *forest) {
//            auto filling_info = tree.second->get_filling_info(timestamp);
//            res.first += filling_info.first;
//            res.second += filling_info.second;
//        }
        return res;
    }

#if VERTEX_PROPERTY_NUM >= 1
    Property_t NeoGraphIndex::get_vertex_property(uint64_t vertex, uint8_t property_id, uint64_t timestamp) const {
        auto raw_direction = forest->at(gen_tree_direction(vertex)).get();
        if(raw_direction == nullptr) {
            return std::numeric_limits<Property_t>::max();
        }
        return raw_direction->get_vertex_property(vertex, property_id, timestamp);
    }
#endif
#if VERTEX_PROPERTY_NUM > 1
    void NeoGraphIndex::get_vertex_multi_property(uint64_t vertex, std::vector<uint8_t>* property_ids, std::vector<Property_t>& res, uint64_t timestamp) const {
        auto iter = forest->at(gen_tree_direction(vertex)).get();
        if(iter != nullptr) {
            iter->get_vertex_multi_property(vertex, property_ids, res, timestamp);
        } else {
            throw std::invalid_argument("get_vertex_multi_property: vertex does not exist");
        }
    }
#endif
#if EDGE_PROPERTY_NUM >= 1
    Property_t NeoGraphIndex::get_edge_property(uint64_t src, uint64_t dest, uint8_t property_id, uint64_t timestamp) const {
        auto iter = forest->at(gen_tree_direction(src)).get();
        if(iter != nullptr) {
            return iter->get_edge_property(src, dest, property_id, timestamp);
        } else {
            return Property_t();
        }
    }
#endif
#if EDGE_PROPERTY_NUM > 1
    void NeoGraphIndex::get_edge_multi_property(uint64_t src, uint64_t dest, std::vector<uint8_t>* property_ids, std::vector<Property_t>& res, uint64_t timestamp) const {
        auto iter = forest->at(gen_tree_direction(src)).get();
        if(iter != nullptr) {
            iter->get_edge_multi_property(src, dest, property_ids, res, timestamp);
        } else {
            throw std::invalid_argument("get_edge_multi_property: edge does not exist");
        }
    }
#endif

    void NeoGraphIndex::get_vertices(std::vector<uint64_t> &vertices, uint64_t timestamp) const {
//        for(auto & tree: *forest) {
//            for(auto idx = 0; idx < VERTEX_GROUP_SIZE; idx++) {
//                auto tree_version = tree->find_version(timestamp);
//                if(tree_version->vertex_map->at(idx).exist) {
//                    vertices.push_back(tree->vertex_prefix | idx);
//                }
//            }
//        }
        // not implemented
    }

    ///@return false if the vertex does not exist
    bool NeoGraphIndex::get_neighbor(uint64_t src, std::vector<RangeElement> &neighbor, uint64_t timestamp) const {
        auto direction = gen_tree_direction(src);
        if(forest->size() <= direction) {
            return false;
        }
        auto raw_direction = forest->at(direction).get();
        if(raw_direction == nullptr) {
            return false;
        }
        return raw_direction->get_neighbor(src, neighbor, timestamp);
    }


    void NeoGraphIndex::intersect(uint64_t src1, uint64_t src2, std::vector<uint64_t> &result, uint64_t timestamp) const {
        uint64_t tree_dir1 = gen_tree_direction(src1);
        uint64_t tree_dir2 = gen_tree_direction(src2);
        if(tree_dir1 == tree_dir2) {
            auto raw_direction = forest->at(tree_dir1).get();
            if(raw_direction == nullptr) {
                return;
            }
            NeoTree::intersect(raw_direction, src1, raw_direction, src2, result, timestamp);
        } else {
            auto raw_direction1 = forest->at(tree_dir1).get();
            auto raw_direction2 = forest->at(tree_dir2).get();
            if(raw_direction1 == nullptr || raw_direction2 == nullptr) {
                return;
            }
            NeoTree::intersect(raw_direction1, src1, raw_direction2, src2, result, timestamp);
        }
    }

    uint64_t NeoGraphIndex::intersect(uint64_t src1, uint64_t src2, uint64_t timestamp) const {
        uint64_t tree_dir1 = gen_tree_direction(src1);
        uint64_t tree_dir2 = gen_tree_direction(src2);
        uint64_t res = 0;
        if(tree_dir1 == tree_dir2) {
            auto raw_direction = forest->at(tree_dir1).get();
            if(raw_direction == nullptr) {
                return 0;
            }
            res = NeoTree::intersect(raw_direction, src1, raw_direction, src2, timestamp);
        } else {
            auto raw_direction1 = forest->at(tree_dir1).get();
            auto raw_direction2 = forest->at(tree_dir2).get();
            if(raw_direction1 == nullptr || raw_direction2 == nullptr) {
                return 0;
            }
            res = NeoTree::intersect(raw_direction1, src1, raw_direction2, src2, timestamp);
        }
        return res;
    }

    bool NeoGraphIndex::insert_vertex(uint64_t vertex, Property_t* property, WriterTraceBlock* trace_block) {
        auto direction = gen_tree_direction(vertex);
        if(forest->size() <= direction) {
            forest->resize(direction + 1);
        }
        auto raw_direction = forest->at(direction).get();
        if(raw_direction == nullptr) {
            auto new_tree = std::make_unique<NeoTree>(vertex & ~VERTEX_GROUP_MASK);
            new_tree->insert_vertex(vertex, property, trace_block);
            if(forest->size() >= direction) {
                // enlarge forest
                forest->resize(direction + 1);
            }
            (*forest)[direction] = std::move(new_tree);
        } else {
            raw_direction->insert_vertex(vertex, property, trace_block);
        }
        return true;
    }

    bool NeoGraphIndex::insert_vertex_batch(const uint64_t *vertices, Property_t ** properties, uint64_t count, WriterTraceBlock* trace_block) {
        if(count == 0 || vertices == nullptr) {
            throw std::invalid_argument("insert_vertex_batch: vertices is nullptr or count is 0");
        }
        uint64_t st = 0;
        uint64_t ed = 0;
        if(properties != nullptr) {
            while(ed != count) {
                while(ed != count && gen_tree_direction(vertices[ed]) == gen_tree_direction(vertices[st])) {
                    ed++;
                }
                auto direction = gen_tree_direction(vertices[st]);

                if(forest->size() <= direction) {
                    forest->resize(direction + 1);
                }

                auto raw_direction = forest->at(direction).get();
                if(!raw_direction) {
                    auto new_tree = std::make_unique<NeoTree>(vertices[st] & VERTEX_GROUP_MASK);
                    new_tree->insert_vertex_batch(vertices + st, properties + st, ed - st, trace_block);
                    (*forest)[direction] = std::move(new_tree);
                } else {
                    raw_direction->insert_vertex_batch(vertices + st, properties + st, ed - st, trace_block);
                }
                st = ed;
            }
        } else {
            while(ed != count) {
                while(ed != count && gen_tree_direction(vertices[ed]) == gen_tree_direction(vertices[st])) {
                    ed++;
                }
                auto direction = gen_tree_direction(vertices[st]);

                if(forest->size() <= direction) {
                    forest->resize(direction + 1);
                }

                auto raw_direction = forest->at(direction).get();
                if(!raw_direction) {
                    auto new_tree = std::make_unique<NeoTree>(vertices[st] & VERTEX_GROUP_MASK);
                    new_tree->insert_vertex_batch(vertices + st, nullptr, ed - st, trace_block);
                    (*forest)[direction] = std::move(new_tree);
                } else {
                    raw_direction->insert_vertex_batch(vertices + st, nullptr, ed - st, trace_block);
                }
                st = ed;
            }
        }
        return true;
    }

#if VERTEX_PROPERTY_NUM >= 1
    bool NeoGraphIndex::set_vertex_property(uint64_t vertex, uint8_t property_id, Property_t property, uint64_t timestamp) {
        auto iter = forest->find(gen_tree_direction(vertex));
        if(iter != forest->end()) {
            iter->second->set_vertex_property(vertex, property_id, property);
        } else {
            throw std::invalid_argument("insert_edge_batch: tree does not exist");
        }
        return true;
    }

    bool NeoGraphIndex::set_vertex_string_property(uint64_t vertex, uint8_t property_id, std::string&& property, uint64_t timestamp) {
        auto iter = forest->find(gen_tree_direction(vertex));
        if(iter != forest->end()) {
            iter->second->set_vertex_string_property(vertex, property_id, std::move(property));
        } else {
            throw std::invalid_argument("insert_edge_batch: tree does not exist");
        }
        return true;
    }
#endif

#if VERTEX_PROPERTY_NUM > 1
    bool NeoGraphIndex::set_vertex_properties(uint64_t vertex, std::vector<uint8_t>* property_ids, Property_t* properties, uint64_t timestamp) {
        auto iter = forest->find(gen_tree_direction(vertex));
        if(iter != forest->end()) {
            iter->second->set_vertex_properties(vertex, property_ids, properties);
        } else {
            throw std::invalid_argument("insert_edge_batch: tree does not exist");
        }
        return true;
    }
#endif

    bool NeoGraphIndex::insert_edge(uint64_t src, uint64_t dest, Property_t* property, WriterTraceBlock* trace_block) {
        auto raw_direction = forest->at(gen_tree_direction(src)).get();
        if(raw_direction == nullptr) {
            return false;
        }
        raw_direction->insert_edge(src, dest, property, trace_block);
        return true;
    }

#if EDGE_PROPERTY_NUM >= 1
    bool NeoGraphIndex::set_edge_property(uint64_t src, uint64_t dest, uint8_t property_id, Property_t property, uint64_t timestamp) {
//        auto iter = forest->find(gen_tree_direction(src));
//        if(iter != forest->end()) {
//            iter->second->set_edge_property(src, dest, property_id, property);
//        } else {
//            throw std::invalid_argument("remove_edge: tree does not exist");
//        }
//        return true;
    }
#endif

#if EDGE_PROPERTY_NUM > 1
    bool NeoGraphIndex::set_edge_properties(uint64_t src, uint64_t dest, std::vector<uint8_t>* property_ids, Property_t* properties, uint64_t timestamp) {
        auto iter = forest->find(gen_tree_direction(src));
        if(iter != forest->end()) {
            iter->second->set_edge_properties(src, dest, property_ids, properties);
        } else {
            throw std::invalid_argument("remove_edge: tree does not exist");
        }
        return true;
    }
#endif

    bool NeoGraphIndex::remove_vertex(uint64_t vertex, bool is_directed, WriterTraceBlock* trace_block) {
        auto raw_direction = forest->at(gen_tree_direction(vertex)).get();
        if(raw_direction == nullptr) {
            return false;
        }
        return raw_direction->remove_vertex(vertex, is_directed, trace_block);
    }

    bool NeoGraphIndex::remove_edge(uint64_t src, uint64_t dest, WriterTraceBlock* trace_block) {
        auto raw_direction = forest->at(gen_tree_direction(src)).get();
        if(raw_direction == nullptr) {
            return false;
        }
        raw_direction->remove_edge(src, dest, trace_block);
        return true;
    }

    NeoTree* NeoGraphIndex::commit(uint64_t direction, uint64_t timestamp) {
        auto raw_direction = forest->at(direction).get();
        if(raw_direction == nullptr) {
            abort();
        }
        raw_direction->commit_version(timestamp);
        return raw_direction;
    }

    void NeoGraphIndex::gc(uint64_t direction, WriterTraceBlock* trace_block) {
        auto raw_direction = forest->at(direction).get();
        if(raw_direction == nullptr) {
            abort();
        }
        raw_direction->gc(trace_block);
    }

    void NeoGraphIndex::clear() {
        forest->clear();
    }


    bool NeoGraphIndex::insert_edge_batch(const std::pair<RangeElement, RangeElement> *edges, Property_t ** properties, uint64_t count, WriterTraceBlock* trace_block) {
        if(count == 0 || edges == nullptr) {
            std::cerr << "insert_edge_batch: edges is nullptr or count is 0" << std::endl;
            return true;
        }
//        std::cout << "pool with" << BATCH_UPDATE_THREAD_NUM << std::endl;

        uint64_t st = 0;
        uint64_t ed = 0;
        auto tree_direction = gen_tree_direction(edges[st].first);
        std::vector<std::pair<uint64_t, uint64_t>> directions;
        if (properties != nullptr) {

            while (ed != count) {
                while (ed != count && gen_tree_direction(edges[ed].first) == tree_direction) {
                    ed++;
                }
                directions.emplace_back(st, ed - st);
                tree_direction = gen_tree_direction(edges[ed].first);
                st = ed;
            }
            if (st != count) {
                directions.emplace_back(st, ed - st);
            }
            if(directions.size() <= BATCH_UPDATE_THREAD_NUM) {
                std::vector<std::thread> threads;
                for(uint64_t i = 0; i < directions.size(); i++) {
                    auto direction = directions.at(i);
                    threads.emplace_back([edges, properties, direction, this] (size_t thread_id) {
                        insert_edge_batch_single_thread(edges + direction.first, properties + direction.first, direction.second, get_trace_block(thread_id));
                    }, i);
                }
                for(auto &thread: threads) {
                    thread.join();
                }
            } else {
                ThreadPool pool(BATCH_UPDATE_THREAD_NUM);
                std::vector< std::future<bool> > results;
                for(auto &direction: directions){
                    results.emplace_back(
                        pool.enqueue([edges, properties, direction, this] (size_t thread_id) {
                            insert_edge_batch_single_thread(edges + direction.first, properties + direction.first, direction.second, get_trace_block(thread_id));
                            return true;
                        })
                    );
                }
                for(auto && result: results) {
                    result.get();
                }
            }
        }
        else {
            while(ed != count) {
                while (ed != count && gen_tree_direction(edges[ed].first) == tree_direction) {
                    ed++;
                }
                if(ed - st != 0) {
                    this->insert_edge_batch_single_thread(edges + st, nullptr, ed - st, trace_block);
                }
                tree_direction = gen_tree_direction(edges[ed].first);
                st = ed;
            }
            if (st != count) {
                insert_edge_batch_single_thread(edges + st, nullptr, count - st, trace_block);
            }
        }
        return true;
    }

    void NeoGraphIndex::insert_edge_batch_single_thread(const std::pair<RangeElement, RangeElement> *edges, Property_t ** properties, uint64_t count, WriterTraceBlock* trace_block) {
        if(count == 0 || edges == nullptr) {
            throw std::invalid_argument("insert_edge_batch_single_thread: edges is nullptr or count is 0");
        }

        auto raw_direction = forest->at(gen_tree_direction(edges[0].first)).get();
        if(raw_direction == nullptr) {
            return;
        }
//        if(gen_tree_direction(edges[0].first) != gen_tree_direction(948)) {
//            return;
//        }
//        std::cout << gen_tree_direction(edges[0].first) << std::endl;
        raw_direction->insert_edge_batch(edges, properties, count, trace_block);
    }
}
