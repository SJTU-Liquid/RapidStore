#include "neo_wrapper.h"
#include <queue>
#include <utility>
#include <thread>
#include <random>
thread_local WriterTraceBlock* tracer = nullptr;

namespace wrapper {
    void execute(const DriverConfig & config) {
        auto mem1 = getValue();
        auto wrapper = Neo_Graph_Wrapper(false, true);
        std::cout << getValue() - mem1 << std::endl;
        Driver<Neo_Graph_Wrapper, std::shared_ptr<Neo_Graph_Wrapper::Snapshot>> d(wrapper, config);
        d.execute(config.workload_type, config.target_stream_type);
    }
}

#include "driver_main.h"
// Function Implementations
// Init
void Neo_Graph_Wrapper::load(const std::string &path, driver::reader::readerType type) {
    auto reader = driver::reader::Reader::open(path, type);
    switch (type) {
        case driver::reader::readerType::edgeList: {
            driver::graph::weightedEdge edge;
            while (reader->read(edge)) {
                try {
                    if (!has_vertex(edge.source)) {
                        insert_vertex(edge.source);
                    }
                    if (!has_vertex(edge.destination)) {
                        insert_vertex(edge.destination);
                    }
                    if (!has_edge(edge.source, edge.destination)) {
                        insert_edge(edge.source, edge.destination);
                    }
                } catch (const std::exception & e) {
                    std::cerr << e.what() << std::endl;
                }
            }
            break;
        }
        case driver::reader::readerType::vertexList: {
            uint64_t vertex;
            while (reader->read(vertex)) {
                try {
                    if (!has_vertex(vertex)) {
                        insert_vertex(vertex);
                    }
                } catch (const std::exception & e) {
                    std::cerr << e.what() << std::endl;
                }
            }
            break;
        }
    }
}

std::shared_ptr<Neo_Graph_Wrapper> Neo_Graph_Wrapper::create_update_interface(const std::string& graph_type) {
    if (graph_type == "vec2vec") {
        return std::make_shared<Neo_Graph_Wrapper>();
    }
    else {
        throw std::runtime_error("Graph name not recognized, interfaces::create_update_interface");
    }
}

// Graph Operations
bool Neo_Graph_Wrapper::is_directed() const {
    return m_is_directed;
}

bool Neo_Graph_Wrapper::is_weighted() const {
    return m_is_weighted;
}

bool Neo_Graph_Wrapper::is_empty() const {
    return (this->vertex_count() == 0);
}

void Neo_Graph_Wrapper::set_max_threads(int max_threads) {
    if(max_threads < 0) { // NOTE: for batch: special negative value because of the incompatibility of test framework
        std::cout << "Neo_Graph_Wrapper::set_max_threads(): global writer:" << -max_threads << std::endl;
        for(int i = 0; i < -max_threads; i++) {
            writer_register();
        }
    }
}

void Neo_Graph_Wrapper::init_thread(int thread_id) {
    tracer = writer_register();
}

void Neo_Graph_Wrapper::end_thread(int thread_id) {
    writer_unregister(tracer);
}

bool Neo_Graph_Wrapper::has_vertex(uint64_t vertex) const {
    auto tx = tm.get_read_transaction();
    auto has_vertex = tx->has_vertex(vertex);
    tx->commit();
    delete tx;
    return has_vertex;
}

//bool Neo_Graph_Wrapper::has_vertex(uint64_t vertex) const {
//    auto tx = tm.get_read_transaction();
//    auto has_vertex = tx.has_vertex(vertex);
//    tx.commit();
//    return has_vertex;
//}

bool Neo_Graph_Wrapper::has_edge(driver::graph::weightedEdge edge) const {
    return has_edge(edge.source, edge.destination, edge.weight);
}

bool Neo_Graph_Wrapper::has_edge(uint64_t source, uint64_t destination) const {
    auto tx = tm.get_read_transaction();
    auto has_edge = tx->has_edge(source, destination);
    tx->commit();
    delete tx;
    return has_edge;
}

bool Neo_Graph_Wrapper::has_edge(uint64_t source, uint64_t destination, double weight) const {
    throw driver::error::FunctionNotImplementedError("has_edge::weighted");
}

uint64_t Neo_Graph_Wrapper::degree(uint64_t vertex) const {
    auto tx = tm.get_read_transaction();
    if (!tx->has_vertex(vertex)) {
        throw driver::error::GraphLogicalError("Vertex does not exist : Neo_Graph_Wrapper::degree");
    }
    auto degree = tx->get_degree(vertex);
    tx->commit();
    delete tx;
    return degree;
}

//uint64_t Neo_Graph_Wrapper::degree(uint64_t vertex) const {
//    auto tx = tm.get_read_transaction();
//    if (!tx.has_vertex(vertex)) {
//        throw driver::error::GraphLogicalError("Vertex does not exist : Neo_Graph_Wrapper::degree");
//    }
//    auto degree = tx.get_degree(vertex);
//    tx.commit();
//    return degree;
//}

double Neo_Graph_Wrapper::get_weight(uint64_t source, uint64_t destination) const {
    throw driver::error::FunctionNotImplementedError("get_weight");
}

#if VERTEX_PROPERTY_NUM >= 1
Property_t Neo_Graph_Wrapper::get_vertex_property(uint64_t vertex, uint8_t property_id) const {
    auto tx = tm.get_read_transaction();
    if (!tx->has_vertex(vertex)) {
        throw driver::error::GraphLogicalError("Vertex does not exist : Neo_Graph_Wrapper::get_vertex_property");
    }
    auto property = tx->get_vertex_property(vertex, property_id);
    tx->commit();
    delete tx;
    return property;
}
#endif
#if VERTEX_PROPERTY_NUM > 1
void Neo_Graph_Wrapper::get_vertex_multi_property(uint64_t vertex, std::vector<uint8_t>* property_ids, std::vector<Property_t>& res) const {
    auto tx = tm.get_read_transaction();
    if (!tx->has_vertex(vertex)) {
        throw driver::error::GraphLogicalError("Vertex does not exist : Neo_Graph_Wrapper::get_vertex_multi_property");
    }
    tx->get_vertex_multi_property(vertex, property_ids, res);
    delete tx;
    tx->commit();
}
#endif
#if EDGE_PROPERTY_NUM >= 1
Property_t Neo_Graph_Wrapper::get_edge_property(uint64_t src, uint64_t dest, uint8_t property_id) const {
    auto tx = tm.get_read_transaction();
    if (!tx->has_edge(src, dest)) {
//        throw driver::error::GraphLogicalError("Edge does not exist : Neo_Graph_Wrapper::get_edge_property");
        return Property_t();
    }
    auto property = tx->get_edge_property(src, dest, property_id);
    tx->commit();
    delete tx;
    return property;
}
#endif
#if EDGE_PROPERTY_NUM > 1
void Neo_Graph_Wrapper::get_edge_multi_property(uint64_t src, uint64_t dest, std::vector<uint8_t>* property_ids, std::vector<Property_t>& res) const {
    auto tx = tm.get_read_transaction();
    if (!tx->has_edge(src, dest)) {
        throw driver::error::GraphLogicalError("Edge does not exist : Neo_Graph_Wrapper::get_edge_multi_property");
    }
    tx->get_edge_multi_property(src, dest, property_ids, res);
    delete tx;
    tx->commit();
}
#endif

uint64_t Neo_Graph_Wrapper::logical2physical(uint64_t vertex) const {
    return vertex;
}

uint64_t Neo_Graph_Wrapper::physical2logical(uint64_t physical) const {
    return physical;
}

uint64_t Neo_Graph_Wrapper::vertex_count() const {
    auto tx = tm.get_read_transaction();
    auto num_vertices = tx->vertex_count();
    tx->commit();
    delete tx;
    return num_vertices;
}

uint64_t Neo_Graph_Wrapper::edge_count() const {
    auto tx = tm.get_read_transaction();
    auto num_edges = tx->edge_count();
    tx->commit();
    delete tx;
    return num_edges;
}

//uint64_t Neo_Graph_Wrapper::vertex_count() const {
//    auto tx = tm.get_read_transaction();
//    auto num_vertices = tx.vertex_count();
//    tx.commit();
//    return num_vertices;
//}
//
//uint64_t Neo_Graph_Wrapper::edge_count() const {
//    auto tx = tm.get_read_transaction();
//    auto num_edges = tx.edge_count();
//    tx.commit();
//    return num_edges;
//}

void Neo_Graph_Wrapper::get_neighbors(uint64_t vertex, std::vector<uint64_t> &) const {
    throw driver::error::FunctionNotImplementedError("get_neighbors");
}

void Neo_Graph_Wrapper::get_neighbors(uint64_t vertex, std::vector<std::pair<uint64_t, double>> &) const {
    throw driver::error::FunctionNotImplementedError("get_neighbors");
}

bool Neo_Graph_Wrapper::insert_vertex(uint64_t vertex, Property_t* property) {
    auto tx = tm.get_write_transaction();
    bool inserted = true;
    try {
        tx->insert_vertex(vertex, property);
        tx->commit();
    } catch (std::exception &e) {
        // print error message
        std::cerr << e.what() << std::endl;
        tx->abort();
        inserted = false;
    }
    delete tx;
    return inserted;
}

//bool Neo_Graph_Wrapper::insert_vertex(uint64_t vertex, Property_t* property) {
//    auto tx = tm.get_write_transaction();
//    bool inserted = true;
//    try {
//        tx->insert_vertex(vertex, property);
//        tx->commit();
//    } catch (std::exception &e) {
//        // print error message
//        std::cerr << e.what() << std::endl;
//        tx->abort();
//        inserted = false;
//    }
//    return inserted;
//}

#if VERTEX_PROPERTY_NUM >= 1
bool Neo_Graph_Wrapper::set_vertex_property(uint64_t vertex, uint8_t property_id, Property_t property) {
    auto tx = tm.get_write_property_transaction();
    bool has_set = true;
    try {
        tx->vertex_property_set(vertex, property_id, property);
        has_set = tx->commit();
    } catch (std::exception &e) {
        // print error message
        std::cerr << e.what() << std::endl;
        tx->abort();
        has_set = false;
    }
    delete tx;
    return has_set;
}

bool Neo_Graph_Wrapper::set_vertex_string_property(uint64_t vertex, uint8_t property_id, std::string&& property) {
    auto tx = tm.get_write_property_transaction();
    bool has_set = true;
    try {
        tx->vertex_string_property_set(vertex, property_id, std::move(property));
        has_set = tx->commit();
    } catch (std::exception &e) {
        // print error message
        std::cerr << e.what() << std::endl;
        tx->abort();
        has_set = false;
    }
    delete tx;
    return has_set;
}
#endif

bool Neo_Graph_Wrapper::insert_edge(uint64_t source, uint64_t destination, Property_t* property) {
    LightWriteTransaction::insert_edge(source, destination, property, m_is_directed, &tm, tracer);
    return true;
}

bool Neo_Graph_Wrapper::insert_edge(uint64_t source, uint64_t destination) {
    return insert_edge(source, destination, nullptr);
}

bool Neo_Graph_Wrapper::insert_edge(uint64_t source, uint64_t destination, double weight) {
    return insert_edge(source, destination, ((Property_t*) ((uint64_t)weight)));
}

#if EDGE_PROPERTY_NUM >= 1
bool Neo_Graph_Wrapper::set_edge_property(uint64_t src, uint64_t dest, uint8_t property_id, Property_t property) {
    auto tx = tm.get_light_write_transaction();
    bool has_set = true;
    try {
        tx->update_edge(src, dest, property);
        if (!m_is_directed) {
            tx->update_edge(dest, src, property);
        }
        has_set = tx->commit();
    } catch (std::exception &e) {
        // print error message
        std::cerr << e.what() << std::endl;
        tx->abort();
        has_set = false;
    }
    delete tx;
    return has_set;
}
#endif

bool Neo_Graph_Wrapper::remove_vertex(uint64_t vertex) {
    auto tx = tm.get_write_transaction();
    bool removed = true;
    try {
        tx->remove_vertex(vertex);
        tx->commit();
    } catch (std::exception &e) {
        // print error message
        std::cerr << e.what() << std::endl;
        tx->abort();
        removed = false;
    }
    delete tx;
    return removed;
}

bool Neo_Graph_Wrapper::remove_edge(uint64_t source, uint64_t destination) {
    LightWriteTransaction::remove_edge(source, destination, m_is_directed, &tm, tracer);
    return true;
}

bool Neo_Graph_Wrapper::run_batch_vertex_update(std::vector<uint64_t> &vertices, int start, int end) {
    auto tx = tm.get_write_transaction();
    bool inserted = true;
    try {
        for(int i = start; i < end; i++) {
            tx->insert_vertex(vertices[i], nullptr);
        }
        tx->commit(true, false);
    } catch (std::exception &e) {
        // print error message
        std::cerr << e.what() << std::endl;
        tx->abort();
        inserted = false;
    }
    delete tx;
    return inserted;
}

//bool Neo_Graph_Wrapper::run_batch_vertex_update(std::vector<uint64_t> &vertices, int start, int end) {
//    auto tx = tm.get_write_transaction();
//    bool inserted = true;
//    try {
//        for(int i = start; i < end; i++) {
//            tx.insert_vertex(vertices[i], nullptr);
//        }
//        tx.commit(true, false);
//    } catch (std::exception &e) {
//        // print error message
//        std::cerr << e.what() << std::endl;
//        tx.abort();
//        inserted = false;
//    }
//    return inserted;
//}

//bool Neo_Graph_Wrapper::run_batch_vertex_update(std::vector<uint64_t> &vertices, std::vector<Property_t*>* properties, int start, int end) {
//    auto tx = tm.get_write_transaction();
//    bool inserted = true;
//    try {
//        if(properties) {
//            for(int i = start; i < end; i++) {
//                tx->insert_vertex(vertices[i], (*properties)[i]);
//            }
//        } else {
//            for(int i = start; i < end; i++) {
//                tx->insert_vertex(vertices[i], nullptr);
//            }
//        }
//        tx->commit(true, false);
//    } catch (std::exception &e) {
//        // print error message
//        std::cerr << e.what() << std::endl;
//        tx->abort();
//        inserted = false;
//    }
//    delete tx;
//    return inserted;
//}

//bool Neo_Graph_Wrapper::run_batch_vertex_update(std::vector<uint64_t> &vertices, std::vector<Property_t*>* properties, int start, int end) {
//    auto tx = tm.get_write_transaction();
//    bool inserted = true;
//    try {
//        if(properties) {
//            for(int i = start; i < end; i++) {
//                tx.insert_vertex(vertices[i], (*properties)[i]);
//            }
//        } else {
//            for(int i = start; i < end; i++) {
//                tx.insert_vertex(vertices[i], nullptr);
//            }
//        }
//        tx.commit(true, false);
//    } catch (std::exception &e) {
//        // print error message
//        std::cerr << e.what() << std::endl;
//        tx.abort();
//        inserted = false;
//    }
//    return inserted;
//}

// NOTE only batch insert is support
bool Neo_Graph_Wrapper::run_batch_edge_update(std::vector<std::pair<uint64_t, uint64_t>> &edges, int start, int end, operationType type) {
    bool inserted = true;
    auto tx = tm.get_write_transaction();
    if(type == operationType::INSERT) {
        for (int i = start; i < end; i++) {
            tx->insert_edge(edges[i].first, edges[i].second, nullptr);
        }
        if (!m_is_directed) {
            for (int i = start; i < end; i++) {
                tx->insert_edge(edges[i].second, edges[i].first, nullptr);
            }
        }
        tx->commit(false, true);
        delete tx;
        return inserted;
    } else {
        for (int i = start; i < end; i++) {
            tx->remove_edge(edges[i].first, edges[i].second);
        }
        if (!m_is_directed) {
            for (int i = start; i < end; i++) {
                tx->remove_edge(edges[i].second, edges[i].first);
            }
        }
        tx->commit(false, false);
        delete tx;
        return inserted;
    }
}


bool Neo_Graph_Wrapper::run_batch_edge_update(std::vector<operation> & edges, int start, int end, operationType type) {
    bool inserted = true;
    auto tx = tm.get_write_transaction();
    if(type == operationType::INSERT) {
        for (int i = start; i < end; i++) {
            tx->insert_edge(edges[i].e.source, edges[i].e.destination, (Property_t *) ((uint64_t) edges[i].e.weight));
        }
        if (!m_is_directed) {
            for (int i = start; i < end; i++) {
                tx->insert_edge(edges[i].e.destination, edges[i].e.source,
                                (Property_t *) ((uint64_t) edges[i].e.weight));
            }
        }
        tx->commit(false, true);
        delete tx;
        return inserted;
    } else {
        for (int i = start; i < end; i++) {
            tx->remove_edge(edges[i].e.destination, edges[i].e.source);
        }
        if (!m_is_directed) {
            for (int i = start; i < end; i++) {
                tx->remove_edge(edges[i].e.destination, edges[i].e.source);
            }
        }
        tx->commit(false, false);
        delete tx;
        return inserted;
    }
}


//bool Neo_Graph_Wrapper::run_batch_edge_update(std::vector<std::pair<uint64_t, uint64_t>> &edges, int start, int end, operationType type) {
//    bool inserted = true;
//    auto tx = tm.get_light_write_transaction(tracer);
//    switch (type) {
//        case operationType::INSERT: {
//            tx->insert_edge(&edges, start, end);
//            break;
//        }
//        case operationType::DELETE: {
//            for (int i = start; i < end; i++) {
//                tx->remove_edge(edges[i].first, edges[i].second);
//            }
//            if (!m_is_directed) {
//                for (int i = start; i < end; i++) {
//                    tx->remove_edge(edges[i].second, edges[i].first);
//                }
//            }
//            break;
//        }
//        case operationType::UPDATE: {   // For experiment purpose
//            for (int i = start; i < end; i++) {
//                tx->update_edge(edges[i].first, edges[i].second, 1.0);
//            }
//            if (!m_is_directed) {
//                for (int i = start; i < end; i++) {
//                    tx->update_edge(edges[i].second, edges[i].first, 1.0);
//                }
//            }
//            break;
//        }
//        default: {
//            break;
//        }
//    }
//    tx->commit(false, true);
//    delete tx;
//    return inserted;
//}
//
//
//bool Neo_Graph_Wrapper::run_batch_edge_update(std::vector<operation> & edges, int start, int end, operationType type) {
//    bool inserted = true;
//    auto tx = tm.get_light_write_transaction(tracer);
//    switch (type) {
//        case operationType::INSERT: {
//            for (int i = start; i < end; i++) {
//                tx->insert_edge(edges[i].e.source, edges[i].e.destination, nullptr);
//            }
//            if (!m_is_directed) {
//                for (int i = start; i < end; i++) {
//                    tx->insert_edge(edges[i].e.destination, edges[i].e.source, nullptr);
//                }
//            }
////            tx->insert_edge(&edges, start, end);
//            break;
//        }
//        case operationType::DELETE: {
//            for (int i = start; i < end; i++) {
//                tx->remove_edge(edges[i].e.source, edges[i].e.destination);
//            }
//            if (!m_is_directed) {
//                for (int i = start; i < end; i++) {
//                    tx->remove_edge(edges[i].e.destination, edges[i].e.source);
//                }
//            }
//            break;
//        }
//        case operationType::UPDATE: {   // For experiment purpose
//            for (int i = start; i < end; i++) {
//                tx->update_edge(edges[i].e.source, edges[i].e.destination, 1.0);
//            }
//            if (!m_is_directed) {
//                for (int i = start; i < end; i++) {
//                    tx->update_edge(edges[i].e.destination, edges[i].e.source, 1.0);
//                }
//            }
//            break;
//        }
//        default: {
//            break;
//        }
//    }
//    tx->commit(false, true);
//    delete tx;
//    return inserted;
//}

void Neo_Graph_Wrapper::clear() {

}

// Snapshot Related Function Implementations
std::unique_ptr<Neo_Graph_Wrapper::Snapshot> Neo_Graph_Wrapper::get_unique_snapshot() const {
    return std::make_unique<Snapshot>(tm);
}

std::shared_ptr<Neo_Graph_Wrapper::Snapshot> Neo_Graph_Wrapper::get_shared_snapshot() const {
    return std::make_shared<Snapshot>(tm);
}


uint64_t Neo_Graph_Wrapper::Snapshot::size() const {
    return this->m_num_edges;
}

uint64_t Neo_Graph_Wrapper::Snapshot::degree(uint64_t vertex, bool logical) const {
    if (!has_vertex(vertex)) {
        throw driver::error::GraphLogicalError("Vertex does not exist : Neo_Graph_Wrapper::snapshot::degree");
    }
    return snapshot.get_degree(vertex);
}

bool Neo_Graph_Wrapper::Snapshot::has_vertex(uint64_t vertex) const {
    auto non_const_this = const_cast<Snapshot*>(this);
    return non_const_this->snapshot.has_vertex(vertex);
}

bool Neo_Graph_Wrapper::Snapshot::has_edge(driver::graph::weightedEdge edge) const {
    return has_edge(edge.source, edge.destination);
}

bool Neo_Graph_Wrapper::Snapshot::has_edge(uint64_t source, uint64_t destination) const {
    return snapshot.has_edge(source, destination);
}

bool Neo_Graph_Wrapper::Snapshot::has_edge(uint64_t source, uint64_t destination, double weight) const {
    throw driver::error::FunctionNotImplementedError("snapshot::has_edge::weighted");
}

double Neo_Graph_Wrapper::Snapshot::get_weight(uint64_t source, uint64_t destination) const {
    throw driver::error::FunctionNotImplementedError("snapshot::get_weight");
}

#if VERTEX_PROPERTY_NUM >= 1
Property_t Neo_Graph_Wrapper::Snapshot::get_vertex_property(uint64_t vertex, uint8_t property_id) const {
    return snapshot.get_vertex_property(vertex, property_id);
}
#endif
#if VERTEX_PROPERTY_NUM > 1
void Neo_Graph_Wrapper::Snapshot::get_multi_vertex_property(uint64_t vertex, std::vector<uint8_t>* property_ids, std::vector<Property_t>& res) const {
    snapshot.get_vertex_multi_property(vertex, property_ids, res);
}
#endif
#if EDGE_PROPERTY_NUM >= 1
Property_t Neo_Graph_Wrapper::Snapshot::get_edge_property(uint64_t src, uint64_t dest, uint8_t property_id) const {
    return snapshot.get_edge_property(src, dest, property_id);
}
#endif
#if EDGE_PROPERTY_NUM > 1
void Neo_Graph_Wrapper::Snapshot::get_multi_edge_property(uint64_t src, uint64_t dest, std::vector<uint8_t>* property_ids, std::vector<Property_t>& res) const {
    snapshot.get_edge_multi_property(src, dest, property_ids, res);
}
#endif
uint64_t Neo_Graph_Wrapper::Snapshot::vertex_count() const {
    return this->m_num_vertices;
}

uint64_t Neo_Graph_Wrapper::Snapshot::edge_count() const {
    return this->m_num_edges;
}

void* Neo_Graph_Wrapper::Snapshot::get_neighbor_addr(uint64_t index) const {
    return snapshot.get_neighbor_addr(index);
}

uint64_t Neo_Graph_Wrapper::Snapshot::intersect(uint64_t src1, uint64_t src2) const {
    return snapshot.intersect(src1, src2);
}

void Neo_Graph_Wrapper::Snapshot::intersect(uint64_t src1, uint64_t src2, std::vector<uint64_t> &result) const {
    snapshot.intersect(src1, src2, result);
}

void Neo_Graph_Wrapper::Snapshot::edges(uint64_t index, std::vector<uint64_t> &neighbors) const {
    snapshot.get_neighbor(index, neighbors);
//    std::sort(neighbors.begin(), neighbors.end());
}

template<typename F>
void Neo_Graph_Wrapper::Snapshot::edges(uint64_t index, F&& callback) const {
    snapshot.edges(index, std::forward<F>(callback));
}

void Neo_Graph_Wrapper::Snapshot::edges(uint64_t index, std::vector<uint64_t> &neighbors, bool logical) const {
    snapshot.get_neighbor(index, neighbors);
}

template<typename F>
void Neo_Graph_Wrapper::Snapshot::edges(uint64_t index, F&& callback, bool logical) const {
    snapshot.edges(index, std::forward<F>(callback));
}