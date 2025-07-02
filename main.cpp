#include <iostream>
#include <fstream>
#include "readers/reader.hpp"
#include "readers/edgeListReader.hpp"
#include "readers/vertexReader.hpp"
#include "graph/edge.hpp"
#include "graph/edgeStream.hpp"
#include "interfaces/interfaces.hpp"
#include "interfaces/teseo/teseo_interface.hpp"
#include "interfaces/sortledton/sortledton_interface.hpp"
// #include "interfaces/aspen/aspen_interface.hpp"
//#include "interfaces/llama/llama_interface.hpp"
//#include "interfaces/livegraph/livegraph_interface.hpp"
#include "experiments/insertDeletePerformance.hpp"
#include "experiments/analysisPerformance.hpp"
#include "utils/commandLineParser.hpp"
#include "algorithms/BFS.hpp"
#include "algorithms/pageRank.hpp"
#include "algorithms/WCC.hpp"
#include "algorithms/SSSP.hpp"
#include "workloads/scan.hpp"
#include "workloads/batchUpdates.hpp"
#include "workloads/updates.hpp"
#include "workloads/spacialLocality.hpp"
#include <sys/resource.h>
#include <memory>
#include <iostream>
#include <atomic>
#include <string>

using namespace std;

// void handle_sigterm(int signum);


int main() {
    // signal(SIGTERM, handle_sigterm);
    std::cout << 1 << std::endl;
    commandLineParser& parser = commandLineParser::get_instance();
    parser.parse("/lustre/home/acct-sunshixuan/sujixian/LargeGraphBenchmark/config.cfg");

    // for (int i = 1000; i < 100000; i *= 10) {
    //     auto batch_updates = driver::workloads::batchUpdateTest("teseo", parser.get_num_threads(), parser.get_granularity(), i);
    //     batch_updates.exec_all();
    // }

    // auto scanTest = driver::workloads::scanTest(parser.get_graph_name(), parser.get_num_threads(), parser.get_granularity());
    // scanTest.exec_all();
    
    // struct rlimit rl;
    
    // rl.rlim_cur = 20L * 1024 * 1024 * 1024;
    // rl.rlim_max = rl.rlim_cur;
    // if (setrlimit(RLIMIT_AS, &rl) == -1) {
    //     std::cerr << "setrlimit failed" << std::endl;
    //     return 1;
    // }

//{
 //   auto insert_delete_experiment = std::make_unique<driver::microBenchmarks::insertAndDelete>(parser.get_graph_name(), parser.get_num_threads(), parser.get_granularity());
 //   insert_delete_experiment->exec_all();
//}

    // int num_threads[7] = {1, 2, 4, 8, 16, 32, 40};
    // for (auto threads : num_threads)    {
        // auto spacialTest = std::make_unique<driver::workloads::spacialLocalityTest>(parser.get_graph_name(), 1, parser.get_granularity(), parser.get_vertex_file_path(), parser.get_edge_file_path(), true);
        // spacialTest->exec_all();
    // }

    
    // for (auto threads : num_threads)    {
    //     auto spacialTest = std::make_unique<driver::workloads::spacialLocalityTest>(parser.get_graph_name(), threads, parser.get_granularity(), parser.get_vertex_file_path(), parser.get_edge_file_path(), false);
    //     spacialTest->exec_all();
    // }

     //auto analysis_experiment = std::make_unique<driver::microBenchmarks::Analysis>(parser.get_graph_name(), parser.get_num_threads(), parser.get_granularity());
     //analysis_experiment->exec_all();

     try {
         //auto m_graph = driver::interface::create_update_interface(parser.get_graph_name());
         std::shared_ptr<driver::interface::updateInterface> m_graph = std::make_shared<driver::interface::teseo_driver>(false, false);
         auto graph_interface = std::dynamic_pointer_cast<driver::interface::graphalyticsInterface>(m_graph);
      
         auto vertex_reader = driver::reader::Reader::open(parser.get_vertex_file_path(), driver::reader::readerType::vertexList);

         uint64_t vertex_id = 0;
         std::vector<uint64_t> vertex;
         while(vertex_reader->read(vertex_id)) {
             vertex.push_back(vertex_id);
             // m_graph->insert_vertex(vertex_id);
         }
         m_graph->run_batch_vertex_update(vertex, 0, vertex.size());
      
         std::cout << "vertex loaded" << std::endl;

         driver::graph::edgeStream edge_stream;
      
         edge_stream.load_stream(parser.get_edge_file_path());

         std::cout << "edge loaded" << std::endl;

         // std::vector<std::thread> threads_bfs;
      
         // uint64_t size = edge_stream.get_size();
         // int analysis_threads = size / 10000 + 1;
         // m_graph->set_max_threads(analysis_threads);

         // for (uint64_t i = 0; i < size; i += 1000000) {
         //     uint64_t begin = i;
         //     uint64_t end = std::min(size, i + 1000000);
         //     m_graph->run_batch_edge_update(edge_stream, begin, end, driver::interface::updateType::INSERT);
         //     // for (int i = begin; i < end; i++) m_graph->insert_edge(edge_stream[i].source, edge_stream[i].destination);
         //     threads_bfs.emplace_back(std::thread([graph_interface, &parser] (int thread_id) {
         //         graph_interface->init_thread(thread_id);
         //         auto start = std::chrono::high_resolution_clock::now();
         //         auto m_snapshot = graph_interface->get_shared_snapshot();

         //         driver::algorithm::ssspExperiments sssp_experiment(1, parser.get_granularity(), 2.0, graph_interface, m_snapshot);
         //         std::vector<std::pair<uint64_t , double>> result;
         //         sssp_experiment.run_sssp(0, result);
         //         // std::cout << m_snapshot->edge_count() << std::endl;
         //         graph_interface->end_thread(thread_id);
         //         std::cout << m_snapshot->edge_count() << std::endl;
         //         auto end_time = std::chrono::high_resolution_clock::now();
         //         auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start);
         //         // std::cout << "Gapbs SSSP took " << duration.count() << " milliseconds counting in snapshot" << std::endl;
         //     }, i / 1000000));
          
         // }
         // for (auto& th : threads_bfs) {
         //     th.join();
         // }
      
  
         std::vector<std::thread> threads_insert;
         uint64_t num_threads = 40;
         uint64_t chunk_size = (edge_stream.get_size() + num_threads - 1) / num_threads;
      
         auto time_start = std::chrono::high_resolution_clock::now();
         m_graph->set_max_threads(num_threads);
         for (int i = 0; i < num_threads; i++) {
             threads_insert.emplace_back(std::thread([m_graph, &edge_stream, chunk_size] (int thread_id) {
                 m_graph->init_thread(thread_id);
                 uint64_t start = thread_id * chunk_size;
                 uint64_t end = start + chunk_size;
                 uint64_t size = edge_stream.get_size();
                 if (end > size) end = size;
          
                 for(int i = start; i < end; i++) {
                     m_graph->insert_edge(edge_stream[i].source, edge_stream[i].destination);
                  
                 }
                 // m_graph->run_batch_edge_update(edge_stream, start, end, driver::interface::updateType::INSERT);

                 m_graph->end_thread(thread_id);
             }, i));
         }

         for (auto& th : threads_insert) th.join();
         auto time_end = std::chrono::high_resolution_clock::now();
         std::cout << "insertion time: " << std::chrono::duration_cast<std::chrono::milliseconds>(time_end - time_start).count() << std::endl;

         std::cout << "num edges: " << m_graph->edge_count() << std::endl;


         std::cout << "graph loaded" << std::endl;
  
         int threads[7] = {1, 2, 4, 8, 16, 32, 40};
         for (auto num_thread : threads) {
             std::cout << "start running with num threads: " << num_thread << std::endl;
             // auto snapshot = graph_interface->get_shared_snapshot();
             driver::algorithm::ssspExperiments sssp_experiment(num_thread, parser.get_granularity(), 2.0, graph_interface);
             std::vector<std::pair<uint64_t , double>> result;
             sssp_experiment.run_sssp(0, result);

             driver::algorithm::bfsExperiments bfs_experiment(num_thread, parser.get_granularity(), 15.0, 18.0, graph_interface);
             std::vector<std::pair<uint64_t, int64_t>> bfs_result;
             bfs_experiment.run_gapbs_bfs(0, bfs_result);

             driver::algorithm::pageRankExperiments pageRank_experiment(num_thread, parser.get_granularity(), 100, 0.85, graph_interface);
             result.clear();
             pageRank_experiment.run_page_rank(result);

             driver::algorithm::wccExperiments wcc(num_thread, parser.get_granularity(), graph_interface);
             bfs_result.clear();
             wcc.run_wcc(bfs_result);
             std::cout << "end running with num threads: " << num_thread << std::endl;
         }
     } catch (std::exception e) {
         std::cout << e.what();
         std::cout << " error" << std::endl;
     }
    return 0;
}

// void handle_sigterm(int signum);
// {
//     std::cout << "Received SIGTERM, exiting..." << std::endl;
//     // insert_delete_experiment.reset();
//     analysis_experiment.reset();
//     exit(signum);
// }
