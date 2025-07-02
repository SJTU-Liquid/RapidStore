#include "dataset_preprocessor.hpp"
#include "parser.hpp"

int main (int argc, char *argv[]) {
    Parser& parser = Parser::get_instance();
    parser.parse(argc, argv);

    DataPreProcessor processor(parser.get_input_file(), parser.get_is_weighted(), parser.get_delimiter(), parser.get_initial_graph_ratio(), parser.get_vertex_query_ratio(), parser.get_edge_query_ratio(), parser.get_high_degree_vertex_ratio(), parser.get_high_degree_edge_ratio(), parser.get_low_degree_vertex_ratio(), parser.get_low_degree_edge_ratio(), parser.get_insert_num(), parser.get_search_num(), parser.get_scan_num(), parser.get_seed(), parser.get_is_shuffle());
    processor.generateAllWorkloads(parser.get_output_dir());

    return 0;
}
