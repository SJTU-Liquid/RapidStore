#include "parser.hpp"

namespace po = boost::program_options;

Parser& Parser::get_instance() {
    static Parser instance;
    return instance;
}

void Parser::parse(int argc, char* argv[]) {
    po::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "produce help message")
        ("input_file, i", po::value<std::string>(), "input file")
        ("input_file_static, s", po::value<std::string>(), "input file static")
        ("input_file_dynamic, d", po::value<std::string>(), "input file dynamic")
        ("output_dir, o", po::value<std::string>(), "output directory")
        ("is_weighted, w", po::value<bool>()->default_value(false), "is the graph weighted")
        ("is_shuffle, w", po::value<bool>()->default_value(true), "is shuffle")
        ("delimiter, d", po::value<char>()->default_value(' '), "delimiter character")
        ("initial_graph_ratio", po::value<double>()->default_value(0.8), "initial graph ratio")
        ("vertex_query_ratio", po::value<double>()->default_value(0.2), "vertex query ratio")
        ("edge_query_ratio", po::value<double>()->default_value(0.2), "edge query ratio")
        ("high_degree_vertex_ratio", po::value<double>()->default_value(0.01), "high degree vertex ratio")
        ("high_degree_edge_ratio", po::value<double>()->default_value(0.2), "high degree edge ratio")
        ("low_degree_vertex_ratio", po::value<double>()->default_value(0.2), "low degree vertex ratio")
        ("low_degree_edge_ratio", po::value<double>()->default_value(0.5), "low degree edge ratio")
        ("insert_num", po::value<uint64_t>()->default_value(10000), "low degree edge ratio")
        ("search_num", po::value<uint64_t>()->default_value(10000), "low degree edge ratio")
        ("scan_num", po::value<uint64_t>()->default_value(10000), "low degree edge ratio")
        ("seed, s", po::value<unsigned int>()->default_value(0), "random seed")
    ;

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);

    if (vm.count("help")) {
        std::cout << desc << "\n";
        exit(0);
    }

    if (vm.count("input_file")) {
        input_file = vm["input_file"].as<std::string>();
    } else {
        std::cout << "Input edge file was not set.\n";
    }

    if (vm.count("input_file_static")) {
        input_file_static = vm["input_file_static"].as<std::string>();
    } else {
        input_file_static = "";
    }

    if (vm.count("input_file_dynamic")) {
        input_file_dynamic = vm["input_file_dynamic"].as<std::string>();
    } else {
        // std::cout << "Input edge file was not set.\n";
    }

    if (vm.count("output_dir")) {
        output_dir = vm["output_dir"].as<std::string>();
    } else {
        std::cout << "Output directory was not set.\n";
        exit(1);
    }

    is_weighted = vm["is_weighted"].as<bool>();
    is_shuffle = vm["is_shuffle"].as<bool>();
    delimiter = vm["delimiter"].as<char>();
    initial_graph_ratio = vm["initial_graph_ratio"].as<double>();
    vertex_query_ratio = vm["vertex_query_ratio"].as<double>();
    edge_query_ratio = vm["edge_query_ratio"].as<double>();
    high_degree_vertex_ratio = vm["high_degree_vertex_ratio"].as<double>();
    high_degree_edge_ratio = vm["high_degree_edge_ratio"].as<double>();
    low_degree_vertex_ratio = vm["low_degree_vertex_ratio"].as<double>();
    low_degree_edge_ratio = vm["low_degree_edge_ratio"].as<double>();
    insert_num = vm["insert_num"].as<uint64_t>();
    search_num = vm["search_num"].as<uint64_t>();
    scan_num = vm["scan_num"].as<uint64_t>();
    seed = vm["seed"].as<unsigned int>();
}

std::string Parser::get_input_file() const {
    return input_file;
}


std::string Parser::get_input_file_static() const {
    return input_file_static;
}

std::string Parser::get_input_file_dynamic() const {
    return input_file_dynamic;
}

std::string Parser::get_output_dir() const {
    return output_dir;
}

bool Parser::get_is_weighted() const {
    return is_weighted;
}


bool Parser::get_is_shuffle() const {
    return is_shuffle;
}

char Parser::get_delimiter() const {
    return delimiter;
}

double Parser::get_initial_graph_ratio() const {
    return initial_graph_ratio;
}

double Parser::get_vertex_query_ratio() const {
    return vertex_query_ratio;
}

double Parser::get_edge_query_ratio() const {
    return edge_query_ratio;
}

double Parser::get_high_degree_vertex_ratio() const {
    return high_degree_vertex_ratio;
}

double Parser::get_high_degree_edge_ratio() const {
    return high_degree_edge_ratio;
}

double Parser::get_low_degree_vertex_ratio() const {
    return low_degree_vertex_ratio;
}

double Parser::get_low_degree_edge_ratio() const {
    return low_degree_edge_ratio;
}

uint64_t Parser::get_insert_num() const {
    return insert_num;
}

uint64_t Parser::get_search_num() const {
    return search_num;
}

uint64_t Parser::get_scan_num() const {
    return scan_num;
}

unsigned int Parser::get_seed() const {
    return seed;
}
