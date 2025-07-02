#ifndef COMMAND_LINE_PARSER_HPP
#define COMMAND_LINE_PARSER_HPP

#include <string>
#include <fstream>
#include <iostream>
#include <boost/program_options.hpp>

class Parser {
public:
    static Parser& get_instance();

    void parse(int argc, char* argv[]);

    std::string get_input_file() const;
    std::string get_input_file_static() const;
    std::string get_input_file_dynamic() const;
    std::string get_output_dir() const;
    bool get_is_weighted() const;
    bool get_is_shuffle() const;
    char get_delimiter() const;
    double get_initial_graph_ratio() const;
    double get_vertex_query_ratio() const;
    double get_edge_query_ratio() const;
    double get_high_degree_vertex_ratio() const;
    double get_high_degree_edge_ratio() const;
    double get_low_degree_vertex_ratio() const;
    double get_low_degree_edge_ratio() const;
    unsigned int get_seed() const;
    uint64_t get_insert_num() const;
    uint64_t get_search_num() const;
    uint64_t get_scan_num() const;
    
private:
    Parser() = default;
    Parser(const Parser&) = delete;
    Parser& operator=(const Parser&) = delete;

    std::string input_file;
    std::string input_file_static;
    std::string input_file_dynamic;
    std::string output_dir;
    bool is_weighted;
    char delimiter;
    double initial_graph_ratio;
    double vertex_query_ratio;
    double edge_query_ratio;
    double high_degree_vertex_ratio;
    double high_degree_edge_ratio;
    double low_degree_vertex_ratio;
    double low_degree_edge_ratio;
    uint64_t insert_num;
    uint64_t search_num;
    uint64_t scan_num;
    unsigned int seed;
    bool is_shuffle;
};

#endif // COMMAND_LINE_PARSER_HPP
