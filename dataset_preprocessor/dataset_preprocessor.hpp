#pragma once

#include <vector>
#include <string>
#include <fstream>
#include <iostream>
#include <random>
#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <sstream>

#include "types.hpp"

class DataPreProcessor {
private:
    std::vector<weightedEdge> edgeList_;
    std::vector<std::vector<weightedEdge>> graph_;
    vertexID numVertices_;
    std::vector<vertexID> degreeDistribution_;
    std::vector<vertexID> directDegreeDistribution_;
    std::vector<uint64_t> prefixSum_;
    std::unordered_set<vertexID> highDegreeNodes_;
    uint64_t highDegreeEdgeCount_;
    std::unordered_set<vertexID> lowDegreeNodes_;
    uint64_t lowDegreeEdgeCount_;
    unsigned int randomSeed_;

    double initialGraphRatio_;
    double vertexQueryRatio_;
    double edgeQueryRatio_;
    double highVertexRatio_;
    double highEdgeRatio_;
    double lowVertexRatio_;
    double lowEdgeRatio_;
    uint64_t insert_num_;
    uint64_t search_num_;
    uint64_t scan_num_;

    void loadEdges(const std::string & inputFile, bool weighted, char delimiter = ' ');
    void removeDuplicateEdges();
    void randomShuffle();
    void computeDegreeDistribution();
    void saveStream(const std::string & streamPath, std::vector<operation> & stream);
    void selectNodesByDegree();
    vertexID selectNodeByDegree();
    void selectRandomNodes(uint64_t targetSize, std::vector<vertexID> & chosenNodes, bool uniform = false);

    void insertInitialVertices(const std::string & initialStreamPath);
    void processWorkload(const std::string & initialStreamPath, const std::string & targetStreamPath, bool isInsert, targetStreamType streamType);
    void updateWorkload(const std::string & initialStreamPath, const std::string & targetStreamPath);
    void benchmarkQueries(const std::string & targetStreamPath, targetStreamType streamType, operationType opType);
    void initialAnalyticQueries(const std::string & initialStreamPath);
    void targetAnalyticQueries(const std::string & targetStreamPath);
    void initialMixedQueries(const std::string & initialStreamPath);
    void targetMixedQueries(const std::string & targetStream, operationType opType);
    
public:
    DataPreProcessor(std::string inputFile, bool weighted, char delimiter = ' ', double initGraphRatio = 0.8, double vertexQueryRatio = 0.2, double edgeQueryRatio = 0.2, double highDegreeVertexRatio = 0.01, double highDegreeEdgeRatio = 0.2, double lowDegreeVertexRatio = 0.2, double lowDegreeEdgeRatio = 0.5, uint64_t insert_num = 10000, uint64_t search_num = 10000, uint64_t scan_num = 10000, unsigned int seed = 0,  bool shuffle = true);

    void generateAllWorkloads(const std::string & dirPath);
};