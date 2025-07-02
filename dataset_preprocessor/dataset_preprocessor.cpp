#include "dataset_preprocessor.hpp"

DataPreProcessor::DataPreProcessor(std::string inputFile, bool weighted, char delimiter, double initGraphRatio, double vertexQueryRatio, double edgeQueryRatio, double highDegreeVertexRatio, double highDegreeEdgeRatio, double lowDegreeVertexRatio, double lowDegreeEdgeRatio, uint64_t insert_num, uint64_t search_num, uint64_t scan_num, unsigned int seed, bool shuffle)
    : initialGraphRatio_(initGraphRatio), vertexQueryRatio_(vertexQueryRatio), edgeQueryRatio_(edgeQueryRatio), highVertexRatio_(highDegreeVertexRatio), highEdgeRatio_(highDegreeEdgeRatio), lowVertexRatio_(lowDegreeVertexRatio), lowEdgeRatio_(lowDegreeEdgeRatio), randomSeed_(seed), insert_num_(insert_num), search_num_(search_num), scan_num_(scan_num)
{
    loadEdges(inputFile, weighted, delimiter);
    removeDuplicateEdges();
    if (shuffle) randomShuffle();
    computeDegreeDistribution();
    selectNodesByDegree();
}

std::string removeCharacter(const std::string &str, char charToRemove) {
    std::string result;
    for (char c : str) {
        if (c != charToRemove) {
            result += c;
        }
    }
    return result;
}

void splitString(const std::string & str, char delim, std::vector<std::string> & tokens) {
    std::stringstream ss(removeCharacter(str, '\r')); 
    std::string token;
    while (std::getline(ss, token, delim)) {
        if (!token.empty()) {
            tokens.push_back(token);
        }
    }
}

void DataPreProcessor::loadEdges(const std::string & inputFile, bool weighted, char delimiter) {
    std::ifstream handle(inputFile);
    if (!handle.is_open()) {
        std::cerr << "Error: cannot open input file" << std::endl;
        exit(1);
    }

    std::string line;
    int lineNumber = 1;
    std::unordered_map<vertexID, uint64_t> vertexMap;
    vertexID nextVertexIndex = 0;

    while (std::getline(handle, line)) {
        if (line.empty() || line[0] == '#') {
            continue;
        }

        std::vector<std::string> tokens;
        splitString(line, delimiter, tokens);

        vertexID source, destination;
        double weight = random() / static_cast<double>(RAND_MAX);

        try {
            source = std::stoll(tokens.at(0));
            destination = std::stoll(tokens.at(1));
            if (weighted && tokens.size() > 2) {
                weight = std::stod(tokens.at(2));
            }
            if (source == destination) {
                std::cerr << "Invalid line: " << line << std::endl;
                continue;
            }

            if (vertexMap.find(source) == vertexMap.end()) {
                vertexMap[source] = nextVertexIndex++;
                source = nextVertexIndex - 1;
            } else {
                source = vertexMap[source];
            }

            if (vertexMap.find(destination) == vertexMap.end()) {
                vertexMap[destination] = nextVertexIndex++;
                destination = nextVertexIndex - 1;
            } else {
                destination = vertexMap[destination];
            }

            if (source > destination) {
                std::swap(source, destination);
            }

        } catch (const std::exception& e) {
            std::cerr << "Invalid line: " << line << " at line " << lineNumber << std::endl;
            continue;
        }

        edgeList_.push_back({source, destination, weight});
        lineNumber++;
    }

    numVertices_ = nextVertexIndex;
    handle.close();

    graph_.resize(numVertices_);
    for (auto edge : edgeList_) {
        graph_[edge.source].push_back(edge);
    }
}

void DataPreProcessor::removeDuplicateEdges() {
    std::sort(edgeList_.begin(), edgeList_.end(), [](const weightedEdge& a, const weightedEdge& b) {
        if (a.source == b.source) {
            return a.destination < b.destination;
        } else {
            return a.source < b.source;
        }
    });
    
    uint64_t current = 0, ahead = 0;
    for (; ahead < edgeList_.size(); ahead++, current++) {
        while (ahead + 1 < edgeList_.size() && edgeList_[ahead].source == edgeList_[ahead + 1].source && edgeList_[ahead].destination == edgeList_[ahead + 1].destination) {
            ahead++;
        }
        if (ahead > current) edgeList_[current] = edgeList_[ahead];
    }
    edgeList_.resize(current);
}

void DataPreProcessor::randomShuffle() {
    std::default_random_engine engine(randomSeed_);
    std::shuffle(edgeList_.begin(), edgeList_.end(), engine);
}

void DataPreProcessor::computeDegreeDistribution() {
    degreeDistribution_.resize(numVertices_, 0);
    directDegreeDistribution_.resize(numVertices_, 0);
    for (auto & e : edgeList_) {
        degreeDistribution_[e.source]++;
        degreeDistribution_[e.destination]++;
        directDegreeDistribution_[e.source]++;
    }
    uint64_t totalDegree = std::accumulate(degreeDistribution_.begin(), degreeDistribution_.end(), 0);

    prefixSum_.resize(degreeDistribution_.size());
    uint64_t sum = 0;
    for (uint64_t i = 0; i < prefixSum_.size(); i++) {
        prefixSum_[i] = degreeDistribution_[i] + sum;
        sum = prefixSum_[i];
    }
}

void DataPreProcessor::selectNodesByDegree() {
    std::vector<std::pair<uint64_t, vertexID>> nodeDegrees;
    for (uint64_t i = 0; i < numVertices_; i++) {
        nodeDegrees.push_back({degreeDistribution_[i], i});
    }
    std::sort(nodeDegrees.begin(), nodeDegrees.end(), [](const std::pair<uint64_t, vertexID> & a, const std::pair<uint64_t, vertexID> & b) {
        return a.first > b.first;
    });

    uint64_t selectedHighDegreeSize = numVertices_ * highVertexRatio_;
    std::pair<uint64_t, vertexID> node;

    highDegreeEdgeCount_ = 0;
    for (uint64_t i = 0; i < selectedHighDegreeSize; i++) {
        node = nodeDegrees[i];
        highDegreeNodes_.insert(node.second);
        highDegreeEdgeCount_ += node.first;
    } 

    uint64_t selectedLowDegreeSize = numVertices_ * lowEdgeRatio_;
    lowDegreeEdgeCount_ = 0;
    for (uint64_t i = 0; i < selectedLowDegreeSize; i++) {
        node = nodeDegrees[numVertices_ - 1 - i];
        lowDegreeNodes_.insert(node.second);
        lowDegreeEdgeCount_ += node.first;
    }
}

vertexID DataPreProcessor::selectNodeByDegree() {
    uint64_t maxValue = *(prefixSum_.end() - 1); 
    uint64_t randValue = random() % maxValue;
    auto it = std::lower_bound(prefixSum_.begin(), prefixSum_.end(), randValue);
    
    return it - prefixSum_.begin();
}

void DataPreProcessor::selectRandomNodes(uint64_t targetSize, std::vector<vertexID> & chosenNodes, bool uniform) {
    uint64_t j = 0;
    for (uint64_t i = 0; i < targetSize; i++) {
        bool chosen = false;
        uint64_t selectedNode = 0;
        std::mt19937 gen(randomSeed_);
        std::uniform_int_distribution<> dist(0, numVertices_);
        
        while(!chosen) {
            if (uniform) selectedNode = (j++) % numVertices_;
            else selectedNode = selectNodeByDegree();
            
            if (chosenNodes[selectedNode] < directDegreeDistribution_[selectedNode]) {
                chosenNodes[selectedNode]++;
                chosen = true;
            }
        }
    }
}

void DataPreProcessor::saveStream(const std::string & streamPath, std::vector<operation> & stream) {
    std::ofstream file(streamPath, std::ios::binary);
    if (file.is_open()) {
        file.write(reinterpret_cast<const char*>(stream.data()), stream.size() * sizeof(operation));
    }
    file.close();
}

void DataPreProcessor::insertInitialVertices(const std::string & initialStreamPath) {
    std::vector<operation> initialStream;

    for (uint64_t i = 0; i < numVertices_; i++) {
        initialStream.push_back({operationType::INSERT_VERTEX, {i, 0, 0}});
    }

    saveStream(initialStreamPath, initialStream);
    std::cout << "Number of vertices: " << initialStream.size() << std::endl;
    std::cout << "Number of edges: " << edgeList_.size() << std::endl;
}

void DataPreProcessor::processWorkload(const std::string & initialStreamPath, const std::string & targetStreamPath, bool isInsert, targetStreamType streamType) {
    std::vector<operation> initialStream;
    std::vector<operation> targetStream;
    operationType opType = isInsert ? operationType::INSERT : operationType::DELETE;

    if (streamType == targetStreamType::FULL) {
        if (!isInsert) {
            for (auto & e : edgeList_) {
                initialStream.push_back({operationType::INSERT, e});
            }
        }
        for (auto & e : edgeList_) {
            targetStream.push_back({opType, e});
        }
    }

    else if (streamType == targetStreamType::GENERAL) {
        uint64_t initialSize = edgeList_.size() * initialGraphRatio_;
        if (isInsert) {
            for (uint64_t i = 0; i < initialSize; i++) {
                initialStream.push_back({operationType::INSERT, edgeList_[i]});
            }
            for (uint64_t i = initialSize; i < edgeList_.size(); i++) {
                targetStream.push_back({operationType::INSERT, edgeList_[i]});
            }
        }

        else {
            for (auto & e : edgeList_) {
                initialStream.push_back({operationType::INSERT, e});
            }
            for (uint64_t i = initialSize; i < edgeList_.size(); i++) {
                targetStream.push_back({operationType::DELETE, edgeList_[i]});
            }
        }
    }

    else if (streamType == targetStreamType::HIGH_DEGREE) { 
        uint64_t targetEdgeCount = highDegreeEdgeCount_ * highEdgeRatio_;
        if (isInsert) {
            for (auto & e : edgeList_) {
                if ((degreeDistribution_[e.source] > 512 && degreeDistribution_[e.destination] > 512) && (highDegreeNodes_.find(e.source) != highDegreeNodes_.end() && highDegreeNodes_.find(e.destination) != highDegreeNodes_.end())) {
                    targetStream.push_back({operationType::INSERT, e});
                }
                else {
                    initialStream.push_back({operationType::INSERT, e});
                }
            }
        }

        else {
            for (auto & e : edgeList_) {
                initialStream.push_back({operationType::INSERT, e});
            }
            for (auto & e : edgeList_) {
                if ((degreeDistribution_[e.source] > 512 && degreeDistribution_[e.destination] > 512) && (highDegreeNodes_.find(e.source) != highDegreeNodes_.end() && highDegreeNodes_.find(e.destination) != highDegreeNodes_.end())) {
                    targetStream.push_back({operationType::DELETE, e});
                }
            }
        }
    }

    else if (streamType == targetStreamType::LOW_DEGREE) {
        if (isInsert) {
            for (auto & e : edgeList_) {
                if (degreeDistribution_[e.source] < 256 && degreeDistribution_[e.destination] < 256) {
                    targetStream.push_back({operationType::INSERT, e});
                }
                else {
                    initialStream.push_back({operationType::INSERT, e});
                }
            }
        }

        else {
            for (auto & e : edgeList_) {
                initialStream.push_back({operationType::INSERT, e});
            }
            for (auto & e : edgeList_) {
                if (degreeDistribution_[e.source] < 256 && degreeDistribution_[e.destination] < 256) {
                    targetStream.push_back({operationType::DELETE, e});
                }
            }
        }
        std::cout << initialStream.size() << std::endl;
    }

    else if (streamType == targetStreamType::UNIFORM) {
        uint64_t targetSize = insert_num_;
        std::vector<vertexID> chosenNodes(numVertices_, 0);
        selectRandomNodes(targetSize, chosenNodes, true);

        for (auto & edge : edgeList_) {
            if (chosenNodes[edge.source]) {
                targetStream.push_back({opType, edge});
                chosenNodes[edge.source]--;
            } else {
                initialStream.push_back({opType, edge});
            }
        }

        std::cout << "selected insert size uniform: " << targetStream.size() << std::endl;
    }

    else if (streamType == targetStreamType::BASED_ON_DEGREE) {
        uint64_t targetSize = insert_num_;

        std::vector<vertexID> chosenNodes(numVertices_, 0);
        selectRandomNodes(targetSize, chosenNodes, false);
        for (auto & edge : edgeList_) {
            if (chosenNodes[edge.source]) {
                targetStream.push_back({operationType::INSERT, edge});
                chosenNodes[edge.source]--;
            }
            
            else initialStream.push_back({operationType::INSERT, edge});
        }

        std::cout << "selected insert size based on degree: " << targetStream.size() << std::endl;
    }

    else {
        std::cerr << "Unsupported target stream type" << std::endl;
        exit(1);
    }
    std::default_random_engine engine(randomSeed_);
    std::shuffle(targetStream.begin(), targetStream.end(), engine);

    saveStream(initialStreamPath, initialStream);
    saveStream(targetStreamPath, targetStream);
}

void DataPreProcessor::updateWorkload(const std::string & initialStreamPath, const std::string & targetStreamPath) {
    std::vector<operation> initialStream;
    std::vector<operation> targetStream;

    uint64_t initialSize = edgeList_.size() * initialGraphRatio_;

    for (uint64_t i = 0; i < initialSize; i++) {
        initialStream.push_back({operationType::INSERT, edgeList_[i]});
    }

    for (uint64_t j = initialSize; j < edgeList_.size(); j++) {
        targetStream.push_back({operationType::INSERT, edgeList_[j]});
    }

    saveStream(initialStreamPath, initialStream);
    saveStream(targetStreamPath, targetStream);
}

void DataPreProcessor::benchmarkQueries(const std::string & targetStreamPath, targetStreamType streamType, operationType opType) {
    std::vector<operation> targetStream;
    std::vector<vertexID> vertices(numVertices_);
    std::iota(vertices.begin(), vertices.end(), 0);
    std::default_random_engine engine(randomSeed_);
    std::shuffle(vertices.begin(), vertices.end(), engine);

    if (streamType == targetStreamType::GENERAL) {
        if (opType == operationType::GET_VERTEX || opType == operationType::SCAN_NEIGHBOR || opType == operationType::GET_NEIGHBOR) {
            vertexID targetSize = numVertices_ * vertexQueryRatio_;

            for (uint64_t i = 0; i < targetSize; i++) {
                targetStream.push_back({opType, {vertices[i % numVertices_], 0, 0}});
            }
        }

        else if (opType == operationType::GET_EDGE || opType == operationType::GET_WEIGHT) {
            uint64_t targetSize = edgeList_.size() * edgeQueryRatio_;
            for (uint64_t i = 0; i < targetSize; i++) {
                targetStream.push_back({opType, edgeList_[i]});
            }
        }

        else {
            std::cerr << "Unsupported operation type" << std::endl;
            exit(1);
        }
    }

    else if (streamType == targetStreamType::HIGH_DEGREE) {
        if (opType == operationType::GET_VERTEX || opType == operationType::SCAN_NEIGHBOR || opType == operationType::GET_NEIGHBOR) {
            for (uint64_t i = 0; i < numVertices_; i++) {
                if (highDegreeNodes_.find(i) != highDegreeNodes_.end() && degreeDistribution_[i] > 512) {
                    targetStream.push_back({opType, {i, 0, 0}});
                }
            }
            std::cout << "high degree nodes" << targetStream.size() << std::endl;
        } 

        else if (opType == operationType::GET_EDGE || opType == operationType::GET_WEIGHT ) {
            uint64_t targetSize = highDegreeEdgeCount_ * highEdgeRatio_;
            for (auto & e : edgeList_) {
                if ((degreeDistribution_[e.source] > 512 && degreeDistribution_[e.destination] > 512) && (highDegreeNodes_.find(e.source) != highDegreeNodes_.end() && highDegreeNodes_.find(e.destination) != highDegreeNodes_.end())) {
                    targetStream.push_back({opType, e});
                }
            }
            std::cout << "high degree edges" << targetStream.size() << std::endl;
        }

        else {
            std::cerr << "Unsupported operation type" << std::endl;
            exit(1);
        }
    }

    else if (streamType == targetStreamType::LOW_DEGREE) {
        if (opType == operationType::GET_VERTEX || opType == operationType::SCAN_NEIGHBOR || opType == operationType::GET_NEIGHBOR) {
            for (uint64_t i = 0; i < numVertices_; i++) {
                if (degreeDistribution_[i] < 256) {
                    targetStream.push_back({opType, {i, 0, 0}});
                }
            }
            std::cout << "low degree nodes" << targetStream.size() << std::endl;
        }

        else if (opType == operationType::GET_EDGE || opType == operationType::GET_WEIGHT ) {
            for (auto & e : edgeList_) {
                if (degreeDistribution_[e.source] < 256 && degreeDistribution_[e.destination] < 256) {
                    targetStream.push_back({opType, e});
                }
            }
            std::cout << "low degree edges" << targetStream.size() << std::endl;
        }

        else {
            std::cerr << "Unsupported operation type" << std::endl;
            exit(1);
        }
    }


    else if (streamType == targetStreamType::UNIFORM) {
        if (opType == operationType::GET_VERTEX || opType == operationType::SCAN_NEIGHBOR || opType == operationType::GET_NEIGHBOR) {
            uint64_t targetSize = scan_num_;
            for (uint64_t i = 0; i < targetSize; i++) {
                targetStream.push_back({opType, {vertices[i % numVertices_], 0, 0}});
            }
        }
        
        else if (opType == operationType::GET_EDGE || opType == operationType::GET_WEIGHT) {
            uint64_t targetSize = search_num_;
            std::vector<vertexID> chosenNodes(numVertices_, 0);
            selectRandomNodes(targetSize, chosenNodes, true);

            for (auto & edge : edgeList_) {
                if (chosenNodes[edge.source]) {
                    targetStream.push_back({opType, edge});
                    chosenNodes[edge.source]--;
                }
            }
            std::cout << "selected search size uniform: " << targetStream.size() << std::endl;
        }
        
    }

    else if (streamType == targetStreamType::BASED_ON_DEGREE) {
        if (opType == operationType::SCAN_NEIGHBOR) {
            uint64_t targetSize = scan_num_;
            for (uint64_t i = 0; i < targetSize; i++) {
                auto src = selectNodeByDegree();
                targetStream.push_back({opType, {src, 0, 0}});
            }
        }
        else if (opType == operationType::GET_EDGE) {
            uint64_t targetSize = search_num_;
            uint64_t i = 0, src = 0;
            for (uint64_t i = 0; i < targetSize; i++) {
                while(true) {
                    src = selectNodeByDegree();
                    if (graph_[src].size() != 0) break;
                }
                size_t index = std::rand() % graph_[src].size();
                targetStream.push_back({opType, graph_[src][index]});
                
            }

            std::cout << "selected search size based on degree: " << targetStream.size() << std::endl;
        }
    }

    else {
        std::cerr << "Unsupported target stream type" << std::endl;
        exit(1);
    }
    std::shuffle(targetStream.begin(), targetStream.end(), engine);

    saveStream(targetStreamPath, targetStream);
}

void DataPreProcessor::initialAnalyticQueries(const std::string & initialStreamPath) {
    std::vector<operation> initialStream;

    for (auto & e : edgeList_) {
        initialStream.push_back({operationType::INSERT, e});
    }

    saveStream(initialStreamPath, initialStream);
}

void DataPreProcessor::targetAnalyticQueries(const std::string & targetStreamPath) {
    std::vector<operation> targetStream;

    std::default_random_engine engine(randomSeed_);
    std::uniform_int_distribution<vertexID> vertexDistribution(0, numVertices_ - 1);
    vertexID source = vertexDistribution(engine);

    targetStream.push_back({operationType::BFS, {source, 0, 0}});
    targetStream.push_back({operationType::PAGE_RANK, {0, 0, 0}});
    targetStream.push_back({operationType::SSSP, {source, 0, 0}});
    targetStream.push_back({operationType::TC, {0, 0, 0}});
    targetStream.push_back({operationType::WCC, {0, 0, 0}});

    saveStream(targetStreamPath, targetStream);
}

void DataPreProcessor::initialMixedQueries(const std::string & initialStreamPath) {
    std::vector<operation> initialStream;

    uint64_t initialSize = edgeList_.size() * initialGraphRatio_;

    for (uint64_t i = 0; i < initialSize; i++) {
        initialStream.push_back({operationType::INSERT, edgeList_[i]});
    }

    saveStream(initialStreamPath, initialStream);
}

void DataPreProcessor::targetMixedQueries(const std::string & targetStreamPath, operationType opType) {
    std::vector<operation> targetStream;

    int maxThreads = 32;
    
    uint64_t batchSize = (edgeList_.size() * (1 - initialGraphRatio_) + maxThreads - 1) / maxThreads;
    std::vector<vertexID> vertices(numVertices_);
    std::iota(vertices.begin(), vertices.end(), 0);
    std::default_random_engine engine(randomSeed_);
    std::shuffle(vertices.begin(), vertices.end(), engine);
    
    uint64_t initialSize = edgeList_.size() * initialGraphRatio_;
    for (int i = 0; i < maxThreads; i++) {
        uint64_t start = initialSize + i * batchSize;
        uint64_t end = start + batchSize;
        if (end > edgeList_.size()) {
            end = edgeList_.size();
        }
        
        for (uint64_t j = start; j < end; j++) {
            targetStream.push_back({operationType::INSERT, edgeList_[j]});
        }
        targetStream.push_back({opType, {vertices[i % numVertices_], 0, 0}});
    }

    saveStream(targetStreamPath, targetStream);
}

void DataPreProcessor::generateAllWorkloads(const std::string & dirPath) {
    std::string initialStreamPath = dirPath + "/initial_stream";
    std::string targetStreamPath = dirPath + "/target_stream";

    insertInitialVertices(initialStreamPath + "_insert_vertex.stream");

    // Insert workloads
    processWorkload(initialStreamPath + "_insert_full.stream", targetStreamPath + "_insert_full.stream", true, targetStreamType::FULL);
    processWorkload(initialStreamPath + "_insert_general.stream", targetStreamPath + "_insert_general.stream", true, targetStreamType::GENERAL);
    processWorkload(initialStreamPath + "_insert_uniform.stream", targetStreamPath + "_insert_uniform.stream", true, targetStreamType::UNIFORM);
    processWorkload(initialStreamPath + "_insert_based_on_degree.stream", targetStreamPath + "_insert_based_on_degree.stream", true, targetStreamType::BASED_ON_DEGREE);
    

    // Query and analytic workloads
    initialAnalyticQueries(initialStreamPath + "_analytic.stream");
    benchmarkQueries(targetStreamPath + "_get_edge_general.stream", targetStreamType::GENERAL, operationType::GET_EDGE);
    benchmarkQueries(targetStreamPath + "_scan_neighbor_general.stream", targetStreamType::GENERAL, operationType::SCAN_NEIGHBOR);
    benchmarkQueries(targetStreamPath + "_get_edge_based_on_degree.stream", targetStreamType::BASED_ON_DEGREE, operationType::GET_EDGE);
    benchmarkQueries(targetStreamPath + "_scan_neighbor_uniform.stream", targetStreamType::UNIFORM, operationType::SCAN_NEIGHBOR);
    benchmarkQueries(targetStreamPath + "_scan_neighbor_based_on_degree.stream", targetStreamType::BASED_ON_DEGREE, operationType::SCAN_NEIGHBOR);
}
