# 1. Process datasets

* liveJournal
  * Downloaded from [SNAP: Network datasets: LiveJournal social network (stanford.edu)](https://snap.stanford.edu/data/soc-LiveJournal1.html).
* Orkut
  * Downloaded from https://snap.stanford.edu/data/com-Orkut.html
* LDBC
  * Generated from [ldbc/ldbc_snb_datagen_hadoop: The Hadoop-based variant of the SNB Datagen (github.com)](https://github.com/ldbc/ldbc_snb_datagen_hadoop/)
  * Parameters
    * scale factor: sf10
    * serializer: CsvBasic
    * dataformatter: LongDateFormatter
* Graph500
  * Downloaded from  [LDBC Graphalytics Benchmark (LDBC Graphalytics) (ldbcouncil.org)](https://ldbcouncil.org/benchmarks/graphalytics/)
  * Parameters
    * scale factor: 24
* Twitter
  * Downloaded from  https://nrvis.com/download/data/soc/soc-twitter-2010.zip
* Friendster
  * Downloaded from  https://snap.stanford.edu/data/com-Friendster.html

# 2. Build

```bash
git clone (to be added)
cd RapidStore
cd dataset_preprocessor
mkdir build && cd build
cmake ..
make
cd ..
cd ..
mkdir build && cd build
cmake ..
make
```

# 3. Process Dataset and Generate Workloads

## Workloads

`./dataset_preprocessor` generates workloads for experiment use. Workloads include initial stream and target stream. The initial stream includes insert operations that load the initial graph for target stream tests. The target stream includes operations that are executed by the driver.

### Operation Types

#### Insert Workloads

* **Initial Stream**
  * `initial_initial_stream_insert_${target_stream_type}.stream`: Contains the initial graph to be loaded.
* **Target Stream**
  * `target_initial_stream_insert_${target_stream_type}.stream`: Contains the rest of the graph to be tested.

#### Micro Benchmark Tests

* **Initial Stream**
  * `initial_initial_stream_analytice.stream`: Contains the whole graph to be initialized.
* **Target Stream**
  * `target_initial_stream_scan_neighbor_${target_stream_type}.stream`: Contains the `scan` workload for micro benchmark tests.
  * `target_initial_stream_get_edge_${target_stream_type}.stream`: Contains the `search` workload for micro benchmark tests.

### Target Stream Types

* **full**
  * Used only for inserting the entire graph.
* **general**
  * Randomly selects a certain ratio of edges (for `insert` and `search`) and vertices (for `scan`). The ratio for `insert`, `search`, and `scan` is controlled by the parameters `initial_graph_ratio`, `edge_query_ratio`, and `vertex_query_ratio`, respectively.

## Prerequisites

This project requires the following environment and dependencies:

- C++ Compiler (supporting C++11 or later)
- [Boost Library](https://www.boost.org/)
- Intel Vtune Toolkit

Make sure you have the Boost library installed on your system. Refer to the [Boost official documentation](https://www.boost.org/doc/libs) for installation instructions.

## Usage

Detailed instructions on how to use the `commandLineParser` to parse command line arguments:

- `--input_file` (or `-i`): Specifies the path to the input file. Example: `--input_file=path/to/file`
- `--output_dir` (or `-o`): Specifies the directory to store the output workloads. Example: `--output_dir=path/to/file`
- `--is_weighted` (or `-w`): Indicates whether the graph is weighted. Use `true` or `false`. Default is `false`. Example: `--is_weighted=true`
- `--is_shuffle`: Indicates whether the insertion order is shuffled. Use `true` or `false`. Default is `false`. Example: `--is_shuffle=true`
- `--delimiter` (or `-d`): Specifies the delimiter used in the input file. Default is a space. Example: `--delimiter=,`
- `--initial_graph_ratio`: Specifies the initial graph ratio to the total graph in general workloads. Default is `0.8`. Example: `--initial_graph_ratio=0.5`
- `--insert_num`: Specifies the number of edges to be inserted in uniform and based_on_degree workloads in micro-benchmarks. Default is `320000`. Example: `--insert_num=320000`
- `--search_num`: Specifies the number of edges to be searched in uniform and based_on_degree workloads in micro-benchmarks. Default is `320000`. Example: `--insert_num=320000`
- `--scan_num`: Specifies the number of vertices to be scanned in uniform and based_on_degree workloads in micro-benchmarks. Default is `320000`. Example: `--insert_num=320000`
- `--edge_query_ratio`: Specifies the edge query ratio of general workloads in micro-benchmarks. Default is `0.2`. Example: `--edge_query_ratio=0.4`
- `--vertex_query_ratio`: Specifies the edge query ratio of general workloads in micro-benchmarks. Default is `0.2`. Example: `--vertex_query_ratio=0.4`
- `--seed` (or `-s`): Sets the random seed. Default is `0`. Example: `--seed=19260817`

## Input File Format

	The input should be a file where each line contains `source, destination` for unweighted graphs or `source, destination, weight` for weighted graphs. Unweighted graphs will be randomly assigned a double weight.

## Dataset_preprocessor

```
./dataset_preprocessor/dataset_preprocessor --input_dir=/path/to/your/intput/file --output_dir=/path/to/your/output/directory --is_weighted=true --is_shuffle=false --delimiter=, --initial_graph_ratio=0.8 --edge_query_ratio=0.2 --vertex_query_ratio=0.2 --seed=0 --insert_num=320000 --search_num=320000 --scan_num=320000
```

# 4. Configuration and Execution of Experiments

For experiments, we implement a test driver in `./wrapper` and an interface for each dynamic graph systems in `./wrapper/apps`. The origin implementation of dynamic graph systems are in `./libraries`. To execute the experiments, the config path in `./test_driver/driver_main.h` should be set to the real path of `config.cfg`. Some of the experiments need to change and re-compile the codes in `driver.h`, we are trying to simply it.

## Configuration

`config.cfg` provides the configuration for the test driver and decides the workload type. Here we also provide the settings to reproduce the results in the paper.

### Basic Settings

* `workload_dir`: Specifies the directory containing the workloads that are generated by the workload generator.
  * Example: `/path/to/the/workloads`
* `workload_type`: Defines the type of workload to be executed.
  * Supports: `insert`, `query`,`basic_operation`, `concurrent_read`, `concurrent_write`

### Graph Analytics Experiments

Set `workload_type` to be `query`.

* `query_num_threads`: Specifies the number of threads for query operations.
  * Examples: `1`
* `query_operation_types`: Defines the types of query operations.
  * Supports: `bfs`, `sssp`, `pr`, `wcc`, `tc`, `tc_inter`

#### BFS (Breadth-First Search)

* `alpha`: Parameter for BFS.
  * Example: `15`
* `beta`: Parameter for BFS.
  * Example: `18`
* `bfs_source`: Specifies the source vertex for BFS.
  * Example: `0`

#### SSSP (Single Source Shortest Path)

* `delta`: Parameter for SSSP.
  * Example: `2.0`
* `sssp_source`: Specifies the source vertex for SSSP.
  * Example: `0`

#### PageRank (PR)

* `num_iterations`: Specifies the number of iterations for the PR algorithm.
  * Example: `10`
* `damping_factor`: Sets the damping factor for the PR algorithm.
  * Example: `0.85`

### Insert and Scalability Experiments

Set `workload_type` to be `insert`.

* `insert_delete_checkpoint_size`: Sets the checkpoint size for insert/delete operations.
  * Example: `1000000`
* `insert_delete_num_threads`: Specifies the number of writers for insert/delete operations.
  * Example: `1`
* `target_stream_type`: Indicates the method for selecting edges and vertices.
  * Supports: `full`, `general`, `based_on_degree`

### Update Experiments

Set `workload_type` to be `micro_benchmark` and change the `execute_microbenchmarks()` to the implementation at line 674 in `driver.h`. Set `mb_operation_types` to `search` and `mb_ts_types` to `general`.

* `insert_delete_checkpoint_size`: Sets the number of round for update.
* `microbenchmark_num_threads`: Specifies the number of writers for update operations.
  * Example: `32`

### Concurrent Experiments & Memory bandwidth

Set `workload_type` to be `mixed`.

* `writer_threads`: Specifies the number of writers.
  * Example: `8`
* `reader_threads`: Specifies the number of readers.
  * Example: `8`

To get the memory bandwidth metrics during write performance evaluation with readers, change mixed functions to the second one without `small` mark. We use `Intel vtune` to finish the measuring.

### Memory Consumption Experiment

`query` workload could provide the information of memory consumption.

### Vertex Partition Size Experiment

Vertex partition size can be set by `VERTEX_GROUP_BITS` macro in `./libraries/NeoGraph/utils/config.h`. After changing the partition size, the system should be rebuilt.

### Ablation Experiment

* The ID Compression (IDC) can be enabled or disabled by `COMPRESSION_ENABLE` macro in `./libraries/NeoGraph/CMakeLists.txt`.
* The Clustered Index (CI) can be disabled by setting `RANGE_LEAF_SIZE` and `ART_EXTRACT_THRESHOLD` macro in `./libraries/NeoGraph/utils/config.h` to `0`.
* The small vector optimization (VEC) can be enabled by setting `FROM_CLUSTERED_TO_SMALL_VEC_ENABLE` macro in `./libraries/NeoGraph/utils/config.h` to `1`.
* The per-edge versioning simulation (non-SV) can be enabled by setting `SIMULATE_PER_EDGE_VERSIONING_ENABLE` macro in `./libraries/NeoGraph/utils/config.h` to `1`.
* The trivial ART structure can be enabled by modify the linked library from `c_art` to `art_new` in `./libraries/NeoGraph/CMakeLists.txt`. Note that some included headers are also needed to be changed.

### Batch Update Mixed Experiment

Change the current mixed workload test functions in Driver to the corresponding ones with `small` mark to respectively get batch update and loopup performance. And execute the concurrent experiments. We use `insert_delete_checkpoint_size` in `config.cfg` to set batch size. Note that due to not-full incompatibility between systems and test framework, we need to turn real number of threads to negative to enable corresponding code path.

### MicroBenchmark Experiments

Set `workload_type` to be `micro_benchmark`.

* `mb_repeat_times`: Specifies the number of times the experiment should be repeated.
  * Example: `0`
* `mb_checkpoint_size`: Sets the checkpoint size for experiment.
  * Example: `100`
* `microbenchmark_num_threads`: Specifies the number of threads for experiment.
  * Examples: `1`, `2`, `4`, `8`, `16`, `32`
* `mb_operation_types`: Defines the types of basic operations for the experiment.
  * Examples: `search`, `scan`
* `mb_ts_types`: Defines the setting of target selection for the experiment.
  * Supports: `general`, `low_degree`, `high_degree`

## 5. Execute the Experiments

```
# CSR
./build/wrapper/csr_wrapper.out
# Sortledton
./build/wrapper/sortledton_wrapper.out
# Teseo
./build/wrapper/teseo_wrapper.out
# LiveGraph
./build/wrapper/livegraph_wrapper.out
# Aspen
./build/wrapper/aspen_wrapper.out
# RapidStore
./build/wrapper/neo_wrapper.out
```

## 6. Third-party Modules

We use the test framework from https://github.com/SJTU-Liquid/DynamicGraphStorage to conduct experiments. You can also access the link for more information about tthe test framework.

Download the official libraries to the [./libraries](./libraries) directory and integrate them using the APIs defined in [./wrapper/apps](./wrapper/apps). Users can add the corresponding subdirectory in [./CMakeLists.txt](./CMakeLists.txt), and integrate them in [./wrapper/CMakeLists.txt](./wrapper/CMakeLists.txt) to create instances for evaluation.

* Download the official library of `LiveGraph` from [LiveGraph](https://github.com/thu-pacman/LiveGraph-Binary/releases).
* Download the official library of `Teseo` from [Teseo](https://github.com/cwida/teseo).
  * Integrated using [./libraries/teseo/CMakeLists.txt](./libraries/teseo/CMakeLists.txt)
* Download official library of `aspen` from [Aspen](https://github.com/ldhulipala/aspen/).
  * Integrated using [./libraries/teseo/CMakeLists.txt](./libraries/teseo/CMakeLists.txt)
* Download official library of `Sortledton` from [Sortledton](https://gitlab.db.in.tum.de/per.fuchs/sortledton)