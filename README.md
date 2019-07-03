# Lumos
**Dependency-Driven Disk-based Graph Processing**

## What is it?
Lumos is a single machine disk-based large-scale graph processing system. It improves over [GridGraph](https://github.com/kevalvora/lumos-internal#benchmarking-summary) using dependency-driven cross-iteration value propagation. [[Read more](https://www.cs.sfu.ca/~keval/contents/papers/lumos-atc19.pdf)]

## Getting Started
### 1. Installation
To compile applications, run the following on root directory:
```
make
```
> You may need to raise the limit of maximum open file descriptors (./tools/raise\_ulimit\_n.sh).
### 2. Preprocessing

Lumos operates on the primary layout (grid) of input graphs. The primary layout can be constructed using the ``preprocess`` tool, as described here:
```
./bin/preprocess -i [input path] -o [output path] -v [vertices] -p [partitions] -t [edge type: 0=unweighted, 1=weighted] -m [mode: 0=advanced, 1=naive]
```
**-i**: Path to input graph file in binary edge list format. This format consists of contiguous list of edge tuples, where each edge tuple is a:
- <4 byte source, 4 byte destination> pair for unweighted graphs; or,
- <4 byte source, 4 byte destination, 4 byte float typed weight> triplet for weighted graphs.

**-o**: Path to output graph file.
**-v**: Number of vertices in graph.
**-p**: Number of partitions in the primary layout.
**-t**: Type of input graph. ``0`` for unweighted graphs and ``1`` for weighted graphs.
**-m**: Mode of graph partitioning. ``0`` for advanced partitioning which enables high cross-iteration propagation and ``1`` for naive partitioning. Recommended value = ``0`` .

For example, to create the primary layout of 8x8 grid for the unweighted graph [LiveJournal](http://snap.stanford.edu/data/soc-LiveJournal1.html) which is in binary edge list format, we run:
```
./bin/preprocess -i LiveJournal -o LiveJournal.pl -v 4847571 -p 8 -t 0 -m 0
```
### 3. Running Graph Algorithms
To run graph algorithms, provide the primary layout path and the memory budget (in GB), along with other program parameters (e.g., the number of iterations). The general template is:
```
./bin/[binary name] [path] [...] [memory budget]
```
To demonstrate the capabilities of Lumos, we provide multiple versions of ``PageRank`` algorithm below.

#### 3.1. PageRank on Lumos (source: examples/pagerank.cpp)
This version uses Lumos's ``stream_primary_edges`` and ``stream_secondary_edges`` methods to process using cross-iteration value propagation technique. Following command runs ``pagerank`` on ``LiveJournal.pl`` for ``30`` iterations with ``14``GB memory limit:
```
./bin/pagerank LiveJournal.pl 30 14
```

#### 3.2. PageRank Delta on Lumos (source: examples/pagerank_delta.cpp)
This version uses Lumos's ``stream_primary_edges`` and ``stream_secondary_edges`` methods along with selective scheduling and incremental computation.  Following command runs ``pagerank_delta`` on ``LiveJournal.pl`` for ``30`` iterations with ``14``GB memory limit:
```
./bin/pagerank_delta LiveJournal.pl 30 14
```

#### 3.3. PageRank on GridGraph
For comparison, we have ``pagerank_gg`` which uses GridGraph's ``stream_edges`` based processing:
```
./bin/pagerank_gg LiveJournal.pl 30 14
```

## Benchmarking Summary
Below performance numbers compare ``pagerank`` and ``pagerank_delta`` with ``pagerank_gg`` on [Twitter](http://konect.uni-koblenz.de/networks/twitter) graph with ``6``GB memory budget. Detailed results with larger graphs and bigger machines can be found [here](https://www.cs.sfu.ca/~keval/contents/papers/lumos-atc19.pdf).

|      Variants     |    2 Iterations   |   10 Iterations   |   20 Iterations   |   30 Iterations   |
|:-----------------:|:-----------------:|:-----------------:|:-----------------:|:-----------------:|
|      pagerank     |     **129 sec**   |     **532 sec**   |      1,027 sec    |      1,670 sec    |
|   pagerank_delta  |       131 sec     |       539 sec     |     **974 sec**   |    **1,093 sec**  |
|    pagerank_gg    |       214 sec     |      1,039 sec    |      2,046 sec    |      3,440 sec    |

As we can observe, ``pagerank`` and ``pagerank_delta`` significantly outperform ``pagerank_gg``. Furthermore, as iterations increase, ``pagerank_delta`` outperforms ``pagerank`` since computations become sparser in later iterations. 

## Improvements
There are few opportunities to improve Lumos for contributors who'd like to enter large-scale graph processing R&D. These would be good projects for undergraduate students to get some hands-on development experience. [Email me](http://www.cs.sfu.ca/~keval/) for more.
1. Pinning secondary layout edges in memory pages to further eliminate I/O in odd iterations. 
2. Source-oriented update propagation capabilities. Implementation will be similar to the existing target-oriented propagation. 
3. Asynchronous propagation beyond dependencies for path algorithms.

## Resources
Keval Vora. [Lumos: Dependency-Driven Disk-based Graph Processing](https://www.cs.sfu.ca/~keval/contents/papers/lumos-atc19.pdf). USENIX Annual Technical Conference (ATC'19). Renton, WA, July 2019.

To cite Lumos, you can use the following BibTeX entry:
```
@inproceedings {234984,
author = {Keval Vora},
title = {{LUMOS}: Dependency-Driven Disk-based Graph Processing},
booktitle = {2019 {USENIX} Annual Technical Conference ({USENIX} {ATC} 19)},
year = {2019},
address = {Renton, WA},
url = {https://www.usenix.org/conference/atc19/presentation/vora},
publisher = {{USENIX} Association},
}
```

## Contact
[Keval Vora](http://www.cs.sfu.ca/~keval/)
