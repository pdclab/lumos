/*
   Modifications Copyright (C) 2018-2019 Keval Vora (keval@cs.sfu.ca)
   Copyright (c) 2014-2015 Xiaowei Zhu, Tsinghua University

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#include "core/graph.hpp"

int main(int argc, char ** argv) {
	if (argc<3) {
		fprintf(stderr, "usage: pagerank [path] [iterations] [memory budget in GB]\n");
		exit(-1);
	}
	std::string path = argv[1];
	int iterations = atoi(argv[2]);
	long memory_bytes = (argc>=4)?atol(argv[3])*1024l*1024l*1024l:8l*1024l*1024l*1024l;

	Graph graph(path, memory_bytes);
	graph.load_outdegrees();
	BigVector<float> pagerank(graph.path+"/pagerank", graph.vertices);
	BigVector<float> sum(graph.path+"/sum", graph.vertices);

	long vertex_data_bytes = (long)graph.vertices * ( sizeof(VertexId) + sizeof(float) + sizeof(float) );
	graph.set_vertex_data_bytes(vertex_data_bytes);

	double begin_time = get_time();
	graph.hint(pagerank, sum);
	graph.stream_vertices<VertexId>(
		[&](VertexId i){
			pagerank[i] = 1.f / graph.outdegrees[i];
			sum[i] = 0.0;
			return 0;
		}, nullptr, 0,
		[&](std::pair<VertexId,VertexId> vid_range){
			pagerank.load(vid_range.first, vid_range.second);
			sum.load(vid_range.first, vid_range.second);
		},
		[&](std::pair<VertexId,VertexId> vid_range){
			pagerank.save();
			sum.save();
		}
	);

	for (int iter=0;iter<iterations;iter++) {
		graph.hint(pagerank);

		graph.stream_edges<VertexId, Empty>(
			[&](Edge<Empty> & e){
				write_add(&sum[e.target], pagerank[e.source]);
				return 0;
			}, nullptr, 0, 1,
			[&](std::pair<VertexId,VertexId> source_vid_range){
				pagerank.lock(source_vid_range.first, source_vid_range.second);
			},
			[&](std::pair<VertexId,VertexId> source_vid_range){
				pagerank.unlock(source_vid_range.first, source_vid_range.second);
			}
		);
		graph.hint(pagerank, sum);
		double stream_vertices_time = 0.0;
		if (iter==iterations-1) {
			graph.stream_vertices<VertexId>(
				[&](VertexId i){
					pagerank[i] = 0.15f + 0.85f * sum[i];
					return 0;
				}, nullptr, 0,
				[&](std::pair<VertexId,VertexId> vid_range){
					pagerank.load(vid_range.first, vid_range.second);
				},
				[&](std::pair<VertexId,VertexId> vid_range){
					pagerank.save();
				}
			);
		} else {
			graph.stream_vertices<int>(
				[&](VertexId i){
					pagerank[i] = (0.15f + 0.85f * sum[i]) / graph.outdegrees[i];
					sum[i] = 0.0;
					return 0;
				}, nullptr, 0,
				[&](std::pair<VertexId,VertexId> vid_range){
					pagerank.load(vid_range.first, vid_range.second);
					sum.load(vid_range.first, vid_range.second);
				},
				[&](std::pair<VertexId,VertexId> vid_range){
					pagerank.save();
					sum.save();
				}
			);
		}
	}
	double end_time = get_time();
	printf("%d iterations of pagerank took %.2f seconds\n", iterations, end_time - begin_time);

	graph.unload_outdegrees();  
	if(graph.is_advanced()) graph.load_mapping_to_new();
	for(int i=0; i<std::min(graph.vertices, 10); ++i) {
		printf("%d: %f\n", i, graph.is_advanced() ? pagerank[graph.otn[i]] : pagerank[i]);
	}
}
