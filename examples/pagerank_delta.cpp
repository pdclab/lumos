/*
   Copyright (C) 2018-2019 Keval Vora (keval@cs.sfu.ca)

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

const float tolerance = 0.01;

int main(int argc, char ** argv) {
	if (argc<3) {
		fprintf(stderr, "usage: pagerank [path] [iterations] [memory budget in GB]\n");
		exit(-1);
	}
	std::string path = argv[1];
	int iterations = atoi(argv[2]);
	long memory_bytes = (argc>=4)?atol(argv[3])*1024l*1024l*1024l:64l*1024l*1024l*1024l;

	Graph graph(path, memory_bytes);
	graph.load_outdegrees();
	BigVector<float> pagerank(graph.path+"/pagerank", graph.vertices);
	BigVector<float> sum(graph.path+"/sum", graph.vertices);
	BigVector<float> second_sum(graph.path+"/secondsum", graph.vertices);
	BigVector<float> delta(graph.path+"/delta", graph.vertices);
	BigVector<float> new_delta(graph.path+"/new_delta", graph.vertices);

	long vertex_data_bytes = (long)graph.vertices * ( sizeof(VertexId) + 5 * sizeof(float) );
	graph.set_vertex_data_bytes(vertex_data_bytes);

	double begin_time = get_time();
	graph.hint(pagerank, sum, second_sum);
	graph.stream_vertices<VertexId>(
		[&](VertexId i){
			pagerank[i] = 1.f / graph.outdegrees[i];
			sum[i] = 0.0;
			second_sum[i] = 0.0;
			delta[i] = pagerank[i];
			new_delta[i] = 0.0;
			return 0;
		}, nullptr, 0,
		[&](std::pair<VertexId,VertexId> vid_range){
			pagerank.load(vid_range.first, vid_range.second);
			sum.load(vid_range.first, vid_range.second);
			second_sum.load(vid_range.first, vid_range.second);
			delta.load(vid_range.first, vid_range.second);
			new_delta.load(vid_range.first, vid_range.second);
		},
		[&](std::pair<VertexId,VertexId> vid_range){
			pagerank.save();
			sum.save();
			second_sum.save();
			delta.save();
			new_delta.save();
		}
	);

	Bitmap * active_in = graph.alloc_bitmap();
	Bitmap * active_out = graph.alloc_bitmap();

	active_out->fill();
	VertexId active_vertices = graph.vertices;
	EdgeId edges_processed = 0;
	EdgeId secondary_edges_processed = 0;

	for (int iter=0;iter<iterations;iter++) {
		printf("Iteration %d: active vertices = %d\n", iter, active_vertices); 
		std::swap(active_in, active_out);
		active_out->clear();

		if(iter % 2 == 0) {
			graph.hint(pagerank, sum);
			std::tie(edges_processed, secondary_edges_processed, active_vertices) = graph.stream_primary_edges<EdgeId, EdgeId, VertexId, Empty>(
				[&](Edge<Empty> & e){
					write_add(&sum[e.target], delta[e.source]);
					return 1;
				},
				[&](Edge<Empty> & e){
					write_add(&second_sum[e.target], new_delta[e.source]);
					return 1;
				},
				active_in, active_out, 0, 0, 1, 
				[&](std::pair<VertexId,VertexId> source_vid_range){
					delta.lock(source_vid_range.first, source_vid_range.second);
				},
				[&](std::pair<VertexId,VertexId> source_vid_range){
					delta.unlock(source_vid_range.first, source_vid_range.second);
				},
				f_none_1, f_none_1,
				[&](VertexId i){
					float newpr = (0.15f + 0.85f * sum[i]) / graph.outdegrees[i];
					new_delta[i] = newpr - pagerank[i];
					int ret = 0;
					if(fabs(new_delta[i]) > tolerance) {
						active_out->set_bit(i);
						graph.secondary_activated(i);
						ret = 1;
					}
					return ret;
				}, nullptr, 0,
				[&](std::pair<VertexId,VertexId> vid_range){
					new_delta.load(vid_range.first, vid_range.second);
				},
				[&](std::pair<VertexId,VertexId> vid_range){
					new_delta.save();
				} 
			);
			printf("edges processed = %ld\n", edges_processed);
			graph.hint(pagerank, sum);
			if(iter!=iterations-1) {
			graph.stream_vertices<VertexId>(
				[&](VertexId i){
					if(fabs(new_delta[i]) > tolerance) {
						pagerank[i] = (0.15f + 0.85f * sum[i]) / graph.outdegrees[i];
					}
					sum[i] += second_sum[i];
					second_sum[i] = 0.0;
					delta[i] = new_delta[i];
					return 0;
				}, nullptr, 0,
				[&](std::pair<VertexId,VertexId> vid_range){
				 	pagerank.load(vid_range.first, vid_range.second);
					sum.load(vid_range.first, vid_range.second);
					second_sum.load(vid_range.first, vid_range.second);
					delta.load(vid_range.first, vid_range.second);
				},
				[&](std::pair<VertexId,VertexId> vid_range){
					pagerank.save();
					sum.save();
					second_sum.save();
					delta.save();
				}
			);
			}
		} else {
			graph.hint(pagerank);
			secondary_edges_processed += graph.stream_secondary_edges<VertexId, Empty>(
				[&](Edge<Empty> & e){
					write_add(&sum[e.target], delta[e.source]);
					return 1;
				}, active_in, 0, 1,
				[&](std::pair<VertexId,VertexId> source_vid_range){
					delta.lock(source_vid_range.first, source_vid_range.second);
				},
				[&](std::pair<VertexId,VertexId> source_vid_range){
					delta.unlock(source_vid_range.first, source_vid_range.second);
				}
			);
			printf("edges processed = %ld\n", secondary_edges_processed);
			graph.hint(pagerank, sum);
			if (iter!=iterations-1) {
				graph.hint(pagerank, sum, second_sum);
				active_vertices = graph.stream_vertices<VertexId>(
					[&](VertexId i) {
						float newpr = (0.15f + 0.85f * sum[i]) / graph.outdegrees[i];
						delta[i] = newpr - pagerank[i];
						int ret = 0;
						if(fabs(delta[i]) > tolerance) {
							pagerank[i] = newpr;
							active_out->set_bit(i);
							ret = 1;
						}
						return ret;
					}, nullptr, 0,
					[&](std::pair<VertexId,VertexId> vid_range){
						delta.load(vid_range.first, vid_range.second);
						pagerank.load(vid_range.first, vid_range.second);
					},
					[&](std::pair<VertexId,VertexId> vid_range){
						delta.save();
						pagerank.save();
					}
				);
			}
		}

		if (iter==iterations-1) {
			graph.hint(pagerank);
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
