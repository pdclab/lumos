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

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <malloc.h>
#include <errno.h>
#include <assert.h>
#include <string.h>

#include <string>
#include <vector>
#include <thread>
#include <algorithm>

#include "core/constants.hpp"
#include "core/type.hpp"
#include "core/filesystem.hpp"
#include "core/queue.hpp"
#include "core/partition.hpp"
#include "core/time.hpp"
#include "core/atomic.hpp"

long PAGESIZE = 4096;

uint32_t* in_degrees = NULL;
uint32_t* out_degrees = NULL;

struct pair {
	VertexId id;
	uint32_t in_degree;
	uint32_t out_degree;
};

VertexId* mapper = NULL;

void renumber(std::string filename, std::string output, VertexId num_vertices, int edge_type) {
	std::vector<struct pair> vertices(num_vertices);
	for(VertexId i=0; i<num_vertices; ++i) {
		vertices[i].id = i;
		vertices[i].in_degree = 0;
		vertices[i].out_degree = 0; 
	}

	int parallelism = std::thread::hardware_concurrency();

	int edge_unit;
	EdgeId edges;
	switch (edge_type) {
	case 0:
		edge_unit = sizeof(VertexId) * 2;
		edges = file_size(filename) / edge_unit;
		break;
	case 1:
		edge_unit = sizeof(VertexId) * 2 + sizeof(Weight);
		edges = file_size(filename) / edge_unit;
		break;
	default:
		fprintf(stderr, "edge type (%d) is not supported.\n", edge_type);
		exit(-1);
	}

	char ** buffers = new char * [parallelism*2];
	bool * occupied = new bool [parallelism*2];
	for (int i=0;i<parallelism*2;i++) {
		buffers[i] = (char *)memalign(PAGESIZE, IOSIZE);
		occupied[i] = false;
	}

	Queue<std::tuple<int, long> > tasks(parallelism);
	std::vector<std::thread> threads;
	for (int ti=0;ti<parallelism;ti++) {
		threads.emplace_back([&]() {
				VertexId source, target;
				while (true) {
					int cursor;
					long bytes;
					std::tie(cursor, bytes) = tasks.pop();
					if (cursor==-1) break;
					char * buffer = buffers[cursor];
					for (long pos=0;pos<bytes;pos+=edge_unit) {
						source = *(VertexId*)(buffer+pos);
						target = *(VertexId*)(buffer+pos+sizeof(VertexId));
						write_add(&vertices[target].in_degree, (uint32_t) 1);
						write_add(&vertices[source].out_degree, (uint32_t) 1);
					} 
					occupied[cursor] = false;
				}
		});
	}

	int fin = open(filename.c_str(), O_RDONLY);
	if (fin==-1) printf("%s\n", strerror(errno));
	assert(fin!=-1);
	int cursor = 0;
	long total_bytes = file_size(filename);
	uint64_t read_bytes = 0;
	while (true) {
		long bytes = read(fin, buffers[cursor], IOSIZE);
		assert(bytes!=-1);
		if (bytes==0) break;
		occupied[cursor] = true;
		tasks.push(std::make_tuple(cursor, bytes));
		read_bytes += bytes;
		printf("progress: %.2f%%\r", 100. * read_bytes / total_bytes);
		fflush(stdout);
		while (occupied[cursor]) {
			cursor = (cursor + 1) % (parallelism * 2);
		}
	}
	close(fin);
	assert(read_bytes==edges*edge_unit);

	for (int ti=0;ti<parallelism;ti++) {
		tasks.push(std::make_tuple(-1, 0));
	}

	for (int ti=0;ti<parallelism;ti++) {
		threads[ti].join();
	}
	threads.clear();
	printf("\n");
	printf("renumbering done\n");
	std::sort(vertices.begin(), vertices.end(), [&](struct pair left, struct pair right) {
		return (((left.in_degree * 1.0) / (left.out_degree)) < ((right.in_degree * 1.0) / (right.out_degree)));
	});

	mapper = new VertexId[num_vertices];
	#pragma omp parallel for num_threads(parallelism)
	for(VertexId i=0; i<num_vertices; ++i) {
		mapper[vertices[i].id] = i;
	}

	if (file_exists(output))
		remove_directory(output);
	create_directory(output);

	uint32_t* degrees = new uint32_t[num_vertices];
	#pragma omp parallel for num_threads(parallelism)
	for(VertexId i=0; i<num_vertices; ++i) {
		degrees[i] = vertices[i].in_degree;
	}

	int fout_in_deg = open((output+"/indegrees").c_str(), O_WRONLY|O_APPEND|O_CREAT, 0644);
	write(fout_in_deg, degrees, num_vertices * sizeof(uint32_t));
	close(fout_in_deg);

	#pragma omp parallel for num_threads(parallelism)
	for(VertexId i=0; i<num_vertices; ++i) {
		degrees[i] = vertices[i].out_degree;
	}

	int fout_out_deg = open((output+"/outdegrees").c_str(), O_WRONLY|O_APPEND|O_CREAT, 0644);
	write(fout_out_deg, degrees, num_vertices * sizeof(uint32_t));
	close(fout_out_deg);

	delete[] degrees;

	int fout_mapper = open((output+"/otn").c_str(), O_WRONLY|O_APPEND|O_CREAT, 0644);
	write(fout_mapper, mapper, num_vertices * sizeof(VertexId));
	close(fout_mapper);

	VertexId* rev_mapper = new VertexId[num_vertices];
	#pragma omp parallel for num_threads(parallelism)
	for(VertexId i=0; i<num_vertices; ++i) {
		rev_mapper[mapper[i]] = i;
	}

	fout_mapper = open((output+"/nto").c_str(), O_WRONLY|O_APPEND|O_CREAT, 0644);
	write(fout_mapper, rev_mapper, num_vertices * sizeof(VertexId));
	close(fout_mapper);

	delete[] rev_mapper;

	printf("mapping done\n");

	for (int i=0;i<parallelism*2;i++)
		delete[] buffers[i];
	delete[] buffers;
	delete[] occupied;
}

void generate_edge_grid(std::string input, std::string output, VertexId vertices, int partitions, int edge_type, bool advanced) {
	int parallelism = std::thread::hardware_concurrency();
	int edge_unit;
	EdgeId edges;
	switch (edge_type) {
	case 0:
		edge_unit = sizeof(VertexId) * 2;
		edges = file_size(input) / edge_unit;
		break;
	case 1:
		edge_unit = sizeof(VertexId) * 2 + sizeof(Weight);
		edges = file_size(input) / edge_unit;
		break;
	default:
		fprintf(stderr, "edge type (%d) is not supported.\n", edge_type);
		exit(-1);
	}
	printf("vertices = %d, edges = %ld\n", vertices, edges);

	if(!advanced) {
		in_degrees = new uint32_t[vertices];
		out_degrees = new uint32_t[vertices];
		for(VertexId i=0; i<vertices; ++i) {
			in_degrees[i] = 0;
			out_degrees[i] = 0;
		}
	}

	char ** buffers = new char * [parallelism*2];
	bool * occupied = new bool [parallelism*2];
	for (int i=0;i<parallelism*2;i++) {
		buffers[i] = (char *)memalign(PAGESIZE, IOSIZE);
		occupied[i] = false;
	}
	Queue<std::tuple<int, long> > tasks(parallelism);
	int ** fout;
	std::mutex ** mutexes;
	fout = new int * [partitions];
	mutexes = new std::mutex * [partitions];
	if(!advanced) {
		if (file_exists(output)) {
			remove_directory(output);
		}
		create_directory(output);
	}

	const int grid_buffer_size = 768; // 12 * 8 * 8
	char * global_grid_buffer = (char *) memalign(PAGESIZE, grid_buffer_size * partitions * partitions);
	char *** grid_buffer = new char ** [partitions];
	int ** grid_buffer_offset = new int * [partitions];
	for (int i=0;i<partitions;i++) {
		mutexes[i] = new std::mutex [partitions];
		fout[i] = new int [partitions];
		grid_buffer[i] = new char * [partitions];
		grid_buffer_offset[i] = new int [partitions];
		for (int j=0;j<partitions;j++) {
			char filename[4096];
			sprintf(filename, "%s/block-%d-%d", output.c_str(), i, j);
			fout[i][j] = open(filename, O_WRONLY|O_APPEND|O_CREAT, 0644);
			grid_buffer[i][j] = global_grid_buffer + (i * partitions + j) * grid_buffer_size;
			grid_buffer_offset[i][j] = 0;
		}
	}

	uint64_t propagation = 0;
	std::vector<std::thread> threads;
	for (int ti=0;ti<parallelism;ti++) {
		threads.emplace_back([&]() {
			char * local_buffer = (char *) memalign(PAGESIZE, IOSIZE);
			int * local_grid_offset = new int [partitions * partitions];
			int * local_grid_cursor = new int [partitions * partitions];
			uint64_t local_propagation = 0;

			VertexId source, target;
			Weight weight;
			while (true) {
				int cursor;
				long bytes;
				std::tie(cursor, bytes) = tasks.pop();
				if (cursor==-1) break;
				memset(local_grid_offset, 0, sizeof(int) * partitions * partitions);
				memset(local_grid_cursor, 0, sizeof(int) * partitions * partitions);
				char * buffer = buffers[cursor];
				for (long pos=0;pos<bytes;pos+=edge_unit) {
					source = advanced ? mapper[*(VertexId*)(buffer+pos)] : *(VertexId*)(buffer+pos);
					target = advanced ? mapper[*(VertexId*)(buffer+pos+sizeof(VertexId))] : *(VertexId*)(buffer+pos+sizeof(VertexId));
					int i = get_partition_id(vertices, partitions, source);
					int j = get_partition_id(vertices, partitions, target);
					if(i <= j) ++local_propagation;
					if(!advanced) {
						write_add(&out_degrees[source], (uint32_t) 1);
						write_add(&in_degrees[target], (uint32_t) 1);
					}
					local_grid_offset[i*partitions+j] += edge_unit;
				}
				local_grid_cursor[0] = 0;
				for (int ij=1;ij<partitions*partitions;ij++) {
					local_grid_cursor[ij] = local_grid_offset[ij - 1];
					local_grid_offset[ij] += local_grid_cursor[ij];
				}
				assert(local_grid_offset[partitions*partitions-1]==bytes);
				for (long pos=0;pos<bytes;pos+=edge_unit) {
					source = advanced ? mapper[*(VertexId*)(buffer+pos)] : *(VertexId*)(buffer+pos);
					target = advanced ? mapper[*(VertexId*)(buffer+pos+sizeof(VertexId))] : *(VertexId*)(buffer+pos+sizeof(VertexId));
					int i = get_partition_id(vertices, partitions, source);
					int j = get_partition_id(vertices, partitions, target);
					*(VertexId*)(local_buffer+local_grid_cursor[i*partitions+j]) = source;
					*(VertexId*)(local_buffer+local_grid_cursor[i*partitions+j]+sizeof(VertexId)) = target;
					if (edge_type==1) {
						weight = *(Weight*)(buffer+pos+sizeof(VertexId)*2);
						*(Weight*)(local_buffer+local_grid_cursor[i*partitions+j]+sizeof(VertexId)*2) = weight;
					}
					local_grid_cursor[i*partitions+j] += edge_unit;
				}
				int start = 0;
				for (int ij=0;ij<partitions*partitions;ij++) {
					assert(local_grid_cursor[ij]==local_grid_offset[ij]);
					int i = ij / partitions;
					int j = ij % partitions;
					std::unique_lock<std::mutex> lock(mutexes[i][j]);
					if (local_grid_offset[ij] - start > edge_unit) {
						write(fout[i][j], local_buffer+start, local_grid_offset[ij]-start);
					} else if (local_grid_offset[ij] - start == edge_unit) {
						memcpy(grid_buffer[i][j]+grid_buffer_offset[i][j], local_buffer+start, edge_unit);
						grid_buffer_offset[i][j] += edge_unit;
						if (grid_buffer_offset[i][j]==grid_buffer_size) {
							write(fout[i][j], grid_buffer[i][j], grid_buffer_size);
							grid_buffer_offset[i][j] = 0;
						}
					}
					start = local_grid_offset[ij];
				}
				occupied[cursor] = false;
			}
			write_add(&propagation, local_propagation);
		});
	}

	int fin = open(input.c_str(), O_RDONLY);
	if (fin==-1) printf("%s\n", strerror(errno));
	assert(fin!=-1);
	int cursor = 0;
	long total_bytes = file_size(input);
	long read_bytes = 0;
	while (true) {
		long bytes = read(fin, buffers[cursor], IOSIZE);
		assert(bytes!=-1);
		if (bytes==0) break;
		occupied[cursor] = true;
		tasks.push(std::make_tuple(cursor, bytes));
		read_bytes += bytes;
		printf("progress: %.2f%%\r", 100. * read_bytes / total_bytes);
		fflush(stdout);
		while (occupied[cursor]) {
			cursor = (cursor + 1) % (parallelism * 2);
		}
	}
	close(fin);
	assert(read_bytes==edges*edge_unit);

	for (int ti=0;ti<parallelism;ti++) {
		tasks.push(std::make_tuple(-1, 0));
	}

	for (int ti=0;ti<parallelism;ti++) {
		threads[ti].join();
	}

	for (int i=0;i<partitions;i++) {
		for (int j=0;j<partitions;j++) {
			if (grid_buffer_offset[i][j]>0) {
				write(fout[i][j], grid_buffer[i][j], grid_buffer_offset[i][j]);
			}
		}
	}

	for (int i=0;i<partitions;i++) {
		for (int j=0;j<partitions;j++) {
			close(fout[i][j]);
		}
	}

	printf("edge blocks generated\n");

	long offset;
	int fout_column = open((output+"/column").c_str(), O_WRONLY|O_APPEND|O_CREAT, 0644);
	int fout_column_offset = open((output+"/column_offset").c_str(), O_WRONLY|O_APPEND|O_CREAT, 0644);
	offset = 0;
	for (int j=0;j<partitions;j++) {
		for (int i=0;i<partitions;i++) {
			printf("progress: %.2f%%\r", 100. * offset / total_bytes);
			fflush(stdout);
			write(fout_column_offset, &offset, sizeof(offset));
			char filename[4096];
			sprintf(filename, "%s/block-%d-%d", output.c_str(), i, j);
			offset += file_size(filename);
			fin = open(filename, O_RDONLY);
			while (true) {
				long bytes = read(fin, buffers[0], IOSIZE);
				assert(bytes!=-1);
				if (bytes==0) break;
				write(fout_column, buffers[0], bytes);
			}
			close(fin);
		}
	}
	write(fout_column_offset, &offset, sizeof(offset));
	close(fout_column_offset);
	close(fout_column);
	printf("column oriented grid generated\n");
	int fout_row = open((output+"/row").c_str(), O_WRONLY|O_APPEND|O_CREAT, 0644);
	int fout_row_offset = open((output+"/row_offset").c_str(), O_WRONLY|O_APPEND|O_CREAT, 0644);
	offset = 0;
	for (int i=0;i<partitions;i++) {
		for (int j=0;j<partitions;j++) {
			printf("progress: %.2f%%\r", 100. * offset / total_bytes);
			fflush(stdout);
			write(fout_row_offset, &offset, sizeof(offset));
			char filename[4096];
			sprintf(filename, "%s/block-%d-%d", output.c_str(), i, j);
			offset += file_size(filename);
			fin = open(filename, O_RDONLY);
			while (true) {
				long bytes = read(fin, buffers[0], IOSIZE);
				assert(bytes!=-1);
				if (bytes==0) break;
				write(fout_row, buffers[0], bytes);
			}
			close(fin);
		}
	}
	write(fout_row_offset, &offset, sizeof(offset));
	close(fout_row_offset);
	close(fout_row);
	printf("row oriented grid generated\n");

	if(!advanced) {
		int fout_in_deg = open((output+"/indegrees").c_str(), O_WRONLY|O_APPEND|O_CREAT, 0644);
		write(fout_in_deg, in_degrees, vertices * sizeof(uint32_t));
		close(fout_in_deg);

		int fout_out_deg = open((output+"/outdegrees").c_str(), O_WRONLY|O_APPEND|O_CREAT, 0644);
		write(fout_out_deg, out_degrees, vertices * sizeof(uint32_t));
		close(fout_out_deg);
	
		delete[] in_degrees;
		delete[] out_degrees;
	} else 
		delete[] mapper;

	printf("cross-iteration propagation enabled: %.2lf%%\n", (1.0 * propagation) / edges * 100.0);
	if(!advanced)
		printf("comment: choose advanced mode for higher cross-iteration propagation\n");

	FILE * fmeta = fopen((output+"/meta").c_str(), "w");
	fprintf(fmeta, "%d %d %ld %d %d", edge_type, vertices, edges, partitions, advanced ? 1 : 0);
	fclose(fmeta);
}

int main(int argc, char ** argv) {
	int opt;
	std::string input = "";
	std::string output = "";
	VertexId vertices = -1;
	int partitions = -1;
	int edge_type = 0;
	int mode = 0;
	while ((opt = getopt(argc, argv, "i:o:v:p:t:m:")) != -1) {
		switch (opt) {
		case 'i':
			input = optarg;
			break;
		case 'o':
			output = optarg;
			break;
		case 'v':
			vertices = atoi(optarg);
			break;
		case 'p':
			partitions = atoi(optarg);
			break;
		case 't':
			edge_type = atoi(optarg);
			break;
		case 'm':
			mode = atoi(optarg);
			break;
		}
	}
	if (input=="" || output=="" || vertices==-1 || ((mode !=0) && (mode != 1))) {
		fprintf(stderr, "usage: %s -i [input path] -o [output path] -v [vertices] -p [partitions] -t [edge type: 0=unweighted, 1=weighted] -m [mode: 0=advanced, 1=naive]\n", argv[0]);
		exit(-1);
	}
	if (partitions==-1) {
		partitions = vertices / CHUNKSIZE;
	}

	if(mode == 0) {
		printf("mode = advanced\n");
		renumber(input, output, vertices, edge_type);
	} else {
		printf("mode = naive\n");
		printf("comment: choose advanced mode for higher cross-iteration propagation\n");
	}

	generate_edge_grid(input, output, vertices, partitions, edge_type, mode == 0);
	return 0;
}
