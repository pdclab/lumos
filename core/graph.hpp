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

#ifndef GRAPH_H
#define GRAPH_H

#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <unistd.h>
#include <malloc.h>
#include <omp.h>
#include <string.h>

#include <thread>
#include <vector>

#include "core/constants.hpp"
#include "core/type.hpp"
#include "core/bitmap.hpp"
#include "core/atomic.hpp"
#include "core/queue.hpp"
#include "core/partition.hpp"
#include "core/bigvector.hpp"
#include "core/time.hpp"

bool f_true(VertexId v) {
	return true;
}

void f_none_1(std::pair<VertexId,VertexId> vid_range) {

}

void f_none_2(std::pair<VertexId,VertexId> source_vid_range, std::pair<VertexId,VertexId> target_vid_range) {

}

class Graph {
	int parallelism;
	int edge_unit;
	bool * should_access_shard;
	
	long ** fsize;
	char ** buffer_pool;
	char ** thread_buffer_pool;
	long * column_offset;
	long * row_offset;

	long memory_bytes;
	int partition_batch;
	long vertex_data_bytes;
	long PAGESIZE;
	long intra_buffer_size;
	EdgeId edges;

	bool advanced;
public:
	std::string path;

	int edge_type;
	VertexId vertices;
	int partitions;
	BigVector<uint32_t> indegrees, outdegrees;
	BigVector<uint32_t> otn, nto;

	Graph (std::string path, long mem_bytes) {
		PAGESIZE = 4096;
		parallelism = std::thread::hardware_concurrency();
		memory_bytes = mem_bytes;
		init(path);
		intra_buffer_size = (intra_buffer_size / PAGESIZE + 1) * PAGESIZE;
		if(intra_buffer_size * parallelism > 0.5 * memory_bytes) {  // back-of-the-envelope calculation
			parallelism = std::min((int)((0.5 * memory_bytes) / intra_buffer_size), 1);
			printf("Limited memory: will run with %d threads. Recreate grid with more partitions for parallel execution.\n", parallelism);
			assert(parallelism > 0);
		}

		buffer_pool = new char * [parallelism*1];
		for (int i=0;i<parallelism*1;i++) {
			buffer_pool[i] = (char *)memalign(PAGESIZE, IOSIZE);
			assert(buffer_pool[i]!=NULL);
			memset(buffer_pool[i], 0, IOSIZE);
		}
		
		thread_buffer_pool = new char * [parallelism*1];
		for (int i=0;i<parallelism*1;i++) {
			thread_buffer_pool[i] = (char *)memalign(PAGESIZE, intra_buffer_size);
			assert(thread_buffer_pool[i]!=NULL);
			memset(thread_buffer_pool[i], 0, intra_buffer_size);
		}
	}

	~Graph() {
		delete[] column_offset;
		delete[] row_offset;
		delete[] fsize;
		for (int i=0;i<parallelism*1;i++) {
			free(buffer_pool[i]);
			free(thread_buffer_pool[i]);
		}
		delete[] buffer_pool;
		delete[] thread_buffer_pool;
	}

	void set_vertex_data_bytes(long vertex_data_bytes) {
		this->vertex_data_bytes = vertex_data_bytes;
	}

	void init(std::string path) {
		this->path = path;

		int adv = -1;
		FILE * fin_meta = fopen((path+"/meta").c_str(), "r");
		fscanf(fin_meta, "%d %d %ld %d %d", &edge_type, &vertices, &edges, &partitions, &adv);
		fclose(fin_meta);

		assert(adv != -1);
		advanced = (adv == 1);

		if (edge_type==0) {
			PAGESIZE = 4096;
		} else {
			PAGESIZE = 12288;
		}

		should_access_shard = new bool[partitions];

		if (edge_type==0) {
			edge_unit = sizeof(VertexId) * 2;
		} else {
			edge_unit = sizeof(VertexId) * 2 + sizeof(Weight);
		}

		partition_batch = partitions;
		vertex_data_bytes = 0;

		long bytes;

		column_offset = new long [partitions*partitions+1];
		int fin_column_offset = open((path+"/column_offset").c_str(), O_RDONLY);
		bytes = read(fin_column_offset, column_offset, sizeof(long)*(partitions*partitions+1));
		assert(bytes==sizeof(long)*(partitions*partitions+1));
		close(fin_column_offset);

		row_offset = new long [partitions*partitions+1];
		int fin_row_offset = open((path+"/row_offset").c_str(), O_RDONLY);
		bytes = read(fin_row_offset, row_offset, sizeof(long)*(partitions*partitions+1));
		assert(bytes==sizeof(long)*(partitions*partitions+1));
		close(fin_row_offset);

		this->intra_buffer_size = 0;
		fsize = new long * [partitions];
		for (int i=0;i<partitions;i++) {
			fsize[i] = new long [partitions];
			for (int j=0;j<partitions;j++) {
				fsize[i][j] = column_offset[j*partitions+i+1] - column_offset[j*partitions+i];

				if(i == j && (fsize[i][j] > this->intra_buffer_size))
					this->intra_buffer_size = fsize[i][j];
			}
		}
	}

	Bitmap * alloc_bitmap() {
		return new Bitmap(vertices);
	}

	template <typename T>
	T stream_vertices(std::function<T(VertexId)> process, Bitmap * bitmap = nullptr, T zero = 0,
		std::function<void(std::pair<VertexId,VertexId>)> pre = f_none_1,
		std::function<void(std::pair<VertexId,VertexId>)> post = f_none_1) {
		T value = zero;
		if (bitmap==nullptr && vertex_data_bytes > (0.8 * memory_bytes - intra_buffer_size * parallelism)) {
			for (int cur_partition=0;cur_partition<partitions;cur_partition+=partition_batch) {
				VertexId begin_vid, end_vid;
				begin_vid = get_partition_range(vertices, partitions, cur_partition).first;
				if (cur_partition+partition_batch>=partitions) {
					end_vid = vertices;
				} else {
					end_vid = get_partition_range(vertices, partitions, cur_partition+partition_batch).first;
				}
				pre(std::make_pair(begin_vid, end_vid));
				#pragma omp parallel for schedule(dynamic) num_threads(parallelism)
				for (int partition_id=cur_partition;partition_id<cur_partition+partition_batch;partition_id++) {
					if (partition_id < partitions) {
						T local_value = zero;
						VertexId begin_vid, end_vid;
						std::tie(begin_vid, end_vid) = get_partition_range(vertices, partitions, partition_id);
						for (VertexId i=begin_vid;i<end_vid;i++) {
							local_value += process(i);
						}
						write_add(&value, local_value);
					}
				}
				#pragma omp barrier
				post(std::make_pair(begin_vid, end_vid));
			}
		} else {
			#pragma omp parallel for schedule(dynamic) num_threads(parallelism)
			for (int partition_id=0;partition_id<partitions;partition_id++) {
				T local_value = zero;
				VertexId begin_vid, end_vid;
				std::tie(begin_vid, end_vid) = get_partition_range(vertices, partitions, partition_id);
				if (bitmap==nullptr) {
					for (VertexId i=begin_vid;i<end_vid;i++) {
						local_value += process(i);
					}
				} else {
					VertexId i = begin_vid;
					while (i<end_vid) {
						unsigned long word = bitmap->data[WORD_OFFSET(i)];
						if (word==0) {
							i = (WORD_OFFSET(i) + 1) << 6;
							continue;
						}
						size_t j = BIT_OFFSET(i);
						word = word >> j;
						while (word!=0) {
							if (word & 1) {
								local_value += process(i);
							}
							i++;
							j++;
							word = word >> 1;
							if (i==end_vid) break;
						}
						i += (64 - j);
					}
				}
				write_add(&value, local_value);
			}
			#pragma omp barrier
		}
		return value;
	}

	void set_partition_batch(long bytes) {
		int x = (int)ceil(bytes / (0.8 * memory_bytes - intra_buffer_size * parallelism));
		partition_batch = partitions / x;
	}

	template <typename... Args>
	void hint(Args... args);

	template <typename A>
	void hint(BigVector<A> & a) {
		long bytes = sizeof(A) * a.length;
		set_partition_batch(bytes);
	}

	template <typename A, typename B>
	void hint(BigVector<A> & a, BigVector<B> & b) {
		long bytes = sizeof(A) * a.length + sizeof(B) * b.length;
		set_partition_batch(bytes);
	}

	template <typename A, typename B, typename C>
	void hint(BigVector<A> & a, BigVector<B> & b, BigVector<C> & c) {
		long bytes = sizeof(A) * a.length + sizeof(B) * b.length + sizeof(C) * c.length;
		set_partition_batch(bytes);
	}

	template <typename A, typename B, typename C, typename D>
	void hint(BigVector<A> & a, BigVector<B> & b, BigVector<C> & c, BigVector<D> & d) {
		long bytes = sizeof(A) * a.length + sizeof(B) * b.length + sizeof(C) * c.length + sizeof(D) * d.length;
		set_partition_batch(bytes);
	}

	template <typename T, typename E>
	T stream_edges(std::function<T(Edge<E>&)> process, Bitmap * bitmap = nullptr, T zero = 0, int update_mode = 1,
		std::function<void(std::pair<VertexId,VertexId> vid_range)> pre_source_window = f_none_1,
		std::function<void(std::pair<VertexId,VertexId> vid_range)> post_source_window = f_none_1,
		std::function<void(std::pair<VertexId,VertexId> vid_range)> pre_target_window = f_none_1,
		std::function<void(std::pair<VertexId,VertexId> vid_range)> post_target_window = f_none_1) {
		if (bitmap==nullptr) {
			for (int i=0;i<partitions;i++) {
				should_access_shard[i] = true;
			}
		} else {
			for (int i=0;i<partitions;i++) {
				should_access_shard[i] = false;
			}
			#pragma omp parallel for schedule(dynamic) num_threads(parallelism)
			for (int partition_id=0;partition_id<partitions;partition_id++) {
				VertexId begin_vid, end_vid;
				std::tie(begin_vid, end_vid) = get_partition_range(vertices, partitions, partition_id);
				VertexId i = begin_vid;
				while (i<end_vid) {
					unsigned long word = bitmap->data[WORD_OFFSET(i)];
					if (word!=0) {
						should_access_shard[partition_id] = true;
						break;
					}
					i = (WORD_OFFSET(i) + 1) << 6;
				}
			}
			#pragma omp barrier
		}

		T value = zero;
		Queue<std::tuple<int, long, long> > tasks(65536);
		std::vector<std::thread> threads;

		long total_bytes = 0;
		for (int i=0;i<partitions;i++) {
			if (!should_access_shard[i]) continue;
			for (int j=0;j<partitions;j++) {
				total_bytes += fsize[i][j];
			}
		}
		int read_mode;
		if (memory_bytes < total_bytes)
			read_mode = O_RDONLY | O_DIRECT;
		else
			read_mode = O_RDONLY;

		int fin;
		long offset = 0;
		switch(update_mode) {
		case 0: // source oriented update
			threads.clear();
			for (int ti=0;ti<parallelism;ti++) {
				threads.emplace_back([&](int thread_id){
					T local_value = zero;
					while (true) {
						int fin;
						long offset, length;
						std::tie(fin, offset, length) = tasks.pop();
						if (fin==-1) break;
						char * buffer = buffer_pool[thread_id];
						long bytes = pread(fin, buffer, length, offset);
						assert(bytes>0);
						// CHECK: start position should be offset % edge_unit
						for (long pos=offset % edge_unit;pos+edge_unit<=bytes;pos+=edge_unit) {
							Edge<E> & e = *(Edge<E>*)(buffer+pos);
							if (bitmap==nullptr || bitmap->get_bit(e.source)) {
								local_value += process(e);
							}
						}
					}
					write_add(&value, local_value);
				}, ti);
			}
			fin = open((path+"/row").c_str(), read_mode);
			posix_fadvise(fin, 0, 0, POSIX_FADV_SEQUENTIAL);
			for (int i=0;i<partitions;i++) {
				if (!should_access_shard[i]) continue;
				for (int j=0;j<partitions;j++) {
					long begin_offset = row_offset[i*partitions+j];
					if (begin_offset - offset >= PAGESIZE) {
						offset = begin_offset / PAGESIZE * PAGESIZE;
					}
					long end_offset = row_offset[i*partitions+j+1];
					if (end_offset <= offset) continue;
					while (end_offset - offset >= IOSIZE) {
						tasks.push(std::make_tuple(fin, offset, IOSIZE));
						offset += IOSIZE;
					}
					if (end_offset > offset) {
						tasks.push(std::make_tuple(fin, offset, (end_offset - offset + PAGESIZE - 1) / PAGESIZE * PAGESIZE));
						offset += (end_offset - offset + PAGESIZE - 1) / PAGESIZE * PAGESIZE;
					}
				}
			}
			for (int i=0;i<parallelism;i++) {
				tasks.push(std::make_tuple(-1, 0, 0));
			}
			for (int i=0;i<parallelism;i++) {
				threads[i].join();
			}
			break;
		case 1: // target oriented update
			fin = open((path+"/column").c_str(), read_mode);
			posix_fadvise(fin, 0, 0, POSIX_FADV_SEQUENTIAL);

			for (int cur_partition=0;cur_partition<partitions;cur_partition+=partition_batch) {
				VertexId begin_vid, end_vid;
				begin_vid = get_partition_range(vertices, partitions, cur_partition).first;
				if (cur_partition+partition_batch>=partitions) {
					end_vid = vertices;
				} else {
					end_vid = get_partition_range(vertices, partitions, cur_partition+partition_batch).first;
				}
				pre_source_window(std::make_pair(begin_vid, end_vid));
				threads.clear();
				for (int ti=0;ti<parallelism;ti++) {
					threads.emplace_back([&](int thread_id){
						T local_value = zero;
						while (true) {
							int fin;
							long offset, length;
							std::tie(fin, offset, length) = tasks.pop();
							if (fin==-1) break;
							char * buffer = buffer_pool[thread_id];
							long bytes = pread(fin, buffer, length, offset);
							assert(bytes>0);
							// CHECK: start position should be offset % edge_unit
							for (long pos=offset % edge_unit;pos+edge_unit<=bytes;pos+=edge_unit) {
								Edge<E> & e = *(Edge<E>*)(buffer+pos);
								if (e.source < begin_vid || e.source >= end_vid) {
									continue;
								}
								if (bitmap==nullptr || bitmap->get_bit(e.source)) {
									local_value += process(e);
								}
							}
						}
						write_add(&value, local_value);
					}, ti);
				}
				offset = 0;
				for (int j=0;j<partitions;j++) {
					for (int i=cur_partition;i<cur_partition+partition_batch;i++) {
						if (i>=partitions) break;
						if (!should_access_shard[i]) continue;
						long begin_offset = column_offset[j*partitions+i];
						if (begin_offset - offset >= PAGESIZE) {
							offset = begin_offset / PAGESIZE * PAGESIZE;
						}
						long end_offset = column_offset[j*partitions+i+1];
						if (end_offset <= offset) continue;
						while (end_offset - offset >= IOSIZE) {
							tasks.push(std::make_tuple(fin, offset, IOSIZE));
							offset += IOSIZE;
						}
						if (end_offset > offset) {
							tasks.push(std::make_tuple(fin, offset, (end_offset - offset + PAGESIZE - 1) / PAGESIZE * PAGESIZE));
							offset += (end_offset - offset + PAGESIZE - 1) / PAGESIZE * PAGESIZE;
						}
					}
				}
				for (int i=0;i<parallelism;i++) {
					tasks.push(std::make_tuple(-1, 0, 0));
				}
				for (int i=0;i<parallelism;i++) {
					threads[i].join();
				}
				post_source_window(std::make_pair(begin_vid, end_vid));
			}

			break;

			default:
			assert(false);
		}

		close(fin);
		return value;
	}

void secondary_activated(VertexId v) {
	int partition_id = get_partition_id(vertices, partitions, v);
	if(should_access_shard[partition_id] == false) 
		should_access_shard[partition_id] = true;
}

template <typename T, typename TS, typename VT, typename E>
	std::tuple<T,TS,VT> stream_primary_edges(std::function<T(Edge<E>&)> process, std::function<T(Edge<E>&)> propagate, 
		Bitmap * bitmap = nullptr, Bitmap * secondary_bitmap = nullptr, T zero = 0, TS secondary_zero = 0, int update_mode = 1,
		std::function<void(std::pair<VertexId,VertexId> vid_range)> pre_source_window = f_none_1,
		std::function<void(std::pair<VertexId,VertexId> vid_range)> post_source_window = f_none_1,
		std::function<void(std::pair<VertexId,VertexId> vid_range)> pre_target_window = f_none_1,
		std::function<void(std::pair<VertexId,VertexId> vid_range)> post_target_window = f_none_1,
		std::function<T(VertexId)> vprocess = nullptr, Bitmap * vbitmap = nullptr, VT vzero = 0,
		std::function<void(std::pair<VertexId,VertexId>)> vpre = f_none_1,
		std::function<void(std::pair<VertexId,VertexId>)> vpost = f_none_1) {
		if (bitmap==nullptr) {
			for (int i=0;i<partitions;i++) {
				should_access_shard[i] = true;
			}
		} else {
			for (int i=0;i<partitions;i++) {
				should_access_shard[i] = false;
			}
			#pragma omp parallel for schedule(dynamic) num_threads(parallelism)
			for (int partition_id=0;partition_id<partitions;partition_id++) {
				VertexId begin_vid, end_vid;
				std::tie(begin_vid, end_vid) = get_partition_range(vertices, partitions, partition_id);
				VertexId i = begin_vid;
				while (i<end_vid) {
					unsigned long word = bitmap->data[WORD_OFFSET(i)];
					unsigned long secondary_word = bitmap->data[WORD_OFFSET(i)];
					if (word!=0 || secondary_word!=0) {
						should_access_shard[partition_id] = true;
						break;
					}
					i = (WORD_OFFSET(i) + 1) << 6;
				}
			}
			#pragma omp barrier
		}

		T value = zero;
		TS secondary_value = secondary_zero;
		VT v_value = vzero;

		Queue<std::tuple<int, long, long, bool> > tasks(65536);

		std::vector<std::thread> threads;
		long read_bytes = 0;

		int fin;
		long offset = 0;
		VertexId begin_vid = 0, end_vid = vertices;
		int sw1 = 0, sw2 = 0;
		switch(update_mode) {
			case 0: // source oriented update
				printf("stream_primary_edges: source oriented update feature is pending\n");
				printf("stream_primary_edges: please use target oriented update (update_mode = 1)\n");
				return std::make_tuple(value, secondary_value, v_value);
			case 1: // target oriented update
			fin = open((path+"/column").c_str(), O_RDONLY);
			posix_fadvise(fin, 0, 0, POSIX_FADV_SEQUENTIAL);

			pre_source_window(std::make_pair(begin_vid, end_vid));
			threads.clear();
			for (int ti=0;ti<parallelism;ti++) {
				threads.emplace_back([&](int thread_id){
					T local_value = zero;
					TS local_secondary_value = secondary_zero;
					long local_read_bytes = 0;
					long thread_buffer_pool_offset = 0;
					char* buffer = NULL; long buffer_offset = 0;
					while (true) {
						int where;
						long offset, length;
						bool cross_propagate;
						std::tie(where, offset, length, cross_propagate) = tasks.pop();
						if (where == -2) {
							write_add(&sw1, 1);
							while(sw1 != 0) { __asm volatile ("pause" ::: "memory"); };
							write_add(&sw2, 1);
							char* buffer = thread_buffer_pool[thread_id];
							for (long pos=0; pos+edge_unit<=thread_buffer_pool_offset; pos+=edge_unit) {
								Edge<E> & e = *(Edge<E>*)(buffer+pos);
								if (e.source < begin_vid || e.source >= end_vid) {
									continue;
								}
								if (secondary_bitmap==nullptr || secondary_bitmap->get_bit(e.source)) {
									local_secondary_value += propagate(e);
								}
							}
							thread_buffer_pool_offset = 0;
							continue;
						}
						else if (where==-1) break;
						else if(where == 0) {
							buffer = buffer_pool[thread_id];
							buffer_offset = 0;
						} else if (where == 1) {
							buffer = thread_buffer_pool[thread_id];
							buffer_offset = thread_buffer_pool_offset;
						} else { assert(false); }
						long bytes = pread(fin, buffer + buffer_offset, length, offset);
						assert(bytes>0);
						local_read_bytes += bytes;
						for (long pos=offset % edge_unit;pos+edge_unit<=bytes;pos+=edge_unit) {
							Edge<E> & e = *(Edge<E>*)(buffer+buffer_offset+pos);
							if (e.source < begin_vid || e.source >= end_vid) {
								continue;
							}
							if (bitmap==nullptr || bitmap->get_bit(e.source)) {
								local_value += process(e);
							}
							if(cross_propagate && (secondary_bitmap==nullptr || secondary_bitmap->get_bit(e.source))) {
								local_secondary_value += propagate(e); 
							}
						}
						if(where == 1)
							thread_buffer_pool_offset += bytes;
					}
					write_add(&value, local_value);
					write_add(&secondary_value, local_secondary_value);
					write_add(&read_bytes, local_read_bytes);
				}, ti);
			}
			for (int j=0;j<partitions;j++) {
				long column_end_offset = column_offset[j*partitions+partitions];
				bool first_non_empty_shard = true;
				for (int i=0;i<partitions;i++) {
					if (!should_access_shard[i]) {
						continue;
					}
					bool cross_propagate = (i < j);

					long begin_offset = column_offset[j*partitions+i];
					if(first_non_empty_shard && (column_offset[j*partitions+i] - column_offset[j*partitions+i + 1] > 0)) {
						offset = begin_offset;
						first_non_empty_shard = false;
					} else if((!should_access_shard[i-1]) || (i-1 == j) || (i == j))
						offset = begin_offset;
					else
						offset = (begin_offset + PAGESIZE - 1) / PAGESIZE * PAGESIZE;
					
					long end_offset = column_offset[j*partitions+i+1];
					if (end_offset <= offset) continue;
					if(!((i == partitions - 1) || (!should_access_shard[i+1]) || (i+1 == j) || (i == j))) {
						end_offset = std::min((end_offset + PAGESIZE - 1) / PAGESIZE * PAGESIZE, column_end_offset);
					}

					int where = (i == j) ? 1 : 0;
					{
						long chunk_end = (offset / PAGESIZE + 1) * PAGESIZE;
						long slot_size = std::min(end_offset - offset, chunk_end - offset);
						tasks.push(std::make_tuple(where, offset, slot_size, cross_propagate));
						offset += slot_size;
					}

					while(end_offset - offset >= PAGESIZE) {
						tasks.push(std::make_tuple(where, offset, PAGESIZE, cross_propagate));
						offset += PAGESIZE;
					}

					if(end_offset > offset) {
						tasks.push(std::make_tuple(where, offset, end_offset - offset, cross_propagate));
						offset += end_offset;
					}
				}

				sw1 = 0;
				for (int x=0;x<parallelism;x++) {
					tasks.push(std::make_tuple(-2, 0, 0, false));
				}
				while(sw1 != parallelism) { __asm volatile ("pause" ::: "memory"); }
				v_value += my_stream_vertices<VT>(vprocess, vbitmap, vzero, vpre, vpost, j, j+1);

				sw1 = 0;  
				while(sw2 != parallelism) { __asm volatile ("pause" ::: "memory"); }
				sw2 = 0;
			}
			for (int i=0;i<parallelism;i++) {
				tasks.push(std::make_tuple(-1, 0, 0, false));
			}
			for (int i=0;i<parallelism;i++) {
				threads[i].join();
			}
			post_source_window(std::make_pair(begin_vid, end_vid));
			break;
	 
		default:
			assert(false);
	}

	close(fin);
	return std::make_tuple(value, secondary_value, v_value);
}


template <typename T, typename E>
	T stream_secondary_edges(std::function<T(Edge<E>&)> process, 
		Bitmap * bitmap = nullptr, T zero = 0, int update_mode = 1,
		std::function<void(std::pair<VertexId,VertexId> vid_range)> pre_source_window = f_none_1,
		std::function<void(std::pair<VertexId,VertexId> vid_range)> post_source_window = f_none_1,
		std::function<void(std::pair<VertexId,VertexId> vid_range)> pre_target_window = f_none_1,
		std::function<void(std::pair<VertexId,VertexId> vid_range)> post_target_window = f_none_1) {
		if (bitmap==nullptr) {
			for (int i=0;i<partitions;i++) {
				should_access_shard[i] = true;
			}
		} else {
			for (int i=0;i<partitions;i++) {
				should_access_shard[i] = false;
			}
			#pragma omp parallel for schedule(dynamic) num_threads(parallelism)
			for (int partition_id=0;partition_id<partitions;partition_id++) {
				VertexId begin_vid, end_vid;
				std::tie(begin_vid, end_vid) = get_partition_range(vertices, partitions, partition_id);
				VertexId i = begin_vid;
				while (i<end_vid) {
					unsigned long word = bitmap->data[WORD_OFFSET(i)];
					if (word!=0) {
						should_access_shard[partition_id] = true;
						break;
					}
					i = (WORD_OFFSET(i) + 1) << 6;
				}
			}
			#pragma omp barrier
		}

		T value = zero;
		Queue<std::tuple<int, long, long> > tasks(65536);

		std::vector<std::thread> threads;
		long read_bytes = 0;

		int fin;
		long offset = 0; 
		switch(update_mode) {
			case 0: // source oriented update
				printf("stream_secondary_edges: source oriented update feature is pending\n");
				printf("stream_secondary_edges: please use target oriented update (update_mode = 1)\n");
				return value; 
			case 1: // target oriented update
			fin = open((path+"/column").c_str(), O_RDONLY);
			posix_fadvise(fin, 0, 0, POSIX_FADV_SEQUENTIAL);

			for (int cur_partition=0;cur_partition<partitions;cur_partition+=partition_batch) {
				VertexId begin_vid, end_vid;
				begin_vid = get_partition_range(vertices, partitions, cur_partition).first;
				if (cur_partition+partition_batch>=partitions) {
					end_vid = vertices;
				} else {
					end_vid = get_partition_range(vertices, partitions, cur_partition+partition_batch).first;
				}
				pre_source_window(std::make_pair(begin_vid, end_vid));
				threads.clear();
				for (int ti=0;ti<parallelism;ti++) {
					threads.emplace_back([&](int thread_id){
						T local_value = zero;
						long local_read_bytes = 0;
						char * buffer = buffer_pool[thread_id];
						while (true) {
							int where;
							long offset, length;
							std::tie(where, offset, length) = tasks.pop();
							if (where==-1) break;
							long bytes = pread(fin, buffer, length, offset);
							assert(bytes>0);
							local_read_bytes += bytes;
							for (long pos=offset % edge_unit;pos+edge_unit<=bytes;pos+=edge_unit) {
								Edge<E> & e = *(Edge<E>*)(buffer+pos);
								if (e.source < begin_vid || e.source >= end_vid) {
									continue;
								}
								if (bitmap==nullptr || bitmap->get_bit(e.source)) {
									local_value += process(e);
								}
							}
						}
						write_add(&value, local_value);
						write_add(&read_bytes, local_read_bytes);
					}, ti);
				}
				offset = 0;
				for (int j=0;j<partitions;j++) {
					long column_end_offset = column_offset[j*partitions+partitions];
					bool first_non_empty_shard = true;
					
					for (int i=j+1;i<partitions;i++) {
						if (!should_access_shard[i]) continue;
						long begin_offset = column_offset[j*partitions+i];
						if(first_non_empty_shard && (column_offset[j*partitions+i] - column_offset[j*partitions+i + 1] > 0)) {
							offset = begin_offset;
							first_non_empty_shard = false;
						} else if((!should_access_shard[i-1]) || (i-1 == j) || (i == j))
							offset = begin_offset;
						else
							offset = (begin_offset + PAGESIZE - 1) / PAGESIZE * PAGESIZE;
						
						long end_offset = column_offset[j*partitions+i+1];
						if (end_offset <= offset) continue;
						if(!((i == partitions - 1) || (!should_access_shard[i+1]) || (i+1 == j) || (i == j))) {
							end_offset = std::min((end_offset + PAGESIZE - 1) / PAGESIZE * PAGESIZE, column_end_offset);
						}

						{
							long chunk_end = (offset / PAGESIZE + 1) * PAGESIZE;
							long slot_size = std::min(end_offset - offset, chunk_end - offset);
							tasks.push(std::make_tuple(0, offset, slot_size));
							offset += slot_size;
						}

						while(end_offset - offset >= PAGESIZE) {
							tasks.push(std::make_tuple(0, offset, PAGESIZE));
							offset += PAGESIZE;
						}

						if(end_offset > offset) {
							tasks.push(std::make_tuple(0, offset, end_offset - offset));
							offset += end_offset;
						}
					}
				}
				for (int i=0;i<parallelism;i++) {
					tasks.push(std::make_tuple(-1, 0, 0));
				}
				for (int i=0;i<parallelism;i++) {
					threads[i].join();
				}
				post_source_window(std::make_pair(begin_vid, end_vid));
			}

			break;

		default:
			assert(false);
		}

		close(fin);
		return value;
	}

	template <typename T>
	T my_stream_vertices(std::function<T(VertexId)> process, Bitmap * bitmap = nullptr, T zero = 0,
		std::function<void(std::pair<VertexId,VertexId>)> pre = f_none_1,
		std::function<void(std::pair<VertexId,VertexId>)> post = f_none_1,
		int begin_partition = 0, int end_partition = 0) {
		T value = zero;
		if (bitmap==nullptr && vertex_data_bytes > (0.8 * memory_bytes - intra_buffer_size * parallelism)) {
			for (int cur_partition=begin_partition;cur_partition<end_partition;cur_partition+=partition_batch) {
				int micro_partition_batch = std::min(partition_batch, end_partition - cur_partition); 
				VertexId begin_vid, end_vid;
				begin_vid = get_partition_range(vertices, partitions, cur_partition).first;
				if (cur_partition+micro_partition_batch>=partitions) {
					end_vid = vertices;
				} else {
					end_vid = get_partition_range(vertices, partitions, cur_partition+micro_partition_batch).first;
				}
				pre(std::make_pair(begin_vid, end_vid));
				#pragma omp parallel for schedule(dynamic) num_threads(parallelism)
				for (int partition_id=cur_partition;partition_id<cur_partition+micro_partition_batch;partition_id++) {
					if (partition_id < partitions) {
						T local_value = zero;
						VertexId begin_vid, end_vid;
						std::tie(begin_vid, end_vid) = get_partition_range(vertices, partitions, partition_id);
						for (VertexId i=begin_vid;i<end_vid;i++) {
							local_value += process(i);
						}
						write_add(&value, local_value);
					}
				}
				#pragma omp barrier
				post(std::make_pair(begin_vid, end_vid));
			}
		} else {
			#pragma omp parallel for schedule(dynamic) num_threads(parallelism)
			for (int partition_id=begin_partition;partition_id<end_partition;partition_id++) {
				T local_value = zero;
				VertexId begin_vid, end_vid;
				std::tie(begin_vid, end_vid) = get_partition_range(vertices, partitions, partition_id);
				if (bitmap==nullptr) {
					for (VertexId i=begin_vid;i<end_vid;i++) {
						local_value += process(i);
					}
				} else {
					VertexId i = begin_vid;
					while (i<end_vid) {
						unsigned long word = bitmap->data[WORD_OFFSET(i)];
						if (word==0) {
							i = (WORD_OFFSET(i) + 1) << 6;
							continue;
						}
						size_t j = BIT_OFFSET(i);
						word = word >> j;
						while (word!=0) {
							if (word & 1) {
								local_value += process(i);
							}
							i++;
							j++;
							word = word >> 1;
							if (i==end_vid) break;
						}
						i += (64 - j);
					}
				}
				write_add(&value, local_value);
			}
			#pragma omp barrier
		}
		return value;
	}

	void load_indegrees() {
		indegrees.initiate(path+"/indegrees", vertices);
	}

	void load_outdegrees() {
		outdegrees.initiate(path+"/outdegrees", vertices);
	}

	void load_mapping_to_new() {
		otn.initiate(path+"/otn", vertices);
	}

	void load_mapping_to_old() {
		nto.initiate(path+"/nto", vertices);
	}

	void unload_indegrees() {
		indegrees.uninitiate();
	}

	void unload_outdegrees() {
		outdegrees.uninitiate();
	}

	void unload_mapping_to_new() {
		otn.uninitiate();
	}

	void unload_mapping_to_old() {
		nto.uninitiate();
	}

	bool is_advanced() {
		return advanced;
	}
};

#endif
