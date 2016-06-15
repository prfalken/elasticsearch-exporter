#!/usr/bin/env python
import string
import optparse
import socket
import sys
import simplejson
import requests
from prometheus_client import start_http_server, Gauge
import logging
import time

class ElasticsearchServer():
    NODE_STATS_URL = '/_nodes/_local/stats/'
    CLUSTER_HEALTH_URL = '/_cluster/health/'
    MASTER_STATUS_URL = '/_cat/master/'
    ES_CLUSTER_MAPPING={
      'green': 0,
      'yellow': 1,
      'red': 2,
      False: 0,
      True: 1
    }

    ES_IS_MASTER = {
        'elasticsearch_cluster_is_master': Gauge('elasticsearch_cluster_is_master', '', ['cluster', 'node'])
    }

    ES_CLUSTER_HEALTH_10 = {
        'active_primary_shards': Gauge('elasticsearch_cluster_health_active_primary_shards', '', ['cluster']),
        'active_shards': Gauge('elasticsearch_cluster_health_active_shards', '', ['cluster']),
        'initializing_shards': Gauge('elasticsearch_cluster_health_initializing_shards', '', ['cluster']),
        'number_of_data_nodes': Gauge('elasticsearch_cluster_health_number_of_data_nodes', '', ['cluster']),
        'number_of_nodes': Gauge('elasticsearch_cluster_health_number_of_nodes', '', ['cluster']),
        'relocating_shards': Gauge('elasticsearch_cluster_health_relocating_shards', '', ['cluster']),
        'status': Gauge('elasticsearch_cluster_health_status', '', ['cluster']),
        'timed_out': Gauge('elasticsearch_cluster_health_timed_out', '', ['cluster']),
        'unassigned_shards': Gauge('elasticsearch_cluster_health_unassigned_shards', '', ['cluster']),

    }
    ES_CLUSTER_HEALTH_16 = {
        'number_of_in_flight_fetch': Gauge('elasticsearch_cluster_health_number_of_in_flight_fetch', '', ['cluster']),
        'number_of_pending_tasks': Gauge('elasticsearch_cluster_health_number_of_pending_tasks', '', ['cluster']),
    }
    ES_CLUSTER_HEALTH_17 = {
        'delayed_unassigned_shards': Gauge('elasticsearch_cluster_health_delayed_unassigned_shards', '', ['cluster']),
    }
    ES_NODES_STATS_10 = {
        'indices.docs.count': Gauge('elasticsearch_indices_docs_count', '', ['cluster', 'node']),
        'indices.docs.deleted': Gauge('elasticsearch_indices_docs_deleted', '', ['cluster', 'node']),
        'indices.fielddata.evictions': Gauge('elasticsearch_indices_fielddata_evictions', '', ['cluster', 'node']),
        'indices.fielddata.memory_size_in_bytes': Gauge('elasticsearch_indices_fielddata_memory_size_in_bytes', '', ['cluster', 'node']),
        'indices.filter_cache.evictions': Gauge('elasticsearch_indices_filter_cache_evictions', '', ['cluster', 'node']),
        'indices.filter_cache.memory_size_in_bytes': Gauge('elasticsearch_indices_filter_cache_memory_size_in_bytes', '', ['cluster', 'node']),
        'indices.flush.total': Gauge('elasticsearch_indices_flush_total', '', ['cluster', 'node']),
        'indices.flush.total_time_in_millis': Gauge('elasticsearch_indices_flush_total_time_in_millis', '', ['cluster', 'node']),
        'indices.get.current': Gauge('elasticsearch_indices_get_current', '', ['cluster', 'node']),
        'indices.get.exists_time_in_millis': Gauge('elasticsearch_indices_get_exists_time_in_millis', '', ['cluster', 'node']),
        'indices.get.exists_total': Gauge('elasticsearch_indices_get_exists_total', '', ['cluster', 'node']),
        'indices.get.missing_time_in_millis': Gauge('elasticsearch_indices_get_missing_time_in_millis', '', ['cluster', 'node']),
        'indices.get.missing_total': Gauge('elasticsearch_indices_get_missing_total', '', ['cluster', 'node']),
        'indices.get.time_in_millis': Gauge('elasticsearch_indices_get_time_in_millis', '', ['cluster', 'node']),
        'indices.get.total': Gauge('elasticsearch_indices_get_total', '', ['cluster', 'node']),
        'indices.indexing.delete_current': Gauge('elasticsearch_indices_indexing_delete_current', '', ['cluster', 'node']),
        'indices.indexing.delete_time_in_millis': Gauge('elasticsearch_indices_indexing_delete_time_in_millis', '', ['cluster', 'node']),
        'indices.indexing.delete_total': Gauge('elasticsearch_indices_indexing_delete_total', '', ['cluster', 'node']),
        'indices.indexing.index_current': Gauge('elasticsearch_indices_indexing_index_current', '', ['cluster', 'node']),
        'indices.indexing.index_time_in_millis': Gauge('elasticsearch_indices_indexing_index_time_in_millis', '', ['cluster', 'node']),
        'indices.indexing.index_total': Gauge('elasticsearch_indices_indexing_index_total', '', ['cluster', 'node']),
        'indices.merges.current': Gauge('elasticsearch_indices_merges_current', '', ['cluster', 'node']),
        'indices.merges.current_docs': Gauge('elasticsearch_indices_merges_current_docs', '', ['cluster', 'node']),
        'indices.merges.current_size_in_bytes': Gauge('elasticsearch_indices_merges_current_size_in_bytes', '', ['cluster', 'node']),
        'indices.merges.total': Gauge('elasticsearch_indices_merges_total', '', ['cluster', 'node']),
        'indices.merges.total_docs': Gauge('elasticsearch_indices_merges_total_docs', '', ['cluster', 'node']),
        'indices.merges.total_size_in_bytes': Gauge('elasticsearch_indices_merges_total_size_in_bytes', '', ['cluster', 'node']),
        'indices.merges.total_time_in_millis': Gauge('elasticsearch_indices_merges_total_time_in_millis', '', ['cluster', 'node']),
        'indices.percolate.current': Gauge('elasticsearch_indices_percolate_current', '', ['cluster', 'node']),
        'indices.percolate.memory_size': Gauge('elasticsearch_indices_percolate_memory_size', '', ['cluster', 'node']),
        'indices.percolate.memory_size_in_bytes': Gauge('elasticsearch_indices_percolate_memory_size_in_bytes', '', ['cluster', 'node']),
        'indices.percolate.queries': Gauge('elasticsearch_indices_percolate_queries', '', ['cluster', 'node']),
        'indices.percolate.time_in_millis': Gauge('elasticsearch_indices_percolate_time_in_millis', '', ['cluster', 'node']),
        'indices.percolate.total': Gauge('elasticsearch_indices_percolate_total', '', ['cluster', 'node']),
        'indices.refresh.total': Gauge('elasticsearch_indices_refresh_total', '', ['cluster', 'node']),
        'indices.refresh.total_time_in_millis': Gauge('elasticsearch_indices_refresh_total_time_in_millis', '', ['cluster', 'node']),
        'indices.search.fetch_current': Gauge('elasticsearch_indices_search_fetch_current', '', ['cluster', 'node']),
        'indices.search.fetch_time_in_millis': Gauge('elasticsearch_indices_search_fetch_time_in_millis', '', ['cluster', 'node']),
        'indices.search.fetch_total': Gauge('elasticsearch_indices_search_fetch_total', '', ['cluster', 'node']),
        'indices.search.open_contexts': Gauge('elasticsearch_indices_search_open_contexts', '', ['cluster', 'node']),
        'indices.search.query_current': Gauge('elasticsearch_indices_search_query_current', '', ['cluster', 'node']),
        'indices.search.query_time_in_millis': Gauge('elasticsearch_indices_search_query_time_in_millis', '', ['cluster', 'node']),
        'indices.search.query_total': Gauge('elasticsearch_indices_search_query_total', '', ['cluster', 'node']),
        'indices.segments.count': Gauge('elasticsearch_indices_segments_count', '', ['cluster', 'node']),
        'indices.segments.memory_in_bytes': Gauge('elasticsearch_indices_segments_memory_in_bytes', '', ['cluster', 'node']),
        'indices.store.size_in_bytes': Gauge('elasticsearch_indices_store_size_in_bytes', '', ['cluster', 'node']),
        'indices.store.throttle_time_in_millis': Gauge('elasticsearch_indices_store_throttle_time_in_millis', '', ['cluster', 'node']),
        'indices.translog.operations': Gauge('elasticsearch_indices_translog_operations', '', ['cluster', 'node']),
        'indices.translog.size_in_bytes': Gauge('elasticsearch_indices_translog_size_in_bytes', '', ['cluster', 'node']),
        'indices.warmer.current': Gauge('elasticsearch_indices_warmer_current', '', ['cluster', 'node']),
        'indices.warmer.total': Gauge('elasticsearch_indices_warmer_total', '', ['cluster', 'node']),
        'indices.warmer.total_time_in_millis': Gauge('elasticsearch_indices_warmer_total_time_in_millis', '', ['cluster', 'node']),
        'jvm.mem.heap_used_in_bytes': Gauge('elasticsearch_jvm_mem_heap_used_in_bytes', '', ['cluster', 'node']),
        'jvm.mem.heap_used_percent': Gauge('elasticsearch_jvm_mem_heap_used_percent', '', ['cluster', 'node']),
        'jvm.mem.heap_committed_in_bytes': Gauge('elasticsearch_jvm_mem_heap_committed_in_bytes', '', ['cluster', 'node']),
        'jvm.mem.heap_max_in_bytes': Gauge('elasticsearch_jvm_mem_heap_max_in_bytes', '', ['cluster', 'node']),
        'jvm.mem.non_heap_used_in_bytes': Gauge('elasticsearch_jvm_mem_non_heap_used_in_bytes', '', ['cluster', 'node']),
        'jvm.mem.non_heap_committed_in_bytes': Gauge('elasticsearch_jvm_mem_non_heap_committed_in_bytes', '', ['cluster', 'node']),
        'jvm.mem.pools.young.used_in_bytes': Gauge('elasticsearch_jvm_mem_pools_young_used_in_bytes', '', ['cluster', 'node']),
        'jvm.mem.pools.young.max_in_bytes': Gauge('elasticsearch_jvm_mem_pools_young_max_in_bytes', '', ['cluster', 'node']),
        'jvm.mem.pools.young.peak_used_in_bytes': Gauge('elasticsearch_jvm_mem_pools_young_peak_used_in_bytes', '', ['cluster', 'node']),
        'jvm.mem.pools.young.peak_max_in_bytes': Gauge('elasticsearch_jvm_mem_pools_young_peak_max_in_bytes', '', ['cluster', 'node']),
        'jvm.mem.pools.survivor.used_in_bytes': Gauge('elasticsearch_jvm_mem_pools_survivor_used_in_bytes', '', ['cluster', 'node']),
        'jvm.mem.pools.survivor.max_in_bytes': Gauge('elasticsearch_jvm_mem_pools_survivor_max_in_bytes', '', ['cluster', 'node']),
        'jvm.mem.pools.survivor.peak_used_in_bytes': Gauge('elasticsearch_jvm_mem_pools_survivor_peak_used_in_bytes', '', ['cluster', 'node']),
        'jvm.mem.pools.survivor.peak_max_in_bytes': Gauge('elasticsearch_jvm_mem_pools_survivor_peak_max_in_bytes', '', ['cluster', 'node']),
        'jvm.mem.pools.old.used_in_bytes': Gauge('elasticsearch_jvm_mem_pools_old_used_in_bytes', '', ['cluster', 'node']),
        'jvm.mem.pools.old.max_in_bytes': Gauge('elasticsearch_jvm_mem_pools_old_max_in_bytes', '', ['cluster', 'node']),
        'jvm.mem.pools.old.peak_used_in_bytes': Gauge('elasticsearch_jvm_mem_pools_old_peak_used_in_bytes', '', ['cluster', 'node']),
        'jvm.mem.pools.old.peak_max_in_bytes': Gauge('elasticsearch_jvm_mem_pools_old_peak_max_in_bytes', '', ['cluster', 'node']),
        'jvm.threads.count': Gauge('elasticsearch_jvm_threads_count', '', ['cluster', 'node']),
        'jvm.threads.peak_count': Gauge('elasticsearch_jvm_threads_peak_count', '', ['cluster', 'node']),
        'jvm.gc.collectors.young.collection_count': Gauge('elasticsearch_jvm_gc_collectors_young_collection_count', '', ['cluster', 'node']),
        'jvm.gc.collectors.young.collection_time_in_millis': Gauge('elasticsearch_jvm_gc_collectors_young_collection_time_in_millis', '', ['cluster', 'node']),
        'jvm.gc.collectors.old.collection_count': Gauge('elasticsearch_jvm_gc_collectors_old_collection_count', '', ['cluster', 'node']),
        'jvm.gc.collectors.old.collection_time_in_millis': Gauge('elasticsearch_jvm_gc_collectors_old_collection_time_in_millis', '', ['cluster', 'node']),
        'jvm.buffer_pools.direct.count': Gauge('elasticsearch_jvm_buffer_pools_direct_count', '', ['cluster', 'node']),
        'jvm.buffer_pools.direct.used_in_bytes': Gauge('elasticsearch_jvm_buffer_pools_direct_used_in_bytes', '', ['cluster', 'node']),
        'jvm.buffer_pools.direct.total_capacity_in_bytes': Gauge('elasticsearch_jvm_buffer_pools_direct_total_capacity_in_bytes', '', ['cluster', 'node']),
        'jvm.buffer_pools.mapped.count': Gauge('elasticsearch_jvm_buffer_pools_mapped_count', '', ['cluster', 'node']),
        'jvm.buffer_pools.mapped.used_in_bytes': Gauge('elasticsearch_jvm_buffer_pools_mapped_used_in_bytes', '', ['cluster', 'node']),
        'jvm.buffer_pools.mapped.total_capacity_in_bytes': Gauge('elasticsearch_jvm_buffer_pools_mapped_total_capacity_in_bytes', '', ['cluster', 'node']),
        'thread_pool.bulk.active': Gauge('elasticsearch_thread_pool_bulk_active', '', ['cluster', 'node']),
        'thread_pool.bulk.completed': Gauge('elasticsearch_thread_pool_bulk_completed', '', ['cluster', 'node']),
        'thread_pool.bulk.largest': Gauge('elasticsearch_thread_pool_bulk_largest', '', ['cluster', 'node']),
        'thread_pool.bulk.queue': Gauge('elasticsearch_thread_pool_bulk_queue', '', ['cluster', 'node']),
        'thread_pool.bulk.rejected': Gauge('elasticsearch_thread_pool_bulk_rejected', '', ['cluster', 'node']),
        'thread_pool.bulk.threads': Gauge('elasticsearch_thread_pool_bulk_threads', '', ['cluster', 'node']),
        'thread_pool.flush.active': Gauge('elasticsearch_thread_pool_flush_active', '', ['cluster', 'node']),
        'thread_pool.flush.completed': Gauge('elasticsearch_thread_pool_flush_completed', '', ['cluster', 'node']),
        'thread_pool.flush.largest': Gauge('elasticsearch_thread_pool_flush_largest', '', ['cluster', 'node']),
        'thread_pool.flush.queue': Gauge('elasticsearch_thread_pool_flush_queue', '', ['cluster', 'node']),
        'thread_pool.flush.rejected': Gauge('elasticsearch_thread_pool_flush_rejected', '', ['cluster', 'node']),
        'thread_pool.flush.threads': Gauge('elasticsearch_thread_pool_flush_threads', '', ['cluster', 'node']),
        'thread_pool.generic.active': Gauge('elasticsearch_thread_pool_generic_active', '', ['cluster', 'node']),
        'thread_pool.generic.completed': Gauge('elasticsearch_thread_pool_generic_completed', '', ['cluster', 'node']),
        'thread_pool.generic.largest': Gauge('elasticsearch_thread_pool_generic_largest', '', ['cluster', 'node']),
        'thread_pool.generic.queue': Gauge('elasticsearch_thread_pool_generic_queue', '', ['cluster', 'node']),
        'thread_pool.generic.rejected': Gauge('elasticsearch_thread_pool_generic_rejected', '', ['cluster', 'node']),
        'thread_pool.generic.threads': Gauge('elasticsearch_thread_pool_generic_threads', '', ['cluster', 'node']),
        'thread_pool.get.active': Gauge('elasticsearch_thread_pool_get_active', '', ['cluster', 'node']),
        'thread_pool.get.completed': Gauge('elasticsearch_thread_pool_get_completed', '', ['cluster', 'node']),
        'thread_pool.get.largest': Gauge('elasticsearch_thread_pool_get_largest', '', ['cluster', 'node']),
        'thread_pool.get.queue': Gauge('elasticsearch_thread_pool_get_queue', '', ['cluster', 'node']),
        'thread_pool.get.rejected': Gauge('elasticsearch_thread_pool_get_rejected', '', ['cluster', 'node']),
        'thread_pool.get.threads': Gauge('elasticsearch_thread_pool_get_threads', '', ['cluster', 'node']),
        'thread_pool.index.active': Gauge('elasticsearch_thread_pool_index_active', '', ['cluster', 'node']),
        'thread_pool.index.completed': Gauge('elasticsearch_thread_pool_index_completed', '', ['cluster', 'node']),
        'thread_pool.index.largest': Gauge('elasticsearch_thread_pool_index_largest', '', ['cluster', 'node']),
        'thread_pool.index.queue': Gauge('elasticsearch_thread_pool_index_queue', '', ['cluster', 'node']),
        'thread_pool.index.rejected': Gauge('elasticsearch_thread_pool_index_rejected', '', ['cluster', 'node']),
        'thread_pool.index.threads': Gauge('elasticsearch_thread_pool_index_threads', '', ['cluster', 'node']),
        'thread_pool.management.active': Gauge('elasticsearch_thread_pool_management_active', '', ['cluster', 'node']),
        'thread_pool.management.completed': Gauge('elasticsearch_thread_pool_management_completed', '', ['cluster', 'node']),
        'thread_pool.management.largest': Gauge('elasticsearch_thread_pool_management_largest', '', ['cluster', 'node']),
        'thread_pool.management.queue': Gauge('elasticsearch_thread_pool_management_queue', '', ['cluster', 'node']),
        'thread_pool.management.rejected': Gauge('elasticsearch_thread_pool_management_rejected', '', ['cluster', 'node']),
        'thread_pool.management.threads': Gauge('elasticsearch_thread_pool_management_threads', '', ['cluster', 'node']),
        'thread_pool.merge.active': Gauge('elasticsearch_thread_pool_merge_active', '', ['cluster', 'node']),
        'thread_pool.merge.completed': Gauge('elasticsearch_thread_pool_merge_completed', '', ['cluster', 'node']),
        'thread_pool.merge.largest': Gauge('elasticsearch_thread_pool_merge_largest', '', ['cluster', 'node']),
        'thread_pool.merge.queue': Gauge('elasticsearch_thread_pool_merge_queue', '', ['cluster', 'node']),
        'thread_pool.merge.rejected': Gauge('elasticsearch_thread_pool_merge_rejected', '', ['cluster', 'node']),
        'thread_pool.merge.threads': Gauge('elasticsearch_thread_pool_merge_threads', '', ['cluster', 'node']),
        'thread_pool.optimize.active': Gauge('elasticsearch_thread_pool_optimize_active', '', ['cluster', 'node']),
        'thread_pool.optimize.completed': Gauge('elasticsearch_thread_pool_optimize_completed', '', ['cluster', 'node']),
        'thread_pool.optimize.largest': Gauge('elasticsearch_thread_pool_optimize_largest', '', ['cluster', 'node']),
        'thread_pool.optimize.queue': Gauge('elasticsearch_thread_pool_optimize_queue', '', ['cluster', 'node']),
        'thread_pool.optimize.rejected': Gauge('elasticsearch_thread_pool_optimize_rejected', '', ['cluster', 'node']),
        'thread_pool.optimize.threads': Gauge('elasticsearch_thread_pool_optimize_threads', '', ['cluster', 'node']),
        'thread_pool.percolate.active': Gauge('elasticsearch_thread_pool_percolate_active', '', ['cluster', 'node']),
        'thread_pool.percolate.completed': Gauge('elasticsearch_thread_pool_percolate_completed', '', ['cluster', 'node']),
        'thread_pool.percolate.largest': Gauge('elasticsearch_thread_pool_percolate_largest', '', ['cluster', 'node']),
        'thread_pool.percolate.queue': Gauge('elasticsearch_thread_pool_percolate_queue', '', ['cluster', 'node']),
        'thread_pool.percolate.rejected': Gauge('elasticsearch_thread_pool_percolate_rejected', '', ['cluster', 'node']),
        'thread_pool.percolate.threads': Gauge('elasticsearch_thread_pool_percolate_threads', '', ['cluster', 'node']),
        'thread_pool.refresh.active': Gauge('elasticsearch_thread_pool_refresh_active', '', ['cluster', 'node']),
        'thread_pool.refresh.completed': Gauge('elasticsearch_thread_pool_refresh_completed', '', ['cluster', 'node']),
        'thread_pool.refresh.largest': Gauge('elasticsearch_thread_pool_refresh_largest', '', ['cluster', 'node']),
        'thread_pool.refresh.queue': Gauge('elasticsearch_thread_pool_refresh_queue', '', ['cluster', 'node']),
        'thread_pool.refresh.rejected': Gauge('elasticsearch_thread_pool_refresh_rejected', '', ['cluster', 'node']),
        'thread_pool.refresh.threads': Gauge('elasticsearch_thread_pool_refresh_threads', '', ['cluster', 'node']),
        'thread_pool.search.active': Gauge('elasticsearch_thread_pool_search_active', '', ['cluster', 'node']),
        'thread_pool.search.completed': Gauge('elasticsearch_thread_pool_search_completed', '', ['cluster', 'node']),
        'thread_pool.search.largest': Gauge('elasticsearch_thread_pool_search_largest', '', ['cluster', 'node']),
        'thread_pool.search.queue': Gauge('elasticsearch_thread_pool_search_queue', '', ['cluster', 'node']),
        'thread_pool.search.rejected': Gauge('elasticsearch_thread_pool_search_rejected', '', ['cluster', 'node']),
        'thread_pool.search.threads': Gauge('elasticsearch_thread_pool_search_threads', '', ['cluster', 'node']),
        'thread_pool.snapshot.active': Gauge('elasticsearch_thread_pool_snapshot_active', '', ['cluster', 'node']),
        'thread_pool.snapshot.completed': Gauge('elasticsearch_thread_pool_snapshot_completed', '', ['cluster', 'node']),
        'thread_pool.snapshot.largest': Gauge('elasticsearch_thread_pool_snapshot_largest', '', ['cluster', 'node']),
        'thread_pool.snapshot.queue': Gauge('elasticsearch_thread_pool_snapshot_queue', '', ['cluster', 'node']),
        'thread_pool.snapshot.rejected': Gauge('elasticsearch_thread_pool_snapshot_rejected', '', ['cluster', 'node']),
        'thread_pool.snapshot.threads': Gauge('elasticsearch_thread_pool_snapshot_threads', '', ['cluster', 'node']),
        'thread_pool.suggest.active': Gauge('elasticsearch_thread_pool_suggest_active', '', ['cluster', 'node']),
        'thread_pool.suggest.completed': Gauge('elasticsearch_thread_pool_suggest_completed', '', ['cluster', 'node']),
        'thread_pool.suggest.queue': Gauge('elasticsearch_thread_pool_suggest_queue', '', ['cluster', 'node']),
        'thread_pool.suggest.rejected': Gauge('elasticsearch_thread_pool_suggest_rejected', '', ['cluster', 'node']),
        'thread_pool.suggest.threads': Gauge('elasticsearch_thread_pool_suggest_threads', '', ['cluster', 'node']),
        'thread_pool.warmer.active': Gauge('elasticsearch_thread_pool_warmer_active', '', ['cluster', 'node']),
        'thread_pool.warmer.completed': Gauge('elasticsearch_thread_pool_warmer_completed', '', ['cluster', 'node']),
        'thread_pool.warmer.largest': Gauge('elasticsearch_thread_pool_warmer_largest', '', ['cluster', 'node']),
        'thread_pool.warmer.queue': Gauge('elasticsearch_thread_pool_warmer_queue', '', ['cluster', 'node']),
        'thread_pool.warmer.rejected': Gauge('elasticsearch_thread_pool_warmer_rejected', '', ['cluster', 'node']),
        'thread_pool.warmer.threads': Gauge('elasticsearch_thread_pool_warmer_threads', '', ['cluster', 'node']),
    }
    ES_NODES_STATS_13 = {
        'indices.segments.index_writer_memory_in_bytes': Gauge('elasticsearch_indices_segments_index_writer_memory_in_bytes', '', ['cluster', 'node']),
        'indices.segments.version_map_memory_in_bytes': Gauge('elasticsearch_indices_segments_version_map_memory_in_bytes', '', ['cluster', 'node']),
        'indices.suggest.current': Gauge('elasticsearch_indices_suggest_current', '', ['cluster', 'node']),
        'indices.suggest.time_in_millis': Gauge('elasticsearch_indices_suggest_time_in_millis', '', ['cluster', 'node']),
        'indices.suggest.total': Gauge('elasticsearch_indices_suggest_total', '', ['cluster', 'node']),
    }
    ES_NODES_STATS_17 = {
        'indices.indexing.is_throttled': Gauge('elasticsearch_indices_indexing_is_throttled', '', ['cluster', 'node']),
        'indices.indexing.noop_update_total': Gauge('elasticsearch_indices_indexing_noop_update_total', '', ['cluster', 'node']),
        'indices.indexing.throttle_time_in_millis': Gauge('elasticsearch_indices_indexing_throttle_time_in_millis', '', ['cluster', 'node']),
        'indices.query_cache.evictions': Gauge('elasticsearch_indices_query_cache_evictions', '', ['cluster', 'node']),
        'indices.query_cache.hit_count': Gauge('elasticsearch_indices_query_cache_hit_count', '', ['cluster', 'node']),
        'indices.query_cache.memory_size_in_bytes': Gauge('elasticsearch_indices_query_cache_memory_size_in_bytes', '', ['cluster', 'node']),
        'indices.query_cache.miss_count': Gauge('elasticsearch_indices_query_cache_miss_count', '', ['cluster', 'node']),
        'indices.recovery.current_as_source': Gauge('elasticsearch_indices_recovery_current_as_source', '', ['cluster', 'node']),
        'indices.recovery.current_as_target': Gauge('elasticsearch_indices_recovery_current_as_target', '', ['cluster', 'node']),
        'indices.recovery.throttle_time_in_millis': Gauge('elasticsearch_indices_recovery_throttle_time_in_millis', '', ['cluster', 'node']),
        'indices.segments.fixed_bit_set_memory_in_bytes': Gauge('elasticsearch_indices_segments_fixed_bit_set_memory_in_bytes', '', ['cluster', 'node']),
        'indices.segments.index_writer_max_memory_in_bytes': Gauge('elasticsearch_indices_segments_index_writer_max_memory_in_bytes', '', ['cluster', 'node']),
        'thread_pool.fetch_shard_started.active': Gauge('elasticsearch_thread_pool_fetch_shard_started_active', '', ['cluster', 'node']),
        'thread_pool.fetch_shard_started.completed': Gauge('elasticsearch_thread_pool_fetch_shard_started_completed', '', ['cluster', 'node']),
        'thread_pool.fetch_shard_started.largest': Gauge('elasticsearch_thread_pool_fetch_shard_started_largest', '', ['cluster', 'node']),
        'thread_pool.fetch_shard_started.queue': Gauge('elasticsearch_thread_pool_fetch_shard_started_queue', '', ['cluster', 'node']),
        'thread_pool.fetch_shard_started.rejected': Gauge('elasticsearch_thread_pool_fetch_shard_started_rejected', '', ['cluster', 'node']),
        'thread_pool.fetch_shard_started.threads': Gauge('elasticsearch_thread_pool_fetch_shard_started_threads', '', ['cluster', 'node']),
        'thread_pool.fetch_shard_store.active': Gauge('elasticsearch_thread_pool_fetch_shard_store_active', '', ['cluster', 'node']),
        'thread_pool.fetch_shard_store.completed': Gauge('elasticsearch_thread_pool_fetch_shard_store_completed', '', ['cluster', 'node']),
        'thread_pool.fetch_shard_store.largest': Gauge('elasticsearch_thread_pool_fetch_shard_store_largest', '', ['cluster', 'node']),
        'thread_pool.fetch_shard_store.queue': Gauge('elasticsearch_thread_pool_fetch_shard_store_queue', '', ['cluster', 'node']),
        'thread_pool.fetch_shard_store.rejected': Gauge('elasticsearch_thread_pool_fetch_shard_store_rejected', '', ['cluster', 'node']),
        'thread_pool.fetch_shard_store.threads': Gauge('elasticsearch_thread_pool_fetch_shard_store_threads', '', ['cluster', 'node']),
        'thread_pool.listener.active': Gauge('elasticsearch_thread_pool_listener_active', '', ['cluster', 'node']),
        'thread_pool.listener.completed': Gauge('elasticsearch_thread_pool_listener_completed', '', ['cluster', 'node']),
        'thread_pool.listener.largest': Gauge('elasticsearch_thread_pool_listener_largest', '', ['cluster', 'node']),
        'thread_pool.listener.queue': Gauge('elasticsearch_thread_pool_listener_queue', '', ['cluster', 'node']),
        'thread_pool.listener.rejected': Gauge('elasticsearch_thread_pool_listener_rejected', '', ['cluster', 'node']),
        'thread_pool.listener.threads': Gauge('elasticsearch_thread_pool_listener_threads', '', ['cluster', 'node']),
        'thread_pool.suggest.largest': Gauge('elasticsearch_thread_pool_suggest_largest', '', ['cluster', 'node']),
    }

    def __init__(self, options):
        self.options = options
        if self.options.host == 'localhost':
            self.options.host = socket.getfqdn()
        self.hostname = self.options.host
        self._get_es_version('/')
        # Build metrics list
        self.cluster_metrics = self.ES_CLUSTER_HEALTH_10
        self.nodes_stats_metrics = self.ES_NODES_STATS_10
        if self.es_version >= [1,3,0]:
            self.nodes_stats_metrics.update(self.ES_NODES_STATS_13)
        if self.es_version >= [1, 6, 0]:
            self.cluster_metrics.update(self.ES_CLUSTER_HEALTH_16)
        if self.es_version >= [1, 7, 0]:
            self.cluster_metrics.update(self.ES_CLUSTER_HEALTH_17)
            self.nodes_stats_metrics.update(self.ES_NODES_STATS_17)

    def _do_get_rawdata(self,url):
        address = 'http://' + self.hostname + ':' + str(self.options.port) + url
        try:
            resp = requests.get(
                address,
                timeout=1
            )
            resp.raise_for_status()
        except:
            logging.error('Failed to open: ' + address)
            raise
        return resp

    def _process_path(self, path, value):
        data = {}
        for k in path.split('.'):
            if value is not None:
                value = value.get(k,None)
            else:
                break
        if value is not None:
            real_key = string.replace(path, '.', '_')
            if value in self.ES_CLUSTER_MAPPING:
                value = self.ES_CLUSTER_MAPPING[value]
            data[real_key] = value
        else:
            logging.warning('Could not find key ' + path)
        return data

    def _get_es_version(self, url):
        raw_data = self._do_get_rawdata(url)
        try:
            raw_data = raw_data.json()
        except TypeError:
            raw_data = raw_data.json
        self.es_version = map(int, raw_data['version']['number'].split('.')[0:3])

    def _cluster_health(self, url):
        data = {}
        raw_data = self._do_get_rawdata(url)
        try:
            self.cluster_health = raw_data.json()
        except TypeError:
            self.cluster_health = raw_data.json
        self.cluster_name = self.cluster_health['cluster_name']

    def _nodes_stats(self, url):
        data = {}
        nodes_stats = self._do_get_rawdata(url)
        try:
            self.nodes_stats = nodes_stats.json()
        except TypeError:
            self.nodes_stats = nodes_stats.json

    def _master_status(self, url):
        data = {}
        # Check wether current node is master for the cluster
        whois_master = self._do_get_rawdata(url)
        whois_master = whois_master.text.split(' ')
        self.is_cluster_master = True if whois_master[3] == self.hostname else False


    def get_metrics(self):
        self._nodes_stats(self.NODE_STATS_URL)
        self._cluster_health(self.CLUSTER_HEALTH_URL)
        self._master_status(self.MASTER_STATUS_URL)

    def create_prometheus_metrics(self):
        self.get_metrics()
        # Process metrics list
        for node in self.nodes_stats['nodes']:
            # Skip non data nodes
            if 'attributes' in self.nodes_stats['nodes'][node] and \
               'data' in self.nodes_stats['nodes'][node]['attributes'] and \
               self.nodes_stats['nodes'][node]['attributes']['data'] == 'false':
                continue
            raw_data = self.nodes_stats['nodes'][node]
            self.node_host = raw_data['host']
            data = {}
            for path, metric in self.nodes_stats_metrics.items():
                for k,v in self._process_path(path, raw_data).items():
                    k = string.replace(k, '.', '_')
                    try:
                        metric.labels({'cluster':self.cluster_name, 'node':self.node_host}).set(v)
                    except Exception as e:
                        logging.warning("Could set value %s for metric %s : %s" % (v, k, e))

        metric = self.ES_IS_MASTER['elasticsearch_cluster_is_master']
        metric.labels({"cluster":self.cluster_name, "node":self.node_host}).set(self.ES_CLUSTER_MAPPING[self.is_cluster_master])

        for path, metric in self.cluster_metrics.items():
            for k,v in self._process_path(path, self.cluster_health).items():
                metric.labels({'cluster':self.cluster_name}).set(v)


if __name__ == '__main__':
    parser = optparse.OptionParser()
    es_options = optparse.OptionGroup(parser, "Elasticsearch Configuration")
    es_options.add_option("-H", "--host", default="localhost",
                          help="Elasticsearch server hostname")
    es_options.add_option("-P", "--port", default=9200,
                          help="Elasticsearch server port")
    es_options.add_option("-v", "--verbose", action="store_true")
    es_options.add_option("-r", "--refresh", default="10", help="get a new measure every N seconds")
    es_options.add_option("-l", "--logging", default="INFO", help="logging level")
    parser.add_option_group(es_options)
    (options, args) = parser.parse_args()

    level = logging.INFO
    if options.logging == "ERROR":
        level=logging.ERROR
    if options.logging == "WARNING":
        level=logging.WARNING
    if options.logging == "DEBUG":
        level=logging.DEBUG

    logging.basicConfig(
        format='[%(asctime)s] %(name)s.%(levelname)s %(threadName)s %(message)s',
        level=level
    )
    logging.captureWarnings(True)


    logging.info('Starting server...')
    port = 9091
    start_http_server(port)
    logging.info('Server started on port %s', port)
    while True:
        try:
            es = ElasticsearchServer(options)
            es.create_prometheus_metrics()
        except Exception as e:
            logging.error(e)

        time.sleep(int(options.refresh))
