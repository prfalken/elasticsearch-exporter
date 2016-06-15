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
    ES_CLUSTER_MAPPING={
      'green': 0,
      'yellow': 1,
      'red': 2,
      False: 0,
      True: 1
    }

    ES_IS_MASTER = {
        'elasticsearch_cluster_is_master': Gauge('elasticsearch_cluster_is_master', '')
    }

    ES_CLUSTER_HEALTH_10 = {
        'active_primary_shards': Gauge('elasticsearch_cluster_health_active_primary_shards', ''),
        'active_shards': Gauge('elasticsearch_cluster_health_active_shards', ''),
        'initializing_shards': Gauge('elasticsearch_cluster_health_initializing_shards', ''),
        'number_of_data_nodes': Gauge('elasticsearch_cluster_health_number_of_data_nodes', ''),
        'number_of_nodes': Gauge('elasticsearch_cluster_health_number_of_nodes', ''),
        'relocating_shards': Gauge('elasticsearch_cluster_health_relocating_shards', ''),
        'status': Gauge('elasticsearch_cluster_health_status', ''),
        'timed_out': Gauge('elasticsearch_cluster_health_timed_out', ''),
        'unassigned_shards': Gauge('elasticsearch_cluster_health_unassigned_shards', ''),

    }
    ES_CLUSTER_HEALTH_16 = {
        'number_of_in_flight_fetch': Gauge('elasticsearch_cluster_health_number_of_in_flight_fetch', ''),
        'number_of_pending_tasks': Gauge('elasticsearch_cluster_health_number_of_pending_tasks', ''),
    }
    ES_CLUSTER_HEALTH_17 = {
        'delayed_unassigned_shards': Gauge('elasticsearch_cluster_health_delayed_unassigned_shards', ''),
    }
    ES_NODES_STATS_10 = {
        'indices.docs.count': Gauge('elasticsearch_indices_docs_count', ''),
        'indices.docs.deleted': Gauge('elasticsearch_indices_docs_deleted', ''),
        'indices.fielddata.evictions': Gauge('elasticsearch_indices_fielddata_evictions', ''),
        'indices.fielddata.memory_size_in_bytes': Gauge('elasticsearch_indices_fielddata_memory_size_in_bytes', ''),
        'indices.filter_cache.evictions': Gauge('elasticsearch_indices_filter_cache_evictions', ''),
        'indices.filter_cache.memory_size_in_bytes': Gauge('elasticsearch_indices_filter_cache_memory_size_in_bytes', ''),
        'indices.flush.total': Gauge('elasticsearch_indices_flush_total', ''),
        'indices.flush.total_time_in_millis': Gauge('elasticsearch_indices_flush_total_time_in_millis', ''),
        'indices.get.current': Gauge('elasticsearch_indices_get_current', ''),
        'indices.get.exists_time_in_millis': Gauge('elasticsearch_indices_get_exists_time_in_millis', ''),
        'indices.get.exists_total': Gauge('elasticsearch_indices_get_exists_total', ''),
        'indices.get.missing_time_in_millis': Gauge('elasticsearch_indices_get_missing_time_in_millis', ''),
        'indices.get.missing_total': Gauge('elasticsearch_indices_get_missing_total', ''),
        'indices.get.time_in_millis': Gauge('elasticsearch_indices_get_time_in_millis', ''),
        'indices.get.total': Gauge('elasticsearch_indices_get_total', ''),
        'indices.indexing.delete_current': Gauge('elasticsearch_indices_indexing_delete_current', ''),
        'indices.indexing.delete_time_in_millis': Gauge('elasticsearch_indices_indexing_delete_time_in_millis', ''),
        'indices.indexing.delete_total': Gauge('elasticsearch_indices_indexing_delete_total', ''),
        'indices.indexing.index_current': Gauge('elasticsearch_indices_indexing_index_current', ''),
        'indices.indexing.index_time_in_millis': Gauge('elasticsearch_indices_indexing_index_time_in_millis', ''),
        'indices.indexing.index_total': Gauge('elasticsearch_indices_indexing_index_total', ''),
        'indices.merges.current': Gauge('elasticsearch_indices_merges_current', ''),
        'indices.merges.current_docs': Gauge('elasticsearch_indices_merges_current_docs', ''),
        'indices.merges.current_size_in_bytes': Gauge('elasticsearch_indices_merges_current_size_in_bytes', ''),
        'indices.merges.total': Gauge('elasticsearch_indices_merges_total', ''),
        'indices.merges.total_docs': Gauge('elasticsearch_indices_merges_total_docs', ''),
        'indices.merges.total_size_in_bytes': Gauge('elasticsearch_indices_merges_total_size_in_bytes', ''),
        'indices.merges.total_time_in_millis': Gauge('elasticsearch_indices_merges_total_time_in_millis', ''),
        'indices.percolate.current': Gauge('elasticsearch_indices_percolate_current', ''),
        'indices.percolate.memory_size': Gauge('elasticsearch_indices_percolate_memory_size', ''),
        'indices.percolate.memory_size_in_bytes': Gauge('elasticsearch_indices_percolate_memory_size_in_bytes', ''),
        'indices.percolate.queries': Gauge('elasticsearch_indices_percolate_queries', ''),
        'indices.percolate.time_in_millis': Gauge('elasticsearch_indices_percolate_time_in_millis', ''),
        'indices.percolate.total': Gauge('elasticsearch_indices_percolate_total', ''),
        'indices.refresh.total': Gauge('elasticsearch_indices_refresh_total', ''),
        'indices.refresh.total_time_in_millis': Gauge('elasticsearch_indices_refresh_total_time_in_millis', ''),
        'indices.search.fetch_current': Gauge('elasticsearch_indices_search_fetch_current', ''),
        'indices.search.fetch_time_in_millis': Gauge('elasticsearch_indices_search_fetch_time_in_millis', ''),
        'indices.search.fetch_total': Gauge('elasticsearch_indices_search_fetch_total', ''),
        'indices.search.open_contexts': Gauge('elasticsearch_indices_search_open_contexts', ''),
        'indices.search.query_current': Gauge('elasticsearch_indices_search_query_current', ''),
        'indices.search.query_time_in_millis': Gauge('elasticsearch_indices_search_query_time_in_millis', ''),
        'indices.search.query_total': Gauge('elasticsearch_indices_search_query_total', ''),
        'indices.segments.count': Gauge('elasticsearch_indices_segments_count', ''),
        'indices.segments.memory_in_bytes': Gauge('elasticsearch_indices_segments_memory_in_bytes', ''),
        'indices.store.size_in_bytes': Gauge('elasticsearch_indices_store_size_in_bytes', ''),
        'indices.store.throttle_time_in_millis': Gauge('elasticsearch_indices_store_throttle_time_in_millis', ''),
        'indices.translog.operations': Gauge('elasticsearch_indices_translog_operations', ''),
        'indices.translog.size_in_bytes': Gauge('elasticsearch_indices_translog_size_in_bytes', ''),
        'indices.warmer.current': Gauge('elasticsearch_indices_warmer_current', ''),
        'indices.warmer.total': Gauge('elasticsearch_indices_warmer_total', ''),
        'indices.warmer.total_time_in_millis': Gauge('elasticsearch_indices_warmer_total_time_in_millis', ''),
        'jvm.mem.heap_used_in_bytes': Gauge('elasticsearch_jvm_mem_heap_used_in_bytes', ''),
        'jvm.mem.heap_used_percent': Gauge('elasticsearch_jvm_mem_heap_used_percent', ''),
        'jvm.mem.heap_committed_in_bytes': Gauge('elasticsearch_jvm_mem_heap_committed_in_bytes', ''),
        'jvm.mem.heap_max_in_bytes': Gauge('elasticsearch_jvm_mem_heap_max_in_bytes', ''),
        'jvm.mem.non_heap_used_in_bytes': Gauge('elasticsearch_jvm_mem_non_heap_used_in_bytes', ''),
        'jvm.mem.non_heap_committed_in_bytes': Gauge('elasticsearch_jvm_mem_non_heap_committed_in_bytes', ''),
        'jvm.mem.pools.young.used_in_bytes': Gauge('elasticsearch_jvm_mem_pools_young_used_in_bytes', ''),
        'jvm.mem.pools.young.max_in_bytes': Gauge('elasticsearch_jvm_mem_pools_young_max_in_bytes', ''),
        'jvm.mem.pools.young.peak_used_in_bytes': Gauge('elasticsearch_jvm_mem_pools_young_peak_used_in_bytes', ''),
        'jvm.mem.pools.young.peak_max_in_bytes': Gauge('elasticsearch_jvm_mem_pools_young_peak_max_in_bytes', ''),
        'jvm.mem.pools.survivor.used_in_bytes': Gauge('elasticsearch_jvm_mem_pools_survivor_used_in_bytes', ''),
        'jvm.mem.pools.survivor.max_in_bytes': Gauge('elasticsearch_jvm_mem_pools_survivor_max_in_bytes', ''),
        'jvm.mem.pools.survivor.peak_used_in_bytes': Gauge('elasticsearch_jvm_mem_pools_survivor_peak_used_in_bytes', ''),
        'jvm.mem.pools.survivor.peak_max_in_bytes': Gauge('elasticsearch_jvm_mem_pools_survivor_peak_max_in_bytes', ''),
        'jvm.mem.pools.old.used_in_bytes': Gauge('elasticsearch_jvm_mem_pools_old_used_in_bytes', ''),
        'jvm.mem.pools.old.max_in_bytes': Gauge('elasticsearch_jvm_mem_pools_old_max_in_bytes', ''),
        'jvm.mem.pools.old.peak_used_in_bytes': Gauge('elasticsearch_jvm_mem_pools_old_peak_used_in_bytes', ''),
        'jvm.mem.pools.old.peak_max_in_bytes': Gauge('elasticsearch_jvm_mem_pools_old_peak_max_in_bytes', ''),
        'jvm.threads.count': Gauge('elasticsearch_jvm_threads_count', ''),
        'jvm.threads.peak_count': Gauge('elasticsearch_jvm_threads_peak_count', ''),
        'jvm.gc.collectors.young.collection_count': Gauge('elasticsearch_jvm_gc_collectors_young_collection_count', ''),
        'jvm.gc.collectors.young.collection_time_in_millis': Gauge('elasticsearch_jvm_gc_collectors_young_collection_time_in_millis', ''),
        'jvm.gc.collectors.old.collection_count': Gauge('elasticsearch_jvm_gc_collectors_old_collection_count', ''),
        'jvm.gc.collectors.old.collection_time_in_millis': Gauge('elasticsearch_jvm_gc_collectors_old_collection_time_in_millis', ''),
        'jvm.buffer_pools.direct.count': Gauge('elasticsearch_jvm_buffer_pools_direct_count', ''),
        'jvm.buffer_pools.direct.used_in_bytes': Gauge('elasticsearch_jvm_buffer_pools_direct_used_in_bytes', ''),
        'jvm.buffer_pools.direct.total_capacity_in_bytes': Gauge('elasticsearch_jvm_buffer_pools_direct_total_capacity_in_bytes', ''),
        'jvm.buffer_pools.mapped.count': Gauge('elasticsearch_jvm_buffer_pools_mapped_count', ''),
        'jvm.buffer_pools.mapped.used_in_bytes': Gauge('elasticsearch_jvm_buffer_pools_mapped_used_in_bytes', ''),
        'jvm.buffer_pools.mapped.total_capacity_in_bytes': Gauge('elasticsearch_jvm_buffer_pools_mapped_total_capacity_in_bytes', ''),
        'thread_pool.bulk.active': Gauge('elasticsearch_thread_pool_bulk_active', ''),
        'thread_pool.bulk.completed': Gauge('elasticsearch_thread_pool_bulk_completed', ''),
        'thread_pool.bulk.largest': Gauge('elasticsearch_thread_pool_bulk_largest', ''),
        'thread_pool.bulk.queue': Gauge('elasticsearch_thread_pool_bulk_queue', ''),
        'thread_pool.bulk.rejected': Gauge('elasticsearch_thread_pool_bulk_rejected', ''),
        'thread_pool.bulk.threads': Gauge('elasticsearch_thread_pool_bulk_threads', ''),
        'thread_pool.flush.active': Gauge('elasticsearch_thread_pool_flush_active', ''),
        'thread_pool.flush.completed': Gauge('elasticsearch_thread_pool_flush_completed', ''),
        'thread_pool.flush.largest': Gauge('elasticsearch_thread_pool_flush_largest', ''),
        'thread_pool.flush.queue': Gauge('elasticsearch_thread_pool_flush_queue', ''),
        'thread_pool.flush.rejected': Gauge('elasticsearch_thread_pool_flush_rejected', ''),
        'thread_pool.flush.threads': Gauge('elasticsearch_thread_pool_flush_threads', ''),
        'thread_pool.generic.active': Gauge('elasticsearch_thread_pool_generic_active', ''),
        'thread_pool.generic.completed': Gauge('elasticsearch_thread_pool_generic_completed', ''),
        'thread_pool.generic.largest': Gauge('elasticsearch_thread_pool_generic_largest', ''),
        'thread_pool.generic.queue': Gauge('elasticsearch_thread_pool_generic_queue', ''),
        'thread_pool.generic.rejected': Gauge('elasticsearch_thread_pool_generic_rejected', ''),
        'thread_pool.generic.threads': Gauge('elasticsearch_thread_pool_generic_threads', ''),
        'thread_pool.get.active': Gauge('elasticsearch_thread_pool_get_active', ''),
        'thread_pool.get.completed': Gauge('elasticsearch_thread_pool_get_completed', ''),
        'thread_pool.get.largest': Gauge('elasticsearch_thread_pool_get_largest', ''),
        'thread_pool.get.queue': Gauge('elasticsearch_thread_pool_get_queue', ''),
        'thread_pool.get.rejected': Gauge('elasticsearch_thread_pool_get_rejected', ''),
        'thread_pool.get.threads': Gauge('elasticsearch_thread_pool_get_threads', ''),
        'thread_pool.index.active': Gauge('elasticsearch_thread_pool_index_active', ''),
        'thread_pool.index.completed': Gauge('elasticsearch_thread_pool_index_completed', ''),
        'thread_pool.index.largest': Gauge('elasticsearch_thread_pool_index_largest', ''),
        'thread_pool.index.queue': Gauge('elasticsearch_thread_pool_index_queue', ''),
        'thread_pool.index.rejected': Gauge('elasticsearch_thread_pool_index_rejected', ''),
        'thread_pool.index.threads': Gauge('elasticsearch_thread_pool_index_threads', ''),
        'thread_pool.management.active': Gauge('elasticsearch_thread_pool_management_active', ''),
        'thread_pool.management.completed': Gauge('elasticsearch_thread_pool_management_completed', ''),
        'thread_pool.management.largest': Gauge('elasticsearch_thread_pool_management_largest', ''),
        'thread_pool.management.queue': Gauge('elasticsearch_thread_pool_management_queue', ''),
        'thread_pool.management.rejected': Gauge('elasticsearch_thread_pool_management_rejected', ''),
        'thread_pool.management.threads': Gauge('elasticsearch_thread_pool_management_threads', ''),
        'thread_pool.merge.active': Gauge('elasticsearch_thread_pool_merge_active', ''),
        'thread_pool.merge.completed': Gauge('elasticsearch_thread_pool_merge_completed', ''),
        'thread_pool.merge.largest': Gauge('elasticsearch_thread_pool_merge_largest', ''),
        'thread_pool.merge.queue': Gauge('elasticsearch_thread_pool_merge_queue', ''),
        'thread_pool.merge.rejected': Gauge('elasticsearch_thread_pool_merge_rejected', ''),
        'thread_pool.merge.threads': Gauge('elasticsearch_thread_pool_merge_threads', ''),
        'thread_pool.optimize.active': Gauge('elasticsearch_thread_pool_optimize_active', ''),
        'thread_pool.optimize.completed': Gauge('elasticsearch_thread_pool_optimize_completed', ''),
        'thread_pool.optimize.largest': Gauge('elasticsearch_thread_pool_optimize_largest', ''),
        'thread_pool.optimize.queue': Gauge('elasticsearch_thread_pool_optimize_queue', ''),
        'thread_pool.optimize.rejected': Gauge('elasticsearch_thread_pool_optimize_rejected', ''),
        'thread_pool.optimize.threads': Gauge('elasticsearch_thread_pool_optimize_threads', ''),
        'thread_pool.percolate.active': Gauge('elasticsearch_thread_pool_percolate_active', ''),
        'thread_pool.percolate.completed': Gauge('elasticsearch_thread_pool_percolate_completed', ''),
        'thread_pool.percolate.largest': Gauge('elasticsearch_thread_pool_percolate_largest', ''),
        'thread_pool.percolate.queue': Gauge('elasticsearch_thread_pool_percolate_queue', ''),
        'thread_pool.percolate.rejected': Gauge('elasticsearch_thread_pool_percolate_rejected', ''),
        'thread_pool.percolate.threads': Gauge('elasticsearch_thread_pool_percolate_threads', ''),
        'thread_pool.refresh.active': Gauge('elasticsearch_thread_pool_refresh_active', ''),
        'thread_pool.refresh.completed': Gauge('elasticsearch_thread_pool_refresh_completed', ''),
        'thread_pool.refresh.largest': Gauge('elasticsearch_thread_pool_refresh_largest', ''),
        'thread_pool.refresh.queue': Gauge('elasticsearch_thread_pool_refresh_queue', ''),
        'thread_pool.refresh.rejected': Gauge('elasticsearch_thread_pool_refresh_rejected', ''),
        'thread_pool.refresh.threads': Gauge('elasticsearch_thread_pool_refresh_threads', ''),
        'thread_pool.search.active': Gauge('elasticsearch_thread_pool_search_active', ''),
        'thread_pool.search.completed': Gauge('elasticsearch_thread_pool_search_completed', ''),
        'thread_pool.search.largest': Gauge('elasticsearch_thread_pool_search_largest', ''),
        'thread_pool.search.queue': Gauge('elasticsearch_thread_pool_search_queue', ''),
        'thread_pool.search.rejected': Gauge('elasticsearch_thread_pool_search_rejected', ''),
        'thread_pool.search.threads': Gauge('elasticsearch_thread_pool_search_threads', ''),
        'thread_pool.snapshot.active': Gauge('elasticsearch_thread_pool_snapshot_active', ''),
        'thread_pool.snapshot.completed': Gauge('elasticsearch_thread_pool_snapshot_completed', ''),
        'thread_pool.snapshot.largest': Gauge('elasticsearch_thread_pool_snapshot_largest', ''),
        'thread_pool.snapshot.queue': Gauge('elasticsearch_thread_pool_snapshot_queue', ''),
        'thread_pool.snapshot.rejected': Gauge('elasticsearch_thread_pool_snapshot_rejected', ''),
        'thread_pool.snapshot.threads': Gauge('elasticsearch_thread_pool_snapshot_threads', ''),
        'thread_pool.suggest.active': Gauge('elasticsearch_thread_pool_suggest_active', ''),
        'thread_pool.suggest.completed': Gauge('elasticsearch_thread_pool_suggest_completed', ''),
        'thread_pool.suggest.queue': Gauge('elasticsearch_thread_pool_suggest_queue', ''),
        'thread_pool.suggest.rejected': Gauge('elasticsearch_thread_pool_suggest_rejected', ''),
        'thread_pool.suggest.threads': Gauge('elasticsearch_thread_pool_suggest_threads', ''),
        'thread_pool.warmer.active': Gauge('elasticsearch_thread_pool_warmer_active', ''),
        'thread_pool.warmer.completed': Gauge('elasticsearch_thread_pool_warmer_completed', ''),
        'thread_pool.warmer.largest': Gauge('elasticsearch_thread_pool_warmer_largest', ''),
        'thread_pool.warmer.queue': Gauge('elasticsearch_thread_pool_warmer_queue', ''),
        'thread_pool.warmer.rejected': Gauge('elasticsearch_thread_pool_warmer_rejected', ''),
        'thread_pool.warmer.threads': Gauge('elasticsearch_thread_pool_warmer_threads', ''),
    }
    ES_NODES_STATS_13 = {
        'indices.segments.index_writer_memory_in_bytes': Gauge('elasticsearch_indices_segments_index_writer_memory_in_bytes', ''),
        'indices.segments.version_map_memory_in_bytes': Gauge('elasticsearch_indices_segments_version_map_memory_in_bytes', ''),
        'indices.suggest.current': Gauge('elasticsearch_indices_suggest_current', ''),
        'indices.suggest.time_in_millis': Gauge('elasticsearch_indices_suggest_time_in_millis', ''),
        'indices.suggest.total': Gauge('elasticsearch_indices_suggest_total', ''),
    }
    ES_NODES_STATS_17 = {
        'indices.indexing.is_throttled': Gauge('elasticsearch_indices_indexing_is_throttled', ''),
        'indices.indexing.noop_update_total': Gauge('elasticsearch_indices_indexing_noop_update_total', ''),
        'indices.indexing.throttle_time_in_millis': Gauge('elasticsearch_indices_indexing_throttle_time_in_millis', ''),
        'indices.query_cache.evictions': Gauge('elasticsearch_indices_query_cache_evictions', ''),
        'indices.query_cache.hit_count': Gauge('elasticsearch_indices_query_cache_hit_count', ''),
        'indices.query_cache.memory_size_in_bytes': Gauge('elasticsearch_indices_query_cache_memory_size_in_bytes', ''),
        'indices.query_cache.miss_count': Gauge('elasticsearch_indices_query_cache_miss_count', ''),
        'indices.recovery.current_as_source': Gauge('elasticsearch_indices_recovery_current_as_source', ''),
        'indices.recovery.current_as_target': Gauge('elasticsearch_indices_recovery_current_as_target', ''),
        'indices.recovery.throttle_time_in_millis': Gauge('elasticsearch_indices_recovery_throttle_time_in_millis', ''),
        'indices.segments.fixed_bit_set_memory_in_bytes': Gauge('elasticsearch_indices_segments_fixed_bit_set_memory_in_bytes', ''),
        'indices.segments.index_writer_max_memory_in_bytes': Gauge('elasticsearch_indices_segments_index_writer_max_memory_in_bytes', ''),
        'thread_pool.fetch_shard_started.active': Gauge('elasticsearch_thread_pool_fetch_shard_started_active', ''),
        'thread_pool.fetch_shard_started.completed': Gauge('elasticsearch_thread_pool_fetch_shard_started_completed', ''),
        'thread_pool.fetch_shard_started.largest': Gauge('elasticsearch_thread_pool_fetch_shard_started_largest', ''),
        'thread_pool.fetch_shard_started.queue': Gauge('elasticsearch_thread_pool_fetch_shard_started_queue', ''),
        'thread_pool.fetch_shard_started.rejected': Gauge('elasticsearch_thread_pool_fetch_shard_started_rejected', ''),
        'thread_pool.fetch_shard_started.threads': Gauge('elasticsearch_thread_pool_fetch_shard_started_threads', ''),
        'thread_pool.fetch_shard_store.active': Gauge('elasticsearch_thread_pool_fetch_shard_store_active', ''),
        'thread_pool.fetch_shard_store.completed': Gauge('elasticsearch_thread_pool_fetch_shard_store_completed', ''),
        'thread_pool.fetch_shard_store.largest': Gauge('elasticsearch_thread_pool_fetch_shard_store_largest', ''),
        'thread_pool.fetch_shard_store.queue': Gauge('elasticsearch_thread_pool_fetch_shard_store_queue', ''),
        'thread_pool.fetch_shard_store.rejected': Gauge('elasticsearch_thread_pool_fetch_shard_store_rejected', ''),
        'thread_pool.fetch_shard_store.threads': Gauge('elasticsearch_thread_pool_fetch_shard_store_threads', ''),
        'thread_pool.listener.active': Gauge('elasticsearch_thread_pool_listener_active', ''),
        'thread_pool.listener.completed': Gauge('elasticsearch_thread_pool_listener_completed', ''),
        'thread_pool.listener.largest': Gauge('elasticsearch_thread_pool_listener_largest', ''),
        'thread_pool.listener.queue': Gauge('elasticsearch_thread_pool_listener_queue', ''),
        'thread_pool.listener.rejected': Gauge('elasticsearch_thread_pool_listener_rejected', ''),
        'thread_pool.listener.threads': Gauge('elasticsearch_thread_pool_listener_threads', ''),
        'thread_pool.suggest.largest': Gauge('elasticsearch_thread_pool_suggest_largest', ''),
    }

    def __init__(self):
        self.parse_args()
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

    def parse_args(self):
        parser = optparse.OptionParser()
        es_options = optparse.OptionGroup(parser, "Elasticsearch Configuration")
        es_options.add_option("-H", "--host", default="localhost",
                              help="Elasticsearch server hostname")
        es_options.add_option("-P", "--port", default=9200,
                              help="Elasticsearch server port")
        es_options.add_option("-v", "--verbose", action="store_true")
        es_options.add_option("-r", "--refresh", default="10", help="get a new measure every N seconds")
        parser.add_option_group(es_options)
        (options, args) = parser.parse_args()
        self.options = options

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
            raw_data = raw_data.json()
        except TypeError:
            raw_data = raw_data.json
        # Process metrics list
        for path, metric in self.cluster_metrics.items():
            for k,v in self._process_path(path, raw_data).items():
                metric.set(v)

    def _nodes_stats(self, url):
        data = {}
        nodes_stats = self._do_get_rawdata(url)
        try:
            nodes_stats = nodes_stats.json()
        except TypeError:
            nodes_stats = nodes_stats.json
        # Process metrics list
        for node in nodes_stats['nodes']:
            # Skip non data nodes
            if 'attributes' in nodes_stats['nodes'][node] and \
               'data' in nodes_stats['nodes'][node]['attributes'] and \
               nodes_stats['nodes'][node]['attributes']['data'] == 'false':
                continue
            raw_data = nodes_stats['nodes'][node]
            data = {}
            for path, metric in self.nodes_stats_metrics.items():
                for k,v in self._process_path(path, raw_data).items():
                    k = string.replace(k, '.', '_')
                    try:
                        metric.set(v)
                    except:
                        logging.warning("Could not parse value %s for metric %s" % (v, k))

    def _master_status(self, url):
        data = {}
        # Check wether current node is master for the cluster
        whois_master = self._do_get_rawdata(url)
        whois_master = whois_master.text.split(' ')
        self.is_cluster_master = True if whois_master[3] == self.hostname else False
        metric = self.ES_IS_MASTER['elasticsearch_cluster_is_master']
        metric.set(self.ES_CLUSTER_MAPPING[self.is_cluster_master])

    def get_metrics(self):
        self._cluster_health('/_cluster/health/')
        self._master_status('/_cat/master/')
        self._nodes_stats('/_nodes/_local/stats/')


if __name__ == '__main__':
    logging.basicConfig(
        format='[%(asctime)s] %(name)s.%(levelname)s %(threadName)s %(message)s',
        level=logging.DEBUG
    )
    logging.captureWarnings(True)


    logging.info('Starting server...')
    port = 9091
    start_http_server(port)
    logging.info('Server started on port %s', port)
    while True:
        try:
            es = ElasticsearchServer()
            es.get_metrics()
        except:
            pass

        time.sleep(int(es.options.refresh))
