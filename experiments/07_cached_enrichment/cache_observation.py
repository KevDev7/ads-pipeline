"""Scoped persistence with metadata-only snapshots; no materialization action."""
from pyspark import StorageLevel


class CachedEnrichment:
    def __init__(self, frame):
        self.frame = frame.persist(StorageLevel.MEMORY_AND_DISK_DESER)
        self.rdd_id = None

    def snapshot(self, spark):
        # This Spark 4.0.1 cache-manager lookup identifies this frame's cached RDD,
        # rather than attributing unrelated Delta metadata caches to enrichment.
        cached = spark._jsparkSession.sharedState().cacheManager().lookupCachedData(self.frame._jdf)
        registered = cached.isDefined()
        if registered:
            self.rdd_id = int(cached.get().cachedRepresentation().cacheBuilder().cachedColumnBuffers().id())
        rows = [info for info in spark.sparkContext._jsc.sc().getRDDStorageInfo()
                if int(info.id()) == self.rdd_id]
        info = rows[0] if rows else None
        level = self.frame.storageLevel
        return {
            'registered': registered, 'rdd_id': self.rdd_id,
            'storage_level': {'use_memory': level.useMemory, 'use_disk': level.useDisk,
                              'deserialized': level.deserialized, 'replication': level.replication},
            'storage_info_present': info is not None,
            'total_partitions': int(info.numPartitions()) if info else 0,
            'cached_partitions': int(info.numCachedPartitions()) if info else 0,
            'memory_bytes': int(info.memSize()) if info else 0,
            'disk_bytes': int(info.diskSize()) if info else 0,
        }

    def release(self):
        self.frame.unpersist(blocking=True)
