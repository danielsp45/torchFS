#include "cache.h"
#include "eviction_policy.h"
#include "file_handle.h"
#include <memory>
#include <gflags/gflags.h>

DEFINE_int32(cache_size, 0, "Size of the cache in number of entries");

Cache::Cache(std::unique_ptr<IEvictionPolicy> policy) {
    capacity_ = FLAGS_cache_size;
    policy_ = std::move(policy);
}

std::shared_ptr<FileHandle> Cache::lookup(const uint64_t &inode) {
    std::shared_lock lk(mu_);

    auto it = index_.find(inode);
    if (it == index_.end()) {
        return nullptr;
    }
    policy_->update(inode);
    return it->second;
}

void Cache::insert(const uint64_t &inode, std::shared_ptr<FileHandle> value) {
    std::unique_lock lk(mu_);

    if (index_.size() == capacity_) {
        uint64_t evict_key = policy_->evict();
        if (evict_key != static_cast<uint64_t>(-1)) {
            // index_.erase(evict_key);
            auto it = index_.find(evict_key);
            if (it != index_.end()) {
                index_.erase(it); // Remove from cache
                it->second->uncache(); // Mark the file handle as uncached
            } 

            index_[inode] = value;
            policy_->insert(inode);
            value->cache(); // Mark the file handle as cached
        }
    } else {
        index_[inode] = value;
        policy_->insert(inode);
        value->cache(); // Mark the file handle as cached
    }
}
