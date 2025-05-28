#include "cache.h"
#include "eviction_policy.h"
#include "file_handle.h"

Cache::Cache(std::unique_ptr<IEvictionPolicy> policy) {
    capacity_ = 5;
    policy_ = std::move(policy);
}

std::shared_ptr<FileHandle> Cache::is_cached(const std::string &key) {
    auto it = index_.find(key);
    if (it == index_.end()) {
        return nullptr;
    }
    policy_->update(key);
    return it->second;
}

void Cache::insert(const std::string& key, std::shared_ptr<FileHandle> value) {
    if (index_.size() == capacity_) {
        std::string evict_key = policy_->evict();
        if (!evict_key.empty()) {
            index_.erase(evict_key);
        }
        std::cout << "Evicting key: " << evict_key << std::endl;
    }
    index_[key] = value;
    policy_->insert(key);
}
