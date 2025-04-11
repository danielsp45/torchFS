#ifndef SLICE_H
#define SLICE_H

#include <cassert>
#include <cstddef>
#include <cstring>
#include <string>
#pragma once

class Slice {
  public:
    // Create an empty slice.
    Slice() : data_(nullptr), size_(0), owns_(false) {}

    // Create a slice that refers to d[0, n-1].
    // 'owns' indicates whether this slice owns the memory.
    Slice(const char *d, size_t n, bool owns = false)
        : data_(d), size_(n), owns_(owns) {}

    // Create a slice that refers to the contents of a std::string.
    // Here, we assume the slice does not own the memory.
    Slice(const std::string &s)
        : data_(s.data()), size_(s.size()), owns_(false) {}

    // Create a slice that refers to the contents of a std::string_view.
    Slice(const std::string_view &sv)
        : data_(sv.data()), size_(sv.size()), owns_(false) {}

    // Create a slice that refers to a null-terminated C-string.
    Slice(const char *s)
        : data_(s), size_((s == nullptr) ? 0 : std::strlen(s)), owns_(false) {}

    ~Slice() {
        // If we own the data, delete it (assuming it was allocated with new[]).
        if (owns_ && data_) {
            delete[] data_;
        }
    }

    // Returns a pointer to the underlying data.
    const char *data() const { return data_; }

    // Returns a pointer to the underlying data (non-const version).
    char *data() { return const_cast<char *>(data_); }

    // Returns the size (in bytes) of the slice.
    size_t size() const { return size_; }

    // Returns true if the slice is empty.
    bool empty() const { return size_ == 0; }

    // Access the nth byte (with bounds checking in debug builds).
    char operator[](size_t n) const {
        assert(n < size_);
        return data_[n];
    }

    // Remove the first n bytes from the slice.
    void remove_prefix(size_t n) {
        assert(n <= size_);
        data_ += n;
        size_ -= n;
    }

    // Remove the last n bytes from the slice.
    void remove_suffix(size_t n) {
        assert(n <= size_);
        size_ -= n;
    }

    // Returns a std::string copy of the slice.
    std::string to_string() const { return std::string(data_, size_); }

    // Compares two slices.
    int compare(const Slice &other) const {
        const size_t min_len = (size_ < other.size_) ? size_ : other.size_;
        int cmp = std::memcmp(data_, other.data_, min_len);
        if (cmp == 0) {
            if (size_ < other.size_)
                cmp = -1;
            else if (size_ > other.size_)
                cmp = 1;
        }
        return cmp;
    }

    // Returns true if this slice starts with the given prefix.
    bool starts_with(const Slice &prefix) const {
        return (size_ >= prefix.size_) &&
               (std::memcmp(data_, prefix.data_, prefix.size_) == 0);
    }

    // Returns true if this slice ends with the given suffix.
    bool ends_with(const Slice &suffix) const {
        return (size_ >= suffix.size_) &&
               (std::memcmp(data_ + size_ - suffix.size_, suffix.data_,
                            suffix.size_) == 0);
    }

    // Setter to update the size, e.g., after a read operation.
    void set_size(size_t new_size) { size_ = new_size; }

    // Optionally, provide a setter for the data pointer if needed.
    void set_data(const char *new_data) { data_ = new_data; }

  private:
    const char *data_;
    size_t size_;
    bool owns_; // Indicates if this Slice is responsible for deleting the
                // memory.
};

#endif // SLICE_H