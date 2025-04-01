#pragma once

#include <string>
#include <utility>

// A simple implementation of a Status class similar to RocksDB's design.
class Status {
public:
  // Error codes used to represent the result of an operation.
  enum Code {
    kOk = 0,
    kNotFound,
    kCorruption,
    kInvalidArgument,
    kIOError,
    // Extend with additional error codes as needed.
  };

  // Default constructor creates an OK status.
  Status() : code_(kOk), msg_("") {}

  // Constructor for creating an error status with a message.
  Status(Code code, std::string msg) : code_(code), msg_(std::move(msg)) {}

  // Copy constructor and assignment operators are defaulted.
  Status(const Status &other) = default;
  Status(Status &&other) noexcept = default;
  Status &operator=(const Status &other) = default;
  Status &operator=(Status &&other) noexcept = default;

  // Returns true if the status represents success.
  bool ok() const { return code_ == kOk; }

  // Returns a human-readable string representation of this status.
  std::string ToString() const {
    if (ok()) {
      return "OK";
    } else {
      return CodeToString(code_) + ": " + msg_;
    }
  }

  // Factory methods for common statuses.
  static Status OK() { return Status(); }
  static Status NotFound(const std::string &msg) {
    return Status(kNotFound, msg);
  }
  static Status Corruption(const std::string &msg) {
    return Status(kCorruption, msg);
  }
  static Status InvalidArgument(const std::string &msg) {
    return Status(kInvalidArgument, msg);
  }
  static Status IOError(const std::string &msg) {
    return Status(kIOError, msg);
  }

  // Comparison operators.
  bool operator==(const Status &other) const {
    return code_ == other.code_ && msg_ == other.msg_;
  }
  bool operator!=(const Status &other) const { return !(*this == other); }

private:
  Code code_;
  std::string msg_;

  // Helper function to convert an error code to a string.
  static std::string CodeToString(Code code) {
    switch (code) {
    case kOk:
      return "OK";
    case kNotFound:
      return "NotFound";
    case kCorruption:
      return "Corruption";
    case kInvalidArgument:
      return "InvalidArgument";
    case kIOError:
      return "IOError";
    default:
      return "UnknownError";
    }
  }
};
