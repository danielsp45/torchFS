# TorchFS

**A distributed, POSIX-compatible filesystem optimized for PyTorch training workloads**

TorchFS addresses the gap between general-purpose distributed storage and the sequential, read-heavy patterns of modern deep learning. By plugging directly into the PyTorch ecosystem via FUSE, TorchFS exposes a familiar POSIX interface while retooling its backend for training-centric performance and resilience.

Key design highlights:
1. **Epoch-Aware Caching & Prefetching**  
   Utilizes training-step hints to keep upcoming data locally hot, reducing per-epoch latency and sustaining GPU utilization.

2. **Horizontal Scalability**  
   Object-striped storage nodes (SNs) add bandwidth and capacity linearly, and can join the cluster by simply registering with the metadata service.

3. **Metadata–Data Separation**  
   A Raft-based metadata cluster (MDNs) manages namespace operations independently of bulk I/O, preventing metadata contention from interrupting tensor streaming.

4. **Erasure-Code Resilience**  
   Reed–Solomon encoding across k + m fragments delivers fault tolerance with low storage overhead, while client-side decode/encode hides node failures from training jobs.

To read more about our decisions, look into our paper inside this repository.

## Installation

1. **Install the required system packages**
```bash
sudo apt update
sudo apt install -y \
  cmake \
  g++ \
  gcc \
  pkg-config \
  liberasurecode-dev \
  librocksdb-dev \
  libprotobuf-dev \
  libgflags-dev \
  libfuse-dev \
  libfuse3-dev \
  flex \
  bison
```

Also make sure you have VCPKG installed in your machine.

2. **Clone the repository**
```bash
git clone https://github.com/danielsp45/torchFS.git
cd torchFS
```
2. **Install dependencies**
```bash
bin/build
```

## Running the project

To run the project you need to first start off the metadata service:
```bash
bin/metadata
```
Then you can start the storage nodes:
```bash 
bin/storage
```
Finally, you can mount the filesystem:
```bash
bin/client
```
