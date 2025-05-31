#import "@preview/lilaq:0.3.0" as lq : *
#import "lib.typ": *

#let author1 = (
  name: "Daniel Pereira",
  institution: "University of Minho",
  email: "pg55928@alunos.uminho.pt",
)

#let author2 = (
  name: "Duarte Ribeiro",
  institution: "University of Minho",
  email: "pg6969@alunos.uminho.pt",
)

#let author3 = (
  name: "Filipe Pereira",
  institution: "University of Minho",
  email: "pg55941@alunos.uminho.pt",
)

#let conference = (
  name: "Data Storage Systems",
  location: "",
  date: "",
)

#let abstract = ([
	As deep learning and machine learning advance, the need for efficient data storage and retrieval has become crucial due to the exponential growth of training datasets. We introduce TorchFS, a distributed file system tailored for storing and retrieving data used in training deep learning models. Designed for integration with the PyTorch framework, TorchFS prioritizes performance and resilience to meet the demands of modern AI workflows.
])

#show: doc => sigplan(
  title: "TorchFS: A machine learning oriented file-system",
  authors: (author1, author2, author3),
  conference: conference,
  abstract: abstract,
  keywords: ("typst", "acm"),
  bibliography-file: "refs.bib",
  doc
)


= Introduction

With the rapid evolution of deep learning (DL) and machine learning (ML), the demand for a filesystem that can handle the intense data ingestion requirements of ML training pipelines has become paramount. As the volume of training data continues to grow exponentially, efficient data storage and retrieval are crucial. Traditional distributed filesystems like Ceph, HDFS, and Lustre @ceph @hdfs @lustre offer reliable throughput and durability but are not optimized for the predominantly sequential, read-heavy access patterns typical of ML data workflows.

In this paper, we introduce TorchFS, a distributed filesystem that addresses this gap by providing a filesystem designed specifically for ML workloads. TorchFS plugs directly into the PyTorch ecosystem, exposing a familiar POSIX-style interface while retooling the backend for training-centric performance and resilience.

Our design centers on three key ideas:

1. *Epoch aware caching & pre-fetch* — utilizes training-step hints to keep frequently accessed training data on the host device, reducing latency and minimizing data transfer overhead.
2. *Horizontal scalability* — object-striped data servers that add bandwidth and capacity linearly as you scale out.  
3. *Metadata/data separation* — decoupling namespace operations from bulk I/O (inspired by GFS, GPFS and BeeGFS @gfs @gpfs @beegfs), so lookups never contend with tensor streaming.  

This design ensures that TorchFS can handle the unique demands of DL workloads, providing a robust and efficient solution for data storage and retrieval.

= Background and Challenges

We aimed to develop a filesystem that was optimized for the specific needs of ML workloads, particularly for use with the PyTorch framework. Therefore, we needed to consider the unique characteristics of these workloads and how they differ from traditional file systems.

Traditional file systems are designed to handle a wide variety of workloads, including random access patterns, small file sizes, and a mix of read and write operations. However, ML workloads often involve large datasets that are accessed in a more predictable manner, with the bulk of the load coming from reading training files. This means we can tailor our filesystem to better suit these workloads using optimizations.

== PyTorch Access Patterns

To know how to optimize our filesystem to suit PyTorch workloads, we first needed to understand how PyTorch accesses data. Therefore, we have to first profile the PyTorch data access patterns. 

We profiled the data acess patterns of PyTorch using a passthrough implementation of FUSE @fuse that logged the accesses made to files during training. We evaluated the access patterns with two different datasets: resnet and cosmoflow, with 1 and 4 GPUs each. Resnet is a composed of a few large files (~80MB each), while cosmoflow is composed of many small files (~64KB each). These four configurations let us evaluate the access patterns of PyTorch in different scenarios.

After evaluation, we found the PyTorch access pattern to be as follows:
1. Epoch Starts
2. The GPU opens a few files, and reads a few chunks #footnote[Chunks are about 130KB in size] of data from each file immediately (presumably as a pre-fetch cache)
3. The GPUs then start reading from the first file sequentially, in a first-come-first-serve manner, until the end of the file is reached.
  3.1 When the file is a chunk long, each GPU uses a whole file by themselves
4. When a file is fully read, it is closed. A new one (that is after the first few that were opened at the start) is then opened and its first chunks read, and the next file in the order is fully read.
5. When all files are fully read, the epoch ends.
6. After each epoch, a small number of files are opened and read sequentially, presumably for validation purposes.
7. A new epoch starts and the process repeats.

Below is a bar chart that plots the operation count for each dataset.
We discovered that the acess pattern varies accross operations:
- `Readdir` and `Create` are used a fixed (and small) ammount of times 
- `Open` and `Close` are proportional to the number of files in the dataset (and therefore the number of epochs)
- `Write` is tied mostly to epoch count, but also to the number of files
- `Getattr` is tied both to the file size and file count, as it was called for every open, close and also every two reads
- `Read` is tied to the file size (or count if the file is smaller than a chunk)

#figure(
  image("images/acesses_graph.svg")
)

=== Insights

This behaviour brings us some insights into how we can optimize our filesystem to better suit PyTorch workloads:

1. The access pattern is predictable, with a few files being opened and read sequentially, and the next file in the order is read.
  - We can accurately pre-fetch files and chunks into a in-memory cache before they are neeeded, reducing latency and increasing GPU usage.
2. After each epoch, the first few files are opened and read sequentially.
  - We can prefetch these files into the same cache, again reducing latency.


= The TorchFS design

TorchFS addresses the unique demands of machine learning workloads through three core design objectives: scalability, performance, and availability. The system employs a decoupled architecture that separates metadata management from data storage operations, enabling each component to scale independently while optimizing for the distinct access patterns characteristic of ML applications.

The architecture comprises three interconnected components: the client component exposes a standard POSIX filesystem interface, providing transparent access to the distributed storage infrastructure while ensuring compatibility with existing ML frameworks and training pipelines; a data storage cluster that manages all file operations including reads, writes, and deletions, operating independently from metadata operations to maximize throughput; and a metadata cluster that maintains the filesystem namespace (file attributes and directories) and manages the mapping between logical file paths and their physical storage locations across the data cluster.

This separation of concerns enables TorchFS to scale each subsystem according to workload demands. Metadata operations, which involve namespace traversal and file discovery, are handled exclusively by the metadata cluster. Most of data operations bypass the metadata cluster during execution, allowing the storage infrastructure to scale horizontally based on bandwidth and storage capacity requirements without creating bottlenecks in the metadata layer.

TorchFS incorporates performance optimizations tailored to ML training patterns. The system implements aggressive read caching to maintain frequently accessed data locally on the client, reducing latency during training iterations. The prefetching mechanism anticipates future data requirements based on the access patterns mentioned above, ensuring that required data is available before the ML framework requests it. These techniques effectively hide the latency associated with distributed storage access while leveraging the predictable and sequential nature of ML dataset consumption.

The system maintains high availability through comprehensive fault tolerance mechanisms. The metadata cluster employs the Raft consensus algorithm to ensure consistency and availability of the filesystem namespace in case of any metadata server failures. Data durability is achieved through erasure coding applied to data chunks distributed across the storage cluster. This approach delivers configurable fault tolerance levels while maintaining storage efficiency, enabling the system to tolerate multiple simultaneous server failures while preserving data integrity and operational continuity.

= Client Operation

- Here we descrube TorchFS interaction with applications by descibring client operation (interaction between applications and distributed storage via client)

- Client runs on the host and exposes a file system interface to applications

- TorchFS client runs in user space and can be accessed as a mounted file system via FUSE @fuse

- Translates posix calls to distributed filesystem operations

== Client-Side Operation Processing

- Open Flow
  - client contacts metadata server to fetch file inode, attributes 
  - Client gets a FileHandle to call operations over that file

- Read Flow
  - Client checks if the file is cached locally
    - Read
  - If not requests the location of the file chunks to the metadata server
    - Fetch erasure code chunks from the storage nodes
    - Rebuild the file with erasure decoding of the chunks
    - Cache the file locally
    - Read

- Write flow
  - File is local
    - Write to the FileHandle
  - FIle is remote
    - Fetch the file like in read
    - Write to the file
  - Flushing to the storage nodes implies encoding the file to get the shards and send them to each server accordingly

- CLose flow
  - If theres no one using the file, flush it to storage even if cached
  - Update metadata in metadata server
  - Fsync
  - Remove the file if not cached

== Erasure Coding

- Fault tolerance, Space Optimization

== Caching and Prefetching

- Client maintains its own file data cache,
independent of the kernel page or buffer caches, making
it accessible to applications that use the mounted filesystem

= Distributed Metadata

- Raft, RocksDB, Centralized approach

= Distributed Storage

- Nothing special, rely on OS optimizations

= Performance Evaluation (and Scalability?)

- Resnet & cosmoflow
  - Test with different optimization configurations

- Resnet (max)

- cosmoflow (max)

- NFS (max)

= Related Work

#lorem(50)

= Conclusion

#lorem(50)
