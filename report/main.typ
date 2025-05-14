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

We profiled the data acess patterns of PyTorch using a passthrough implementation of FUSE that logged the accesses made to files during training. We evaluated the access patterns with two different datasets: resnet and cosmoflow, with 1 and 4 GPUs each. Resnet is a composed of a few large files (~80MB each), while cosmoflow is composed of many small files (~64KB each). These four configurations let us evaluate the access patterns of PyTorch in different scenarios.

After evaluation, we found the PyTorch access pattern to be as follows:
1. Epoch Starts
2. The GPU opens a few files, and reads a few chunks #footnote[Chunks are about 130KB in size] of data from each file immediately (presumably as a pre-fetch cache)
3. The GPUs then start reading from the first file sequentially, in a first-come-first-serve manner, until the end of the file is reached.
  3.1 When the file is a chunk long, each GPU uses a file
4. When a file is fully read, it is closed, a new one (that is after the first few that were opened at the start) is opened and its first chunks read, and the next file in the order is fully read.
5. When all files are fully read, the epoch ends.
6. After each epoch, a small number files are opened and read sequentially, presumable for validation purposes.
7. A new epoch starts and the process repeats.

=== Insights

This behaviour brings us some insights into how we can optimize our filesystem to better suit PyTorch workloads:

- The access pattern is predictable, with a few files being opened and read sequentially, and the next file in order is read.
  - We can accurately pre-fetch files and chunks into a in-memory cache before they are neeeded, therefore reducing the latency and increasing GPU usage.
- After each epoch, the first few files are opened and read sequentially.
  - We can prefetch these files into the same cache, again reducing latency.


= The TorchFS design

#lorem(100)

= Implementation

#lorem(100)

= Conclusion

#lorem(100)
