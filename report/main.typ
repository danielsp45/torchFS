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

#lorem(100)

== Sub Motivation 1

#lorem(200)

= The TorchFS design

#lorem(100)

= Implementation

#lorem(100)

= Conclusion

#lorem(100)
