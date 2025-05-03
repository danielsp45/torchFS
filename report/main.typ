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

With the rapid evolution of deep learning (DL) and machine learning (ML), there's not only a demand for efficient algorithms and powerfull GPU's, but also for storage layers that can keep up with the increasing size of training datasets. As the amount of data used in training DL models continues to grow exponentially, the need for efficient data storage and retrieval has become crucial. Traditional distributed filesystem solutions @ceph @hdfs @lustre deliver solid throughput and durability, but they aren’t tuned for the mostly sequential, read-heavy access patterns of ML data pipelines. 

In this paper, we introduce TorchFS, a distributed filesystem built specifically for deep-learning workloads. TorchFS plugs directly into the PyTorch ecosystem @pytorch, exposing a familiar POSIX-style interface while retooling the backend for training-centric performance and resilience.

Our design centers on three key ideas:

1. *Metadata/data separation* — decoupling namespace operations from bulk I/O (inspired by GFS, GPFS and BeeGFS @gfs @gpfs @beegfs), so lookups never contend with tensor streaming.  
2. *Horizontal scalability* — object-striped data servers that add bandwidth and capacity linearly as you scale out.  
3. *Epoch aware caching & pre-fetch* — using training-step hints to drive asynchronous, batched reads that overlap decoding and host-to-device copies, minimizing pipeline stalls. REVER ISTO

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
