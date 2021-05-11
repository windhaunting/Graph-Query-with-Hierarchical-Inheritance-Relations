# Graph Query with Inheritance relations

Graph query is to query answers from a heterogeneous information networks with a query graph. We explore hierarchical inheritance
relations between entities and formulate the problem of graph query on heterogeneous information networks with hierarchical inheritance relations. We propose a graph query search algorithm by decomposing the original query graph into multiple star queries and apply a star query algorithm to each star query.

## Table of content

- [Installation](#installation)
- [Datasets](#datasets)
- [Algorithm Overview](#algorithm-overview)
- [Results](#results)

## Installation

- Hadoop
- Spark GraphX


## Datasets

- Synthetic graph data
- Cisco Product Data
- Extended DBLP Data

## Algorithm Overview

- Model the graph inheritance relations
- Query graph decompostion into Star query graphs
- Solve star query 
- Combine Star query answer 


The graph query flow:

![flow](https://github.com/windhaunting/graphQueryHierarchical/blob/master/hierQuery1.png)

The graph query architecure:

![flow](https://github.com/windhaunting/graphQueryHierarchical/blob/master/distributed_graph_query_architecture.png)


## Results

We have greatly the query quality considering the hierachical relations while maintains the similar efficiency compared with the state-of-the-art method.



