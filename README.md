# Bitcoin Cluster Aggregator

#### *Author:* 
#### *Giacomo Bossi*

## Abstract

The aim of the project is to parse and analyze the bitcoin block-chain in order to retrieve useful information about the transaction between known wallets, using a distributed approach.

## Approach

Since the bitcoin block-chain is heavily compressed, first we must unwrap all the transaction in a more readable way and after condensing all the addresses belonging to the same entity and build an entity graph.

## Bitcoin Transaction
### Transaction Composition

In order to retrieve all the data needed to compose each transaction it has been used the Hadoop Crypto-Ledger library (LINK), 
a multi-blockchain parser, that allow to translate the raw-byte data into an object. But this unwrap of the blockchain produce this result:

<p align="center">
  <img width="75%" src="https://github.com/gbossi/BitcoinClusterAggregator/blob/master/img/UnwrapResult.png">
</p>

To recompose each transaction in a more readable way (Input,Output,Amount), 
it is possible to join two transaction using as key the transaction ID and the transaction index and reconstruct the flow of coins between two wallets in the following way:

<p align="center">
  <img width="75%" src="https://github.com/gbossi/BitcoinClusterAggregator/blob/master/img/Join Explained.png">
</p>

So, given a current transaction and a previous one with the backward link of the current pointing to the previous, 
the resulting unwrapping it’s the following:

<p align="center">
  <img width="40%" src="https://github.com/gbossi/BitcoinClusterAggregator/blob/master/img/transaction.png">
</p>


## Entity
We define an entity as a cluster of addresses belonging to the same owner/organization.
### Partial Entity

Since a transaction can involve multiple input and multiple output, it’s possible to state that all the inputs belong to the same entity.
This cluster is formed by all the addresses that has been used simultaneously as an input in a transaction.  
So, for example an entity is the following: 

<p align="center">
  <img width="40%" src="https://github.com/gbossi/BitcoinClusterAggregator/blob/master/img/partial_entity.png">
</p>
 
But this kind of entity results incomplete, since there could be another transaction that involves one of more addresses previously used.  It’s clear the concept of partial entity, the inputs address of a single transaction describe a possible subset of all the entity’s addresses. 

### Complete Entity

A complete entity can be defined as the union of the intersection of the partial entities. 
This kind of clustering results into a recursive intersection between all the inputs of all the transaction, since it’s required to search all the intersections between the other partial entity, and in positive case we merge the two partial entity and start from the beginning again. But this kind of algorithm it’s just to complex to be computed in a feasible time, so we have developed an algorithm that translate the domain into a graph and find all the connected components inside it.


## Complete Entity Algorithm
In order to compute the intersection over all the partial entity, we first transform the intersection over the transactions into a graph and then we have applied an optimized version of the connected component algorithm in order to accumulate all the intersection and provide a solution.
### Generating chain subgraph
First, given all the partial entity we create a new tuple with the following values inside:
(address,partialEntity)
After we accumulate for each address all the entity in which it is present:
(address,List[PartialEntity])

<p align="center">
  <img width="50%" src="https://github.com/gbossi/BitcoinClusterAggregator/blob/master/img/initial_config.png">
</p>
 
Now we got for each address all the intersection over the partial entities. From this list we create a list of edges that links all the partial entities together. In order to save memory and computational power, the shape of all the subgraphs defined by a partial entity is a chain. 

<p align="center">
  <img width="40%" src="https://github.com/gbossi/BitcoinClusterAggregator/blob/master/img/intermediate_result.png">
</p>

So, given a list of edges it’s possible to run the connected components algorithm over the set of subgraphs.

### Connected Components
The result of the connected components algorithm is the following:

<p align="center">
  <img width="30%" src="https://github.com/gbossi/BitcoinClusterAggregator/blob/master/img/ccresult.png">
</p>

Every entity is the union of a set of partial entity. So, for each  cluster, retrieving the content of each partial entity and reassembling together all the addresses we finally find all the entities. 

<p align="center">
  <img width="30%" src="https://github.com/gbossi/BitcoinClusterAggregator/blob/master/img/finalResult.png">
</p>

## Entity Graph

<p align="center">
  <img width="75%" src="https://github.com/gbossi/BitcoinClusterAggregator/blob/master/img/resulting_graph.png">
</p>

### Instruction
Build using mave the project
```
mvn clean install
```

Put the bitcoin blockchain raw data inside a hdfs:
```
hadoop fs -mkdir -p /user/bitcoin/input
hadoop fs -put ~./.bitcoin/blocks/blk*.dat /user/bitcoin/input
```

Run the cluster-based graph builder:
```
spark-submit --class Builder --master local[4] ./target/BitcoinExplorer-0.0.1.jar hdfs://localhost:9000/user/bitcoin/input hdfs://localhost:9000/user/bitcoin/output/vertices hdfs://localhost:9000/user/bitcoin/output/edges
```

Run the Pagerank Algorithm
```
spark-submit --class Pagerank --master local[4] ./target/BitcoinExplorer-0.0.1.jar hdfs://localhost:9000/user/bitcoin/output/vertices hdfs://localhost:9000/user/bitcoin/output/edges hdfs://localhost:9000/user/bitcoin/output/pagerank
```

Run the Triangle Count Algorithm
```
spark-submit --class TriangleCount --master local[4] ./target/BitcoinExplorer-0.0.1.jar hdfs://localhost:9000/user/bitcoin/output/vertices hdfs://localhost:9000/user/bitcoin/output/edges hdfs://localhost:9000/user/bitcoin/output/trianglecount
```

