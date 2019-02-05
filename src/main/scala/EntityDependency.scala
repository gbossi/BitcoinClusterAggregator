package main.scala
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.hadoop.conf._

object EntityDependency{
  
  def run(sc: SparkContext,inputNodes: RDD[(Long, List[String])],inputEdges: RDD[(Long,String)]): RDD[(Long, List[String])] = {
    val reverseEdges = inputEdges.map(f=>(f._2,Array(f._1))).reduceByKey(_ ++ _)
    val joinInput = reverseEdges.flatMap(g => g._2.combinations(2).map(i => (i.head,i.last)))  
    val mappedEdges = joinInput.map(f =>  Edge(f._1,f._2,1)).distinct()
    inputNodes.persist
    val entityGraph: Graph[(List[String]),Int] = Graph(inputNodes,mappedEdges)
    val connectedEntity =  entityGraph.connectedComponents().vertices.map(f => (f._1.toLong,f._2.toLong)).groupByKey()
    val broadNodes = sc.broadcast(inputNodes.collectAsMap)
    val ent = connectedEntity.map(id => (id._1,(broadNodes.value.get(id._1).getOrElse(List[String]()) ++ id._2.flatMap(broadNodes.value.get(_).getOrElse(List[String]())))))
    inputNodes.unpersist()
    val result = ent.map(f => (f._1,f._2.distinct))
    result;
  } 
}