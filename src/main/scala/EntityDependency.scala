package main.scala
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.hadoop.conf._

object EntityDependency{
  
  def run(sc: SparkContext,inputNodes: RDD[(Long, List[String])],inputEdges: RDD[(Long,String)]): RDD[(Long, List[String])] = {
    
    val cartesianInput = inputEdges.cartesian(inputEdges)
    val mappedEdges = cartesianInput.map(f => if(f._1._2==f._2._2) Edge(f._1._1,f._2._1,1) else Edge(-1,-1,0)).filter(node => node.attr!=0)
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