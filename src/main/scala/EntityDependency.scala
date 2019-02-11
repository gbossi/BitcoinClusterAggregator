package main.scala
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.hadoop.conf._
import scala.collection.mutable.ListBuffer


object EntityDependency extends Serializable{
  
  def run(sc: SparkContext,inputNodes: RDD[(Long, List[String])],inputEdges: RDD[(String,Long)]): RDD[(Long, List[String])] = {
    
    
    val reverseEdges = inputEdges.aggregateByKey(ListBuffer.empty[Long])(
        (numList, num) => {numList += num; numList},
         (numList1, numList2) => {numList1.appendAll(numList2); numList1})
            .map(_._2.distinct.toArray).flatMap(splitArrayEvery(_,4))
             
    val mappedEdges = reverseEdges.flatMap(f => 
                if(f.length==1) Iterator((f(0),f(0))) else
                  if(f.length==2) Iterator((f(0),f(1))) else
                    if(f.length==3) Iterator((f(0),f(1)),(f(1),f(2))) else
                    Iterator((f(0),f(1)),(f(1),f(2)),(f(2),f(3))))
    mappedEdges.count
    
    //return id -> cluster
    val cc = ConnectedComponent.alternatingAlgo(mappedEdges, 9999999, 9999999, false, 0, 40)
    
    val reversecc = cc._1.map(f => (f._2,f._1))
    
    val connectedEntity = reversecc.aggregateByKey(ListBuffer.empty[Long])(
        (numList, num) => {numList += num; numList},
         (numList1, numList2) => {numList1.appendAll(numList2); numList1}).map(f => (f._1,f._2.distinct.toArray))
             
    val broadNodes = sc.broadcast(inputNodes.collectAsMap)    
    val ent = connectedEntity.map(id => (id._1,(broadNodes.value.get(id._1).getOrElse(List[String]()) ++ id._2.flatMap(broadNodes.value.get(_).getOrElse(List[String]())))))
    val result = ent.map(f => (f._1,f._2.distinct))
    result;
  }
  
  def splitArrayEvery(xs: Array[Long], sep: Int): List[Array[Long]] = {
    var res = List[Array[Long]]()
    val ordxs = xs.sortWith(_ > _)
    val length = ordxs.length
    if(length>sep){
      var i=0
      val limit = ((length-1).toFloat/(sep-1)).ceil.toInt
      for (j <- 1 to limit) {    
        res ::= ordxs.slice(i, List(i+sep,length).min)
        i = j*(sep-1)
      }
    }else
      res::=ordxs
    res
  } 
}