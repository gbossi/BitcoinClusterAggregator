/**
 *  Bitcoin Cluster Aggregator
 *  Cluster-based graph representation of the bitcoin blockchain
 *  Copyright (C) 2019  Giacomo Bossi
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 * 
 */


import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.hadoop.conf._
import scala.collection.mutable.ListBuffer
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import scala.Iterator


object EntityDependency extends Serializable{
  
  /**
    * Unwrap the the tuple to provide a flat result
    * 
    * @param sc current Spark context
    * @param inputNodes RDD containing a tuple of (ID, List[Address])
    * @param inputEdges RDD containing a tuple of (Address,ID)
    * 
    * @return a RDD containing all the clusters found
    */
  
  def run(sc: SparkContext,inputNodes: RDD[(Long, List[String])],inputEdges: RDD[(String,Long)]): RDD[(Long, List[String])] = {
    
    /** Accumulate all the transaction numbers in which an address appear and create an array with length less than 5*/
    val reverseEdges = inputEdges.aggregateByKey(ListBuffer.empty[Long])(
        (numList, num) => {numList += num; numList},
         (numList1, numList2) => {numList1.appendAll(numList2); numList1})
            .map(_._2.distinct.toArray).flatMap(splitArrayEvery(_,4))
    
    /** Create a chain of edges */        
    val mappedEdges = reverseEdges.flatMap(f => 
                if(f.length==1) Iterator((f(0),f(0))) else
                  if(f.length==2) Iterator((f(0),f(1))) else
                    if(f.length==3) Iterator((f(0),f(1)),(f(1),f(2))) else
                    Iterator((f(0),f(1)),(f(1),f(2)),(f(2),f(3))))
    mappedEdges.count
    
    /** Return the connected components, given the list of edges */
    val cc = ConnectedComponent.alternatingAlgo(mappedEdges, 9999999, 9999999, false, 0, 40)
    
    /** Reorder the output of the connected components into (ClusterID, NodeID) */
    val reversecc = cc._1.map(f => (f._2,f._1))
    
    /** Accumulate all the NodeIDs for each cluster */
    val connectedEntity = reversecc.aggregateByKey(ListBuffer.empty[Long])(
        (numList, num) => {numList += num; numList},
         (numList1, numList2) => {numList1.appendAll(numList2); numList1}).map(f => (f._1,f._2.distinct.toArray))
             
    /** Put into broadcast the inputNodes*/
    val broadNodes = sc.broadcast(inputNodes.collectAsMap)    
    
    /** USe the broadcast join to recover the values of the addresses belonging to a cluster */
    val ent = connectedEntity.map(id => (id._1,(broadNodes.value.get(id._1).getOrElse(List[String]()) ++ id._2.flatMap(broadNodes.value.get(_).getOrElse(List[String]())))))
    
    /** Extract the result */
    val result = ent.map(f => (f._1,f._2.distinct))
    result;
  }
   
  
  /**
    * Unwrap the the tuple to provide a flat result
    * 
    * @param xs array to be split
    * @param sep maximum length of the split array
    *  
    * @return a list of ordered array 
    */
  
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