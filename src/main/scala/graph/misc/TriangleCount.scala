/**
 *  Bitcoin Address Explorer 
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
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.hadoop.conf._
import scala.math.BigInt


class TriangleCount {
  def main(args: Array[String]): Unit = {
     val conf = new SparkConf().setAppName("Bitcoin Entity - Triangle count")
     val sc= new SparkContext(conf)
     val hadoopConf = new Configuration()
     run(sc,hadoopConf,args(0),args(1),args(2))
     sc.stop()
  }
  
  def run(sc: SparkContext, hadoopConf: Configuration, inputVertices: String, inputEdges: String, output: String): Unit = {
    val vertices = sc.objectFile[(VertexId,List[String])](inputVertices)
    val edges = sc.objectFile[Edge[BigInt]](inputEdges)

    val graph = Graph.apply(vertices, edges).cache
    
    val countTriangles = graph.triangleCount
    val triCounts = graph.triangleCount().vertices

    val triCountNodes = vertices.join(triCounts).map { case (id, (username, tc)) =>
      (username, tc)
        }
    triCountNodes.sortBy(_._2, false, 1).saveAsObjectFile(output)
  }
}