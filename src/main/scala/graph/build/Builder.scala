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


import bitcoin.parser._
import bitcoin.structure._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.hadoop.conf._
import scala.math.BigInt
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.io._
import scala.collection.JavaConverters._
import bitcoin.parser.BitcoinBlockFileInputFormat
import bitcoin.structure.BitcoinBlock
import org.apache.hadoop.io.BytesWritable
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import bitcoin.parser.BitcoinBlockFileInputFormat
import bitcoin.structure.BitcoinBlock
import org.apache.hadoop.io.BytesWritable



object Builder {
  
   def main(args: Array[String]): Unit = {
     val conf = new SparkConf().setAppName("Bitcoin Explorer")
     val sc= new SparkContext(conf)
     val hadoopConf = new Configuration()
     hadoopConf.set("hadoopcryptoledger.bitcoinblockinputformat.filter.magic","F9BEB4D9")
     parser(sc,hadoopConf,args(0),args(1),args(2))
     sc.stop()
     }
   
   /**
		* Parse the Block-chain into a entity-based graph 
		* 
    * @param sc current spark context
    * @param hadoopConf current spark
    * @param inputFile hdfs directory location where the blockchain is stored
    * @param outputVertices directory location where to store the vertices 
    * @param outputEdges directory location where to store the edges
		*
    */

   def parser(sc: SparkContext, hadoopConf: Configuration, inputFile: String, outputVertices: String, outputEdges: String): Unit = {
     val bitcoinBlocksRDD = sc.newAPIHadoopFile(inputFile, classOf[BitcoinBlockFileInputFormat], classOf[BytesWritable], classOf[BitcoinBlock], hadoopConf)

     val bitcoinTransactionRDD = bitcoinBlocksRDD.flatMap(hadoopKeyValueTuple => extractTransactionData(hadoopKeyValueTuple._2))
     
     bitcoinTransactionRDD.persist()
     
     /** Map the transactions using as key (PreviousId,PreviousIndex) */
     val mappedPrevTransaction = bitcoinTransactionRDD.map(f => ((f.getPrevTxID.+(f.getPrevTxIndex)),f))
     
     /** Map the transactions using as key (CurrentId,CurrentIndex) */
     val mappedCurrTransaction = bitcoinTransactionRDD.map(f => ((f.getTxID.+(f.getTxIndex)),f))
     
     bitcoinTransactionRDD.unpersist()
     
     
     /** Join the transaction using as joining key (PreviousId,PreviousIndex)==(CurrentId,CurrentIndex) */
     val joinTransaction = mappedCurrTransaction.join(mappedPrevTransaction) 
     
     /** Group all the transactions using the Transaction Identifier of the previous transaction */
     val groupedJoinTransaction = joinTransaction.map(f => (f._2._2.getTxID,f._2)).groupByKey()
     
    
     /** Extract all the information of the transactions and assign for each of them an ID number  */
     val tableOfTransaction = groupedJoinTransaction.mapValues(f => extractWalletTransaction(f)).zipWithIndex().map(f => (f._2,f._1._2))

   
     
     tableOfTransaction.persist()
     
     /***/
     val inputWallets = tableOfTransaction.mapValues(f => f.map(g => g._1).distinct)
     
     /***/
     val justTransaction = tableOfTransaction.flatMap(f => f._2)
     
     tableOfTransaction.unpersist()
     
     inputWallets.persist
     
     /** Flat and reorder the input wallets before the clustering*/
     val flatWallets = inputWallets.flatMap(unwrapListStringNumber(_))
     
     /** return the list of cluster of addresses */
     val aggregateResult = EntityDependency.run(sc, inputWallets, flatWallets)  
     
     inputWallets.unpersist()
     
     
     aggregateResult.persist()
     
     /** Flat the result before the mapping */
     val flatResult = aggregateResult.flatMap(unwrapListStringNumber(_))

     /** Maps each address with the belonging address */
     val broadcastNodes = sc.broadcast(flatResult.collectAsMap)  
     
     /** Use the transaction to compose the edges between entities */
     val edges = justTransaction.map(f =>Edge(broadcastNodes.value.get(f._1).getOrElse(-1),broadcastNodes.value.get(f._2).getOrElse(-1),f._3))
         
     val graph: Graph[(List[String]), BigInt] = Graph(aggregateResult, edges).cache
     aggregateResult.unpersist()
     
     /** Save the result ;) */
     graph.vertices.saveAsObjectFile(outputVertices)
     graph.edges.saveAsObjectFile(outputEdges)
     
   }
   
   
   /**
    * Extract from a iterable of joined transaction a list of tuple containing Souce Address, the Target Address and the Amount of Satoshis Exchanged
    * 
    * @param table a list of joined transactions
    * 
    * @return a list tuple containing the source address the destination address and the amount exchanged
    */
   
   def extractWalletTransaction(table : Iterable[(Transaction,Transaction)]): List[(String,String,BigInt)] ={
     val ret = table.map(f => (f._1.getBicointAddress,f._2.getBicointAddress,f._2.getAmount)).toList
     ret;
   } 
   
   /**
    * Unwrap the the tuple to provide a flat result
    * 
    * @param data a tuple containing an ID associated with a list of addresses
    * 
    * @return an array of tuple (Address, ID) 
    */
   
   def unwrapListStringNumber(data:(Long,List[String])): Array[(String,Long)] ={
    val dim = data._2.length 
    val arrayString = data._2.toArray
    val tupleArray:Array[(String,Long)] = new Array[(String,Long)](dim)
    for(i <-0 to dim-1){
       tupleArray(i) = (arrayString(i),data._1)
    }
    tupleArray;
    }
  
   /**
		* Extract from a bitcoin block all the transaction inside
		* 
    * @param bitcoinBlock 
    * 
    * @return an array containing all the transaction inside the block
    */
   
   def extractTransactionData(bitcoinBlock: BitcoinBlock): Array[Transaction] = {

    val transactionCount= bitcoinBlock.getTransactions().size()
		var resultSize=0
		for (i<-0 to transactionCount-1) {
			resultSize += bitcoinBlock.getTransactions().get(i).getListOfInputs().size()*bitcoinBlock.getTransactions().get(i).getListOfOutputs().size()
		}

		val transactions:Array[Transaction]=new Array[Transaction](resultSize)
		var resultCounter: Int = 0
		for (i <- 0 to transactionCount-1) { 
				val currentTx=bitcoinBlock.getTransactions().get(i)
				val currentTxHash=BitcoinUtil.reverseByteArray(BitcoinUtil.getTransactionHash(currentTx))
			  for (j <-0 to  currentTx.getListOfInputs().size()-1) { 
				  val currentTxInput=currentTx.getListOfInputs().get(j)
				  val currentTxInputHash=BitcoinUtil.reverseByteArray(currentTxInput.getPrevTransactionHash())
				  val currentTxInputIndex=currentTxInput.getPreviousTxOutIndex()
				  for (k <-0 to currentTx.getListOfOutputs().size()-1) {
					  val currentTxOutput=currentTx.getListOfOutputs().get(k)
					  val currentTxOutputIndex= k.toLong
					  val currentDestination = BitcoinScriptPatternParser.getPaymentDestination(currentTxOutput.getTxOutScript())
 					  val currentAmount= currentTxOutput.getValue()
					  transactions(resultCounter)=new Transaction(currentTxHash,currentTxOutputIndex,currentTxInputHash,currentTxInputIndex,currentDestination,currentAmount)
					  resultCounter+=1
				    }
		  	}
		}
		transactions;
  }
}
