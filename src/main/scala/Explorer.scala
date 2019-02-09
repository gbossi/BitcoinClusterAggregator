import main.scala._

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.hadoop.conf._
import scala.math.BigInt

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.io._
import scala.collection.JavaConverters._

import bitcoin.structure._
import bitcoin.parser._

object Explorer {
   def main(args: Array[String]): Unit = {
     val conf = new SparkConf().setAppName("Bitcoin Explorer")
     val sc= new SparkContext(conf)
     val hadoopConf = new Configuration()
     hadoopConf.set("hadoopcryptoledger.bitcoinblockinputformat.filter.magic","F9BEB4D9")
     parser(sc,hadoopConf,args(0),args(1))
     sc.stop()
     }

   def parser(sc: SparkContext, hadoopConf: Configuration, inputFile: String, outputFile: String): Unit = {
     val bitcoinBlocksRDD = sc.newAPIHadoopFile(inputFile, classOf[BitcoinBlockFileInputFormat], classOf[BytesWritable], classOf[BitcoinBlock], hadoopConf)
     //Parse the blockchain and retrieve all the transaction inside
     //Transaction(PreviousID,PreviousIndex,CurrentID,CurrentIndex,BitcoinAddress,Amount)
     val bitcoinTransactionRDD = bitcoinBlocksRDD.flatMap(hadoopKeyValueTuple => extractTransactionData(hadoopKeyValueTuple._2))

     //Prepare the unwrapping of the transactions
     bitcoinTransactionRDD.persist()
     
     //Map the transactions using as key (PreviousId,PreviousIndex)
     val mappedPrevTransaction = bitcoinTransactionRDD.map(f => ((f.getPrevTxID.+(f.getPrevTxIndex)),f))
     //Map the transactions using as key (CurrentId,CurrentIndex)
     val mappedCurrTransaction = bitcoinTransactionRDD.map(f => ((f.getTxID.+(f.getTxIndex)),f))
     
     bitcoinTransactionRDD.unpersist()
     
     
     //Join the transaction using as joining factor key == key -> (PreviousId,PreviousIndex)==(CurrentId,CurrentIndex)
     //(ID_A,IN_A,ID_B,IN_B,Add1,Am1) <-> (ID_B,IN_B,ID_C,IN_C,Add2,Am2)
     val joinTransaction = mappedCurrTransaction.join(mappedPrevTransaction) 
     
     //Create a new key using the common data
     //Group all the exchanges using the Transaction ID_A
     val groupedJoinTransaction = joinTransaction.map(f => (f._2._2.getTxID,f._2)).groupByKey()
     
    
     //Accumulate the interesting information getting the unwrapped Transaction
     //[Index,List(Bitcoin_Address_IN,Bitcoin_Address_OUT,Amount)]
     val tableOfTransaction = groupedJoinTransaction.mapValues(f => extractWalletTransaction(f)).zipWithIndex().map(f => (f._2,f._1._2))

   
     
     //Take all the data about the Input Bitcoin Addresses
     //[Index,List(Bitcoin_Address_IN)]
     tableOfTransaction.persist()
     val inputWallets = tableOfTransaction.mapValues(f => f.map(g => g._1).distinct)
     val justTransaction = tableOfTransaction.flatMap(f => f._2)
     tableOfTransaction.unpersist()
     
     
     
     inputWallets.persist
     val flatWallets = inputWallets.flatMap(unwrapListStringNumber(_))
     
     val aggregateResult = EntityDependency.betterRun(sc, inputWallets, flatWallets,outputFile)  
     inputWallets.unpersist()
     
     //Broadcast the result and be prepared for the broadcasted join
     aggregateResult.persist()
     val flatResult = aggregateResult.flatMap(unwrapListStringNumber(_))

     val broadcastNodes = sc.broadcast(flatResult.collectAsMap)     
     val edges = justTransaction.map(f =>Edge(broadcastNodes.value.get(f._1).getOrElse(-1),broadcastNodes.value.get(f._2).getOrElse(-1),f._3))
         
     //Create the graph
     val graph: Graph[(List[String]), BigInt] = Graph(aggregateResult, edges)
     aggregateResult.unpersist()
     
     graph.vertices.saveAsTextFile(outputFile)
     
   }
   
   def extractWalletTransaction(table : Iterable[(Transaction,Transaction)]): List[(String,String,BigInt)] ={
     //Incoming (ID_A,IN_A,ID_B,IN_B,Add1,Am1) <-> (ID_B,IN_B,ID_C,IN_C,Add2,Am2)
     val ret = table.map(f => (f._1.getBicointAddress,f._2.getBicointAddress,f._2.getAmount)).toList
     //Outgoing Add2 -> send to  -> Add1 an amount of Am1
     ret;
   } 
   
   def unwrapListStringNumber(data:(Long,List[String])): Array[(String,Long)] ={
    val dim = data._2.length 
    val arrayString = data._2.toArray
    val tupleArray:Array[(String,Long)] = new Array[(String,Long)](dim)
    for(i <-0 to dim-1){
       tupleArray(i) = (arrayString(i),data._1)
    }
    tupleArray;
    }
  
   
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
   
    
