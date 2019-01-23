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
     val sc=new SparkContext(conf)
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
     
     bitcoinTransactionRDD.persist()
     //Prepare the unwrapping of the transactions
     
     //Map the transactions using as key (PreviousId,PreviousIndex)
     val mappedPrevTransaction = bitcoinTransactionRDD.keyBy(f => (f.getPrevTxID.toString(),f.getPrevTxIndex))
     //Map the transactions using as key (CurrentId,CurrentIndex)
     val mappedCurrTransaction = bitcoinTransactionRDD.keyBy(f => (f.getTxID.toString(),f.getTxIndex))
     
     bitcoinTransactionRDD.unpersist()
     
     //Join the transaction using as joining factor key == key -> (PreviousId,PreviousIndex)==(CurrentId,CurrentIndex)
     //(ID_A,IN_A,ID_B,IN_B,Add1,Am1) <-> (ID_B,IN_B,ID_C,IN_C,Add2,Am2)
     val joinTransaction = mappedPrevTransaction.join(mappedCurrTransaction).map(f => (f._2))
     
     //Create a new key using the common data
     //Group all the exchanges using the Transaction ID_C
     val groupedJoinTransaction = joinTransaction.groupBy(f => f._2.getTxID.toString)
     
     //Accumulate the interesting information getting the unwrapped Transaction
     //[Index,List(Bitcoin_Address_IN,Amount,Bitcoin_Address_OUT)]
     val tableOfTransaction = groupedJoinTransaction.map(f => extractWalletTransaction(f._2)).zipWithIndex().map(f => (f._2,f._1))
     
     //FOLLOWING STEPS TO CREATE THE ENTITY
     //Take all the data about the Input Bitcoin Addresses
     //[Index,List(Bitcoin_Address_IN)]
     val intermediateEntity = tableOfTransaction.map(f => (f._1,f._2.map(g => g._1)))
     
     //Compute a Cartesian product between itself
     //[Index,List(Bitcoin_Address_IN)][Index,List(Bitcoin_Address_IN)]
     val catesianProduct = intermediateEntity.cartesian(intermediateEntity)
     
     //Compute the intersection over the cartesian product
     val mapCartesianProduct = catesianProduct.map(a => if(a._1._2.intersect(a._2._2).isEmpty) a._1 else (a._1._1,a._1._2.union(a._2._2))) 
     
     //Distint all the Bitcoin_Addresses
     val aggregateResult = mapCartesianProduct.reduceByKey(_ union _).map(f => (f._1,f._2.distinct))
        
     aggregateResult.saveAsTextFile(outputFile)
     }
   
   def extractWalletTransaction(table : Iterable[(Transaction,Transaction)]): List[(String,BigInt,String)] ={
     val ret = table.map(f => (f._1.getBicointAddress,f._1.getAmount,f._2.getBicointAddress)).toList
     ret;
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
				val currentTxHash=BitcoinUtil.getTransactionHash(currentTx)
			  for (j <-0 to  currentTx.getListOfInputs().size()-1) { 
				  val currentTxInput=currentTx.getListOfInputs().get(j)
				  val currentTxInputHash=currentTxInput.getPrevTransactionHash()
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
   
    
