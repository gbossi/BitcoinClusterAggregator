import main.scala._

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.hadoop.conf._

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
     val bitcoinTransactionTuples = bitcoinBlocksRDD.flatMap(hadoopKeyValueTuple => extractTransactionData(hadoopKeyValueTuple._2))
     // write results to HDFS
     bitcoinTransactionTuples.repartition(1).saveAsTextFile(outputFile)
     }
   

   
   
   def extractTransactionData(bitcoinBlock: BitcoinBlock): Array[Transaction] = {

     val transactionCount= bitcoinBlock.getTransactions().size()
		var resultSize=0
		for (i<-0 to transactionCount-1) {
			resultSize += bitcoinBlock.getTransactions().get(i).getListOfInputs().size()*bitcoinBlock.getTransactions().get(i).getListOfOutputs().size()
		}

		val transactions:Array[Transaction]=new Array[Transaction](resultSize)
		var resultCounter: Int = 0
		for (i <- 0 to transactionCount-1) { // for each transaction
				val currentTx=bitcoinBlock.getTransactions().get(i)
				val currentTxHash=BitcoinUtil.getTransactionHash(currentTx)
			  for (j <-0 to  currentTx.getListOfInputs().size()-1) { // for each input
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


