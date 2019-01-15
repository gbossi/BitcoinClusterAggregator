import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.hadoop.conf._

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.io._

import bitcoin.structure.BitcoinBlock
import bitcoin.parser._

object Explorer {
   def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Bitcoin Explorer")
        val sc=new SparkContext(conf)
        val hadoopConf = new Configuration()
        hadoopConf.set("hadoopcryptoledger.bitcoinblockinputformat.filter.magic","F9BEB4D9")
        jobTotalNumOfTransactions(sc,hadoopConf,args(0),args(1))
        sc.stop()
        }

   def jobTotalNumOfTransactions(sc: SparkContext, hadoopConf: Configuration, inputFile: String, outputFile: String): Unit = {
      val bitcoinBlocksRDD = sc.newAPIHadoopFile(inputFile, classOf[BitcoinBlockFileInputFormat], classOf[BytesWritable], classOf[BitcoinBlock], hadoopConf)
      val totalCount=transform(bitcoinBlocksRDD)
    	 // write results to HDFS
      totalCount.repartition(1).saveAsTextFile(outputFile)
		  }

   def transform(bitcoinBlocksRDD: RDD[(BytesWritable,BitcoinBlock)]): RDD[(String,Int)] = {
       // extract the no transactions / block (map)
       val noOfTransactionPair = bitcoinBlocksRDD.map(hadoopKeyValueTuple => ("No of transactions: ",hadoopKeyValueTuple._2.getTransactions().size()))
       // reduce total count
       val totalCount = noOfTransactionPair.reduceByKey ((c,d) => c+d)
       totalCount
       }
}


