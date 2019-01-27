package main.scala
import java.math.BigInteger;


class Transaction(entID: Array[Byte],entNum:Long,fromID: Array[Byte],prevNum:Long,address:String, btAmount: BigInteger) extends Serializable {
  var txID:String             = entID.map(x => "%02X" format x).mkString
  var txIndex:Long            = entNum
  var prevTxID:String         = fromID.map(x => "%02X" format x).mkString
  var prevTxIndex:Long        = prevNum
  var bitcoinAddress:String   = address
  var amount:BigInt           = BigInt(btAmount)
  
  def getTxID:String = {
    return txID;
  }
  
  def getTxIndex:Long ={
    return txIndex;
  }
  
  def getPrevTxID:String ={
    return prevTxID;
  }
  
  def getPrevTxIndex:Long = {
    return prevTxIndex;
  }
  
  def getBicointAddress:String = {
    return bitcoinAddress;
  }
  
  def getAmount:BigInt = {
    return amount;
  }
  
  override def toString():String={
    return "[TransactionID= " + txID +" Index= "+ txIndex + ", fromTx= " + prevTxID + " Index= " + prevTxIndex + ", transferred to Address= " + bitcoinAddress+ " Amount of "+ amount + " Satoshis";
  }
  
}
