package main.scala
import java.math.BigInteger;


class Transaction(entID: Array[Byte],entNum:Long,fromID: Array[Byte],prevNum:Long,address:String, btAmount: BigInteger) extends Serializable {
  var txID:Array[Byte]        = entID
  var txIndex:Long            = entNum
  var prevtxID:Array[Byte]    = fromID
  var prevtxIndex:Long        = prevNum
  var btAddress:String        = address
  var amount:BigInteger       = btAmount
  
  def getTxID:Array[Byte] = {
    return txID;
  }
  
  def getTxIndex:Long ={
    return txIndex;
  }
  
  def getPrevTxID:Array[Byte] ={
    return prevtxID;
  }
  
  def getPrevTxIndex:Long = {
    return prevtxIndex;
  }
  
  def getbtAddress:String = {
    return btAddress;
  }
  
  def getAmount:BigInteger = {
    return amount;
  }
  
  override def toString():String={
    return "[TransactionID= " + txID +" Index= "+ txIndex + ", fromTx= " + prevtxID + " Index= " + prevtxIndex + ", transferred to Address= " + btAddress+ " Amount of "+ amount + " Satoshis";
  }
  
}
