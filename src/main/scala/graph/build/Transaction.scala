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


import java.math.BigInteger
import scala.BigInt


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
