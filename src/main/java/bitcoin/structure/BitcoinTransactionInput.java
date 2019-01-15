package bitcoin.structure;

import java.io.Serializable;

public class BitcoinTransactionInput implements Serializable {

/**
	 * 
	 */
	private static final long serialVersionUID = 283893453089295979L;
	
private byte[] prevTransactionHash;
private long previousTxOutIndex;
private byte[] txInScriptLength;
private byte[] txInScript;
private long seqNo;

public BitcoinTransactionInput(byte[] prevTransactionHash, long previousTxOutIndex, byte[] txInScriptLength, byte[] txInScript, long seqNo) {
	this.prevTransactionHash=prevTransactionHash;
	this.previousTxOutIndex=previousTxOutIndex;
	this.txInScriptLength=txInScriptLength;
	this.txInScript=txInScript;
	this.seqNo=seqNo;
}

public byte[] getPrevTransactionHash() {
	return this.prevTransactionHash;
}

public long getPreviousTxOutIndex() {
	return this.previousTxOutIndex;
}

public byte[] getTxInScriptLength() {
	return this.txInScriptLength;
}

public byte[] getTxInScript() {
	return this.txInScript;
}

public long getSeqNo() {
	return this.seqNo;
}

}
