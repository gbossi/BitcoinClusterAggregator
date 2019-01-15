package bitcoin.structure;

import java.io.Serializable;
import java.math.BigInteger;

public class BitcoinTransactionOutput implements Serializable {

/**
	 * 
	 */
	private static final long serialVersionUID = 2854570630540937753L;
	
private BigInteger value;
private byte[] txOutScriptLength;
private byte[] txOutScript;

public BitcoinTransactionOutput(BigInteger value, byte[] txOutScriptLength, byte[] txOutScript) {
	this.value=value;
	this.txOutScriptLength=txOutScriptLength;
	this.txOutScript=txOutScript;
}

public BigInteger getValue() {
	return this.value;
}

public byte[] getTxOutScriptLength() {
	return this.txOutScriptLength;
}

public byte[] getTxOutScript() {
	return this.txOutScript;
}

}
