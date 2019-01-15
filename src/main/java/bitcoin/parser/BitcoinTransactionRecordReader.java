package bitcoin.parser;



import bitcoin.exception.*;

import java.io.IOException;


import org.apache.hadoop.io.BytesWritable; 

import org.apache.hadoop.conf.Configuration;


import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

import bitcoin.structure.*;


public class BitcoinTransactionRecordReader extends AbstractBitcoinRecordReader<BytesWritable, BitcoinTransaction> {
private static final Log LOG = LogFactory.getLog(BitcoinBlockRecordReader.class.getName());

private int currentTransactionCounterInBlock=0;
private BitcoinBlock currentBitcoinBlock;
private BytesWritable currentKey=new BytesWritable();
private BitcoinTransaction currentValue=new BitcoinTransaction();



public BitcoinTransactionRecordReader(Configuration conf) throws HadoopCryptoLedgerConfigurationException {
	super(conf);
}

/**
*
*  get current key after calling next()
*
* @return key is is a 68 byte array (hashMerkleRoot, prevHashBlock, transActionCounter)
*/
@Override
public BytesWritable getCurrentKey() {
	return this.currentKey;
}

/**
*
*  get current value after calling next()
*
* @return value is a deserialized Java object of class BitcoinTransaction
*/
@Override
public BitcoinTransaction getCurrentValue() {
	return this.currentValue;
}


/**
*
* Read a next block. 
*
*
* @return true if next block is available, false if not
*/
@Override
public boolean nextKeyValue() throws IOException {
	// read all the blocks, if necessary a block overlapping a split
	while(getFilePosition()<=getEnd()) { // did we already went beyond the split (remote) or do we have no further data left?
		if ((currentBitcoinBlock==null) || (currentBitcoinBlock.getTransactions().size()==currentTransactionCounterInBlock)){
			try {
				currentBitcoinBlock=getBbr().readBlock();
				currentTransactionCounterInBlock=0;
			} catch (BitcoinBlockReadException e) {
				// log
				LOG.error(e);
			}
		}

		if (currentBitcoinBlock==null) {
			return false;
		}
		BitcoinTransaction currentTransaction=currentBitcoinBlock.getTransactions().get(currentTransactionCounterInBlock);
		// the unique identifier that is linked in other transaction is usually its hash
		byte[] newKey = BitcoinUtil.getTransactionHash(currentTransaction);
		this.currentKey.set(newKey,0,newKey.length);
		this.currentValue.set(currentTransaction);
		currentTransactionCounterInBlock++;
		return true;
	}
	return false;
}



}
