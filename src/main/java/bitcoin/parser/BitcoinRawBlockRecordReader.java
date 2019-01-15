/**
* Copyright 2016 ZuInnoTe (Jörn Franke) <zuinnote@gmail.com>
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
**/

package bitcoin.parser;



import bitcoin.exception.*;

import java.io.IOException;

import java.nio.ByteBuffer;

import org.apache.hadoop.io.BytesWritable; 

import org.apache.hadoop.conf.Configuration;


import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;




public class BitcoinRawBlockRecordReader  extends AbstractBitcoinRecordReader<BytesWritable, BytesWritable> {
private static final Log LOG = LogFactory.getLog(BitcoinRawBlockRecordReader.class.getName());
private BytesWritable currentKey=new BytesWritable();
private BytesWritable currentValue=new BytesWritable();

public BitcoinRawBlockRecordReader(Configuration conf) throws HadoopCryptoLedgerConfigurationException {
	super(conf);
}



/**
*
*  get current key after calling next()
*
* @return key is a 64 byte array (hashMerkleRoot and prevHashBlock)
*/
@Override
public BytesWritable getCurrentKey() {
	return this.currentKey;
}


/**
*
*  get current value after calling next()
*
* @return value is a deserialized Java object of class BitcoinBlock
*/
@Override
public BytesWritable getCurrentValue() {
	return this.currentValue;
}



/**
*
* Read a next block. 
*
* @return true if next block is available, false if not
*/
@Override
public boolean nextKeyValue() throws IOException {
	// read all the blocks, if necessary a block overlapping a split
	while(getFilePosition()<=getEnd()) { // did we already went beyond the split (remote) or do we have no further data left?
		ByteBuffer dataBlock=null;
		try {
			dataBlock=getBbr().readRawBlock();
		} catch (BitcoinBlockReadException e) {
			// log
			LOG.error(e);
		}	
		if (dataBlock==null) {
			return false;
		}
		byte[] newKey=getBbr().getKeyFromRawBlock(dataBlock);
		this.currentKey.set(newKey,0,newKey.length);
		byte[] dataBlockArray;
		if (dataBlock.hasArray()) {
			dataBlockArray=dataBlock.array();
		} else {
			dataBlockArray=new byte[dataBlock.capacity()];
			dataBlock.get(dataBlockArray);
		}
		this.currentValue.set(dataBlockArray,0,dataBlockArray.length);
		return true;
	}
	return false;
}


}
