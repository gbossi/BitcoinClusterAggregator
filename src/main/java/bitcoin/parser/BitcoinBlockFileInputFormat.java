/**
* Copyright 2017 ZuInnoTe (Jörn Franke) <zuinnote@gmail.com>
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



import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

import bitcoin.structure.*;

public class BitcoinBlockFileInputFormat extends AbstractBitcoinFileInputFormat<BytesWritable,BitcoinBlock>   {

private static final Log LOG = LogFactory.getLog(BitcoinBlockFileInputFormat.class.getName());

@Override
public RecordReader<BytesWritable,BitcoinBlock> createRecordReader(InputSplit split, TaskAttemptContext ctx) throws IOException {	
	/** Create reader **/
	try {
		return new BitcoinBlockRecordReader(ctx.getConfiguration());
	} catch (HadoopCryptoLedgerConfigurationException e) {
		// log
		LOG.error(e);
	}
	return null;
}


}
