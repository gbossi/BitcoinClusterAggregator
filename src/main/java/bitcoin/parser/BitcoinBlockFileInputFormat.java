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
