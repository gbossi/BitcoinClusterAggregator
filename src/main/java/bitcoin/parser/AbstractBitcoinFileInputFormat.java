package bitcoin.parser;


import java.io.IOException;



import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


public abstract class AbstractBitcoinFileInputFormat<K,V> extends FileInputFormat<K,V>   {
public static final String CONF_ISSPLITABLE="hadoopcryptoledeger.bitcoinblockinputformat.issplitable";
public static final boolean DEFAULT_ISSPLITABLE=false;



@Override
public abstract RecordReader<K,V> createRecordReader(InputSplit split, TaskAttemptContext ctx) throws IOException;

/**
*
* This method is experimental and derived from TextInputFormat. It is not necessary and not recommended to compress the blockchain files. Instead it is recommended to extract relevant data from the blockchain files once and store them in a format suitable for analytics (including compression), such as ORC or Parquet.
*
*/

@Override
  protected boolean isSplitable(JobContext context, Path file) {
    boolean isSplitable=context.getConfiguration().getBoolean(AbstractBitcoinFileInputFormat.CONF_ISSPLITABLE,AbstractBitcoinFileInputFormat.DEFAULT_ISSPLITABLE);
    if (!(isSplitable)) {
		return false;
    }   
   final CompressionCodec codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
    if (null == codec) {
      return true;
    }
    return codec instanceof SplittableCompressionCodec;
  }




}
