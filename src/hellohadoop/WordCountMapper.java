package hellohadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import sun.misc.Regexp;


public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    Text outKey = new Text("");
    IntWritable outValue = new IntWritable(1);

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        String[] words = line.split("(\\s)+");

        for (String word: words)
        {
            outKey.set(word);
            context.write(outKey,outValue);
        }
    }
}
