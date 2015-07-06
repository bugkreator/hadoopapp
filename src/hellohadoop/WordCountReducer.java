package hellohadoop;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Created by odedrotem on 7/6/15.
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    IntWritable totValue = new IntWritable(0);
    @Override
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context)
            throws IOException, InterruptedException {

        int tot = 0;
        for (IntWritable value : values) {
            tot += value.get();
        }
        totValue.set(tot);
        context.write(key, totValue);
    }
}
