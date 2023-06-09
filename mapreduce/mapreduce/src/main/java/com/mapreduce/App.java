package com.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance( conf, "word count" );
		job.setJarByClass( App.class );
		job.setMapperClass( TokenizerMapper.class );
		job.setCombinerClass( IntSumReducer.class );
		job.setReducerClass( IntSumReducer.class );
		job.setOutputKeyClass( Text.class );
		job.setOutputValueClass( FloatWritable.class );
        System.out.println( args[0] );
		FileInputFormat.addInputPath( job, new Path( args[ 0 ] ) );
		FileOutputFormat.setOutputPath( job, new Path( args[ 1 ] ) );
		System.exit( job.waitForCompletion( true ) ? 0 : 1 );
	}

    public static class TokenizerMapper extends Mapper<Object, Text, Text, FloatWritable> {
		private Text month = new Text();
        private FloatWritable profit = new FloatWritable();

		public void map( Object key, Text value, Context context ) throws IOException, InterruptedException {
            // Split the CSV line into columns
            String[] columns = value.toString().split(",");

			// Extract relevant columns
            String bookingStatus = columns[0];
            try{
                String arrivalMonth = columns[2];
                float price = Float.parseFloat(columns[7]);
                // Filter out non-booked entries
                if ( bookingStatus.equals( "0" ) ) {
                    // Emit the month and profit as key-value pairs
                    month.set( arrivalMonth );
                    profit.set( price );
                    context.write( month, profit );
                }
            } catch( NumberFormatException e ){
                // ignore
                // Need this because the csv has a header line which it attempts to read so when converting to a float it fails to here
            }
		}
	}

    public static class IntSumReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
		private FloatWritable totalProfit = new FloatWritable();

		public void reduce( Text key, Iterable<FloatWritable> values, Context context ) throws IOException, InterruptedException {
			// Calculate the total profit for each month
            float sum = 0;
            for ( FloatWritable value : values ) {
                // Sum up all the intermediate values from the mapper
                sum += value.get();
            }
            totalProfit.set( sum );
      
            // Emit the month and total revenue as key-value pairs
            context.write( key, totalProfit );
        }
	}
}
