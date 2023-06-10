package com.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.Map;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

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
        Path inPath = new Path( args[ 0 ] );
        Path profitPath = new Path( args[1] + "Price" );
        Path countriesPath = new Path( args[1] + "Country" );

        System.out.println( "starting job1 with input: " + inPath + " and output as: " + profitPath );

		Configuration conf = new Configuration();
		Job job = Job.getInstance( conf, "Month Profit" );
		job.setJarByClass( App.class );
		job.setMapperClass( MonthProfitMapper.class );
		//job.setCombinerClass( IntSumReducer.class );
		job.setReducerClass( IntSumReducer.class );
		job.setOutputKeyClass( Text.class );
		job.setOutputValueClass( FloatWritable.class );
		FileInputFormat.addInputPath( job, inPath );
		FileOutputFormat.setOutputPath( job, profitPath );
		// System.exit( job.waitForCompletion( true ) ? 0 : 1 );
        job.waitForCompletion( true );


        System.out.println( "starting job2 with input: " + inPath + " and output as: " + countriesPath );

		Job job2 = Job.getInstance( conf, "Month Profit" );
		job2.setJarByClass( App.class );
		job2.setMapperClass( CountryProfitMapper.class );
		// job2.setCombinerClass( IntSumReducer.class );
		job2.setReducerClass( IntSumReducer.class );
		job2.setOutputKeyClass( Text.class );
		job2.setOutputValueClass( FloatWritable.class );
        
		FileInputFormat.addInputPath( job2, inPath );
		FileOutputFormat.setOutputPath( job2, countriesPath );
		System.exit( job2.waitForCompletion( true ) ? 0 : 1 );
	}

    public static class MonthProfitMapper extends Mapper<Object, Text, Text, FloatWritable> {
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

    public static class CountryProfitMapper extends Mapper<Object, Text, Text, FloatWritable> {
        private Text countryText  = new Text(); 
        private FloatWritable profit = new FloatWritable();

        public void map( Object key, Text value, Context context ) throws IOException, InterruptedException {
            // Split the CSV line into columns
            String[] columns = value.toString().split(",");
            // Extract relevant columns
            String bookingStatus = columns[0];
            try{
                String country = columns[6];
                float price = Float.parseFloat(columns[7]);
                // filter out record with no country
                if( !country.isBlank() && !country.isEmpty() ){
                    // Filter out non-booked entries
                    if ( bookingStatus.equals( "0" ) ) {
                        // Emit the month and profit as key-value pairs
                        countryText.set( country );
                        profit.set( price );
                        context.write( countryText, profit );
                    }
                }
            } catch( NumberFormatException e ){
                // ignore
                // Need this because the csv has a header line which it attempts to read so when converting to a float it fails to here
            }
        }
    }
    
    public static class IntSumReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
        private ArrayList< Map.Entry< Text, Float> > sortme = new ArrayList<>();

		public void reduce( Text key, Iterable<FloatWritable> values, Context context ) throws IOException, InterruptedException {
			// Calculate the total profit for each month
            float sum = 0;
            for ( FloatWritable value : values ) {
                // Sum up all the intermediate values from the mapper
                sum += value.get();
            }
            
            //send the list so i can sort by profit
            Entry<Text, Float> e = new AbstractMap.SimpleEntry<>(new Text(key), sum );
            sortme.add( e );
        }

        //ones once on end of task
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Collections.sort( sortme, new FloatCompare() ) ;
            for ( Entry<Text, Float> entry : sortme ) {
                float profit = entry.getValue();
                Text month = entry.getKey();
                context.write(month, new FloatWritable(profit)); //yeet to file easy dubs
            }
        }
	}

    private static class FloatCompare implements Comparator< Entry<Text, Float> > {
        @Override
        public int compare( Entry<Text, Float> f1, Entry<Text, Float> f2 ) {
            return f2.getValue().compareTo( f1.getValue() );
        }
    }
}
