package com.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * CS236 Project MapReduce Code
 *
 */
public class App 
{

    // Write month and market segment type to output
    private static final String[] months = { "January", "February", "March", "April", "May",
            "June", "July", "August", "September", "October",
            "November", "December"};
    public static void main( String[] args ) throws Exception {
        Path inPath = new Path( args[ 0 ] );
        Path profitPathOut = new Path( args[1] + "Price" );
        Path marketPathOut = new Path( args[1] + "MarketSegment" );

        // Job 1 Configuration
        System.out.println("--------------");
        System.out.println( "Starting month profit job with input from '" + inPath
                            + "' and output to '" + profitPathOut + "'.");

		Configuration conf = new Configuration();
		Job job = Job.getInstance( conf, "Month Profit" );
		job.setJarByClass( App.class );

		job.setMapperClass( MonthProfitMapper.class );
		job.setReducerClass( ProfitReducer.class );
		job.setOutputKeyClass( Text.class );
		job.setOutputValueClass( DoubleWritable.class );

		FileInputFormat.addInputPath( job, inPath );
		FileOutputFormat.setOutputPath( job, profitPathOut );
        job.waitForCompletion( true );

        // Job 2 Configuration
        System.out.println("--------------");
        System.out.println( "Starting market segment job with input from '" + inPath
                            + "' and output to '" + marketPathOut + "'.");

		Job job2 = Job.getInstance( conf, "Month Market Segment" );
		job2.setJarByClass( App.class );
		job2.setMapperClass( PaymentMethodMapper.class );
		job2.setReducerClass( PaymentMethodReducer.class );
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);
		job2.setOutputKeyClass( Text.class );
		job2.setOutputValueClass( Text.class );
        
		FileInputFormat.addInputPath( job2, inPath );
		FileOutputFormat.setOutputPath( job2, marketPathOut );
		System.exit( job2.waitForCompletion( true ) ? 0 : 1 );
	}

    public static class MonthProfitMapper extends Mapper<Object, Text, Text, DoubleWritable> {
		public void map( Object key, Text value, Context context ) throws IOException, InterruptedException {
            // Split the CSV line into columns
            String[] columns = value.toString().split(",");

			// Extract relevant columns
            String bookingStatus = columns[0];
            // Check if booking status is valid
            if (!bookingStatus.equals("0"))
                return;

            String arrivalYear = columns[1];
            String arrivalMonth = columns[2];
            double price = Double.parseDouble(columns[7]);
            // Check if price is valid
            if (price <= 0)
                return;

            context.write(new Text(arrivalYear + "-" + arrivalMonth), new DoubleWritable(price));
		}
	}

    public static class ProfitReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private final List< Entry< Text, Double> > monthlyProfits = new ArrayList<>();

		public void reduce( Text key, Iterable<DoubleWritable> values, Context context ) {
			// Calculate the total profit for each month
            double sum = 0;
            for ( DoubleWritable value : values ) {
                // Sum up all the intermediate values from the mapper
                sum += value.get();
            }
            
            // Add the monthly profit to the list for sorting later
            monthlyProfits.add( new AbstractMap.SimpleEntry<>(new Text(key), sum ) );
        }

        // Sort the profits by value
        protected void cleanup(Context context) throws IOException, InterruptedException {
            monthlyProfits.sort((e1, e2) -> e2.getValue().compareTo( e1.getValue() ));

            for ( Entry<Text, Double> entry : monthlyProfits ) {
                String[] keys = entry.getKey().toString().split("-");
                String year = keys[0];
                int monthIdx = Integer.parseInt(keys[1]) - 1;

                double profit = entry.getValue();

                context.write(new Text(year + "-" + months[monthIdx]), new DoubleWritable(profit));
            }
        }
	}

    public static class PaymentMethodMapper extends Mapper<Object, Text, Text, IntWritable> {
        public void map( Object key, Text value, Context context ) throws IOException, InterruptedException {
            // Split the CSV line into columns
            String[] columns = value.toString().split(",");

            // Extract relevant columns
            String bookingStatus = columns[0];
            if (!bookingStatus.equals("0"))
                return;

            String arrivalMonth = columns[2];
            String marketSegType = columns[5];
            context.write(new Text(arrivalMonth + ":" + marketSegType), new IntWritable(1));
        }
    }

    public static class PaymentMethodReducer extends Reducer<Text, IntWritable, Text, Text> {
        private final List<Map.Entry<String, Integer>> paymentMethod = new ArrayList<>(Collections.nCopies(12, null));

        public void reduce( Text key, Iterable<IntWritable> values, Context context ) {
            String[] keys = key.toString().split(":");
            final int monthIdx = Integer.parseInt(keys[0]) - 1;
            final String segmentType = keys[1];

            // Calculate the total market segment for that type
            int marketSegmentTotal = 0;
            for (IntWritable value : values) {
                marketSegmentTotal += value.get();
            }

            // Check if market segment total is greater than existing
            // market segment for that month.
            boolean insertSegment = true;
            if (paymentMethod.get(monthIdx) != null) {
                Map.Entry<String, Integer> entry = paymentMethod.get(monthIdx);
                if (entry.getValue() >= marketSegmentTotal) {
                    insertSegment = false;
                }
            }

            if (insertSegment) {
                paymentMethod.set(monthIdx, new AbstractMap.SimpleEntry<>(segmentType, marketSegmentTotal) );
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (int monthIdx = 0; monthIdx < 12; monthIdx++) {
                Text month = new Text(months[monthIdx]);

                Map.Entry<String, Integer> entry = paymentMethod.get(monthIdx);
                if (entry == null)
                    continue;

                Text marketSegmentType = new Text(entry.getKey());

                context.write(month, marketSegmentType);
            }
        }
    }
}
