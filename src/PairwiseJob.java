import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


/**
 * Creates an affinity matrix using the block approach described in section 5.2:
 * http://dl.acm.org/citation.cfm?id=1851595 . This matrix is meant for use
 * in spectral clustering as a proof-of-concept.
 * 
 * @author Shannon Quinn
 */
public class PairwiseJob {

	public static final String BLOCKING_FACTOR = "squinn.blocking.factor";
	public static final String DATASET_SIZE = "squinn.dataset.size";

	public static void main(String[] args) {
        if (args.length != 4) {
            System.out.println("[input] [v] [h] [output]");
            System.exit(-1);
        }
        try {
            Configuration conf = new Configuration();
            Path input = new Path(args[0]);
            long v = Long.parseLong(args[1]);
            int h = Integer.parseInt(args[2]);
            Path output = new Path(args[3]);
            FileSystem fs = output.getFileSystem(conf);
            if (fs.exists(output)) {
                fs.delete(output, true);
            }
            conf.setInt(PairwiseJob.BLOCKING_FACTOR, h);
            conf.setLong(PairwiseJob.DATASET_SIZE, v);
            Job job = Job.getInstance(conf);
            

            job.setMapperClass(BlockMapper.class);
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(DataPointWritable.class);

            job.setReducerClass(BlockReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            job.setOutputFormatClass(TextOutputFormat.class);
            FileOutputFormat.setOutputPath(job, output);

            job.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.setInputPaths(job, input);

            job.setJarByClass(PairwiseJob.class);
            job.setNumReduceTasks(10);

            job.waitForCompletion(true);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace(System.err);
            System.exit(1);
        }

        System.exit(0);

	}
	
	static class BlockMapper extends Mapper<LongWritable, Text, IntWritable, DataPointWritable> {
		private long v;
		private int h;
		private IntWritable outKey;
		private DataPointWritable outVal;
		
        protected void setup(Context context) throws IOException, InterruptedException {
        	v = context.getConfiguration().getLong(PairwiseJob.DATASET_SIZE, Long.MAX_VALUE);
        	h = context.getConfiguration().getInt(PairwiseJob.BLOCKING_FACTOR, 100);
        	outKey = new IntWritable();
        	outVal = new DataPointWritable();
        }

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        	// Break the value up into ID + data point.
        	String [] elements = value.toString().split("\\s+");
        	long id = Long.parseLong(elements[0]);
        	outVal.setValue(new Text(elements[1]));
        	outVal.setid(id);
			
			// Imagine setting up a v-by-v pairwise matrix, and splitting it
			// according to the blocking factor h. In order to determine which
			// subsets this data point belongs to, we'll start at row 0 of the
			// column corresponding to this data point, continuing until we
			// hit the diagonal. Then we'll stay at that row and move out along
			// the columns. The whole way, we'll emit any new subsets we enter.
			
        	int edgelength = (int)Math.ceil((double)this.v / (double)this.h);
        	int I = (int)(id / edgelength);
        	int J;
        	// Loop down towards the diagonal.
        	outVal.setType(new Text(DataPointWritable.COL));
        	for (J = 0; J < (this.h / 2) + 1; ++J) {
        		int p = (((I + 1) * I) / 2) + J;
        		outKey.set(p);
        		context.write(outKey, outVal);
        	}
        	
        	// Now loop out towards the edge.
        	outVal.setType(new Text(DataPointWritable.ROW));
        	for (; I < this.h; ++I) {
        		int p = (((I + 1) * I) / 2) + J;
        		outKey.set(p);
        		context.write(outKey, outVal);
        	}
        }
	}
	
	static class BlockReducer extends Reducer<IntWritable, DataPointWritable, Text, Text> {
		
		public static final double SIGMA = 1.0;

		protected void reduce(IntWritable key, Iterable<DataPointWritable> values, Context context) throws IOException, InterruptedException {
			// This should contain all the points in block "key".
			ArrayList<DataPointWritable> rows = new ArrayList<DataPointWritable>();
			ArrayList<DataPointWritable> cols = new ArrayList<DataPointWritable>();
			for (DataPointWritable e : values) {
				String type = e.getType().toString();
				DataPointWritable toAdd = new DataPointWritable();
				toAdd.setid(e.getid());
				toAdd.setValue(e.getValue());
				if (type.equals(DataPointWritable.ROW)) {
					rows.add(toAdd);
				} else {
					cols.add(toAdd);
				}
			}
			
			// Now do a double loop.
			for (int i = 0; i < rows.size(); ++i) {
				DataPointWritable k = rows.get(i);
				for (int j = 0; j < cols.size(); ++j) {
					DataPointWritable d = cols.get(j);
					// Check if we're not along the diagonal.
					if (k.getid() < d.getid()) { 
						double distance = evaluate(k, d);
						// Output twice.
						context.write(new Text(String.format("(%s, %s)", k.getid(), d.getid())), new Text("" + distance));
						context.write(new Text(String.format("(%s, %s)", d.getid(), k.getid())), new Text("" + distance));
					} // else do nothing, because it'd be redundant
				}
			}
		}
		
		/**
		 * This can be whatever you want it to be.
		 * @param a Data point 1.
		 * @param b Data point 2.
		 * @return Some measure of distance between them.
		 */
		public static double evaluate(DataPointWritable a, DataPointWritable b) {
			String [] p1 = a.getValue().toString().split(",");
			String [] p2 = b.getValue().toString().split(",");
			double sum = 0.0;
			for (int i = 0; i < p1.length; ++i) {
				sum += Math.pow(Double.parseDouble(p1[i]) - Double.parseDouble(p2[i]), 2);
			}
			return Math.exp(Math.sqrt(sum) / (-2 * Math.pow(BlockReducer.SIGMA, 2)));
		}
	}

}
