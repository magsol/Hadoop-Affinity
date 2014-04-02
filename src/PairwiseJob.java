import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
        if (args.length != 2) {
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
            job.setMapOutputKeyClass(LongWritable.class);
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
	
	static class BlockMapper extends Mapper<LongWritable, Text, LongWritable, DataPointWritable> {
		private long v;
		private int h;
		private LongWritable outKey;
		private DataPointWritable outVal;
		
        protected void setup(Context context) throws IOException, InterruptedException {
        	v = context.getConfiguration().getLong(PairwiseJob.DATASET_SIZE, Long.MAX_VALUE);
        	h = context.getConfiguration().getInt(PairwiseJob.BLOCKING_FACTOR, 100);
        	outKey = new LongWritable();
        	outVal = new DataPointWritable();
        }

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        	// Break the value up into ID + data point.
        	String [] elements = value.toString().split("\t");
        	long id = Long.parseLong(elements[0]);
        	outVal.setValue(new Text(elements[1]));
        	outVal.seti(id);
			
			// Imagine setting up a v-by-v pairwise matrix, and splitting it
			// according to the blocking factor h. In order to determine which
			// subsets this data point belongs to, we'll start at row 0 of the
			// column corresponding to this data point, continuing until we
			// hit the diagonal. Then we'll stay at that row and move out along
			// the columns. The whole way, we'll emit any new subsets we enter.
			
			long row = 0;
			int edgelength = (int)Math.ceil((double)this.v / (double)this.h);
			outVal.setI((int)(id / edgelength));
			for (; row < id; row += edgelength) {
				outKey.set((id * (id - 1) / 2) + row);
				outVal.setj(row);
				outVal.setJ((int)(row / edgelength));
				context.write(outKey, outVal);
			}
			for (long col = id + 1; col < this.v; col += edgelength) {
				outKey.set((col * (col - 1) / 2) + row);
				outVal.seti(col);
				outVal.setI((int)(col / edgelength));
				context.write(outKey, outVal);
			}
        }
	}
	
	static class BlockReducer extends Reducer<LongWritable, DataPointWritable, Text, Text> {
		
		public static final double SIGMA = 1.0;

		protected void reduce(LongWritable key, Iterable<DataPointWritable> values, Context context) throws IOException, InterruptedException {
			// This should contain all the points in block "key".
			for (DataPointWritable d : values) {
				for (DataPointWritable k : values) {
					// Check if we're not along the diagonal.
					if (k.geti() < d.geti()) { 
						double distance = evaluate(k, d);
						context.write(new Text(String.format("%s:%s", k.geti(), k.getValue().toString())),
								new Text(String.format("%s:%s:%s ", d.geti(), d.getValue().toString(), distance)));
						context.write(new Text(String.format("%s:%s", d.geti(), d.getValue().toString())),
								new Text(String.format("%s:%s:%s ", k.geti(), k.getValue().toString(), distance)));
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
			return Math.exp(Math.sqrt(sum) / (2 * Math.pow(BlockReducer.SIGMA, 2)));
		}
	}

}
