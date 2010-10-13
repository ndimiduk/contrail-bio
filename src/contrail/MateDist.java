package contrail;

import java.io.IOException;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.ArrayList;
import java.util.Map;
import java.util.List;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;


public class MateDist extends Configured implements Tool 
{	
	private static final Logger sLogger = Logger.getLogger(MateDist.class);


	// MateDistMapper
	///////////////////////////////////////////////////////////////////////////

	private static class MateDistMapper extends MapReduceBase 
	implements Mapper<LongWritable, Text, Text, Text> 
	{
		private static int   K = 0;
		private static int   INSERT_LEN = 0;
		private static int   MIN_CTG_LEN = 0;
		private static float MAX_UNIQUE_COV = 0.0f;
		private static float MIN_UNIQUE_COV = 0.0f;


		public void configure(JobConf job) 
		{
			K = Integer.parseInt(job.get("K"));
			INSERT_LEN = Integer.parseInt(job.get("INSERT_LEN"));
			MIN_CTG_LEN = Integer.parseInt(job.get("MIN_CTG_LEN"));
			MIN_UNIQUE_COV = Float.parseFloat(job.get("MIN_UNIQUE_COV"));
			MAX_UNIQUE_COV = Float.parseFloat(job.get("MAX_UNIQUE_COV"));
		}

		public class ReadOffset
		{
			boolean rc;
			boolean internal;
			int dist;
			String read;

			public ReadOffset(boolean isrc, int curdist, String curread)
			{
				rc = isrc;
				dist = curdist;
				read = curread;
				internal = false;
			}
		}

		public void map(LongWritable lineid, Text nodetxt,
				OutputCollector<Text, Text> output, Reporter reporter)
		throws IOException 
		{
			Node node = new Node();
			node.fromNodeMsg(nodetxt.toString());

			reporter.incrCounter("Contrail", "nodes", 1);

			int len = node.len();
			boolean isunique = node.isUnique(MIN_CTG_LEN, MIN_UNIQUE_COV, MAX_UNIQUE_COV);

			if (isunique) { reporter.incrCounter("Contrail", "unique_ctg", 1); }
			
			//System.err.println("checking: " + node.getNodeId() + " len: " + len + " cov: " + node.cov() + " isuni:" + isunique);

			// Only consider mates from unique contigs
			List<String> reads = node.getreads();

			if (isunique && reads != null)
			{
				Map<String, ReadOffset> contiginfo = new HashMap<String, ReadOffset>();

				for(String readstr : reads)
				{
					String [] vals = readstr.split(":");

					String read = vals[0];
					int offset = Integer.parseInt(vals[1]);
					boolean rc = false;

					if (read.startsWith("~"))
					{
						rc = true;
						read = read.substring(1);
					}

					String basename = Node.mate_basename(read);

					if (basename != null)
					{
						if (contiginfo.containsKey(basename))
						{
							ReadOffset ro = contiginfo.get(basename);
							ro.internal = true;

							int idist = 0;
							boolean ok = true;

							if      (ro.rc && !rc) { idist = ro.dist - offset + 1; }
							else if (rc && !ro.rc) { idist = offset - (len - ro.dist) + 1; }
							else
							{
								ok = false;
							}

							if (ok)
							{
								reporter.incrCounter("Contrail", "internal_mates",  1);
								reporter.incrCounter("Contrail", "internal_dist",   idist);
								reporter.incrCounter("Contrail", "internal_distsq", (idist*idist));
							}
							else
							{
								reporter.incrCounter("Contrail", "internal_invalidd", 1);
							}
						}
						else
						{
							int dist = offset;

							if (!rc) { dist = len - offset; }

							ReadOffset ro = new ReadOffset(rc, dist, read);
							contiginfo.put(basename, ro);
						}
					}
				}

				for (String basename : contiginfo.keySet())
				{
					ReadOffset info = contiginfo.get(basename);

					if (!info.internal)
					{
						output.collect(new Text(basename), 
								new Text(Node.MATEDIST + "\t" + node.getNodeId() + "\t" + info.read + "\t" + 
										(info.rc ? "1" : "0") + "\t" + info.dist + "\t" + (isunique ? "1" : "0")));
						reporter.incrCounter("Contrail", "linking_reads", 1);
					}
				}
			}
		}
	}

	// MateDistReducer
	///////////////////////////////////////////////////////////////////////////

	private static class MateDistReducer extends MapReduceBase 
	implements Reducer<Text, Text, Text, Text> 
	{
		private static int K = 0;
		private static int INSERT_LEN = 0;

		public void configure(JobConf job) {
			K = Integer.parseInt(job.get("K"));
			INSERT_LEN = Integer.parseInt(job.get("INSERT_LEN"));
		}
		
		public class ContigLink
		{
		    public String ctg;
		    public String read;
		    public boolean rc;
		    public int dist;
		    public boolean unique;
		    
		    public ContigLink(String [] vals, int offset) throws IOException
		    {
		    	if (!vals[offset].equals(Node.MATEDIST))
		    	{
		    		throw new IOException("Unknown message type");
		    	}
		    	
		    	ctg  = vals[offset + 1];
		    	read = vals[offset + 2];
		    	rc   = Integer.parseInt(vals[offset+3]) == 1;
		    	dist = Integer.parseInt(vals[offset+4]);
		    	unique = Integer.parseInt(vals[offset+5]) == 1;
		    }
		}

		public void reduce(Text basename, Iterator<Text> iter,
				OutputCollector<Text, Text> output, Reporter reporter)
		throws IOException 
		{
			int sawnode = 0;
			
			ContigLink lnk1 = null;
			ContigLink lnk2 = null;

			while(iter.hasNext())
			{
				String msg = iter.next().toString();

				//System.err.println(nodeid.toString() + "\t" + msg);

				String [] vals = msg.split("\t");

				if (vals[0].equals(Node.MATEDIST))
				{
					if (lnk1 == null)
					{
						lnk1 = new ContigLink(vals, 0);
					}
					else if (lnk2 == null)
					{
						lnk2 = new ContigLink(vals, 0);
					}
					else
					{
						throw new IOException("More than 2 contig link messages for: " + basename.toString());
					}
				}
				else
				{
					throw new IOException("Unknown msgtype: " + msg);
				}
			}
			
			if ((lnk1 != null) && (lnk2 != null))
			{
				// Don't both record repeat-repeat bundles
				if (lnk1.unique || lnk2.unique)
				{
					int insertlen = Node.mate_insertlen(lnk1.read, lnk2.read, INSERT_LEN);

					int dist = insertlen - lnk1.dist - lnk2.dist;

					String ee1 = lnk1.rc ? "r" : "f";
					String ee2 = lnk2.rc ? "f" : "r"; 

					String ee = ee1+ee2;
					String ff = Node.flip_link(ee);

					reporter.incrCounter("Contrail", "linking_edges", 1);

					output.collect(new Text(lnk1.ctg), 
							new Text(Node.MATEEDGE + "\t" + ee + "\t" + lnk2.ctg + "\t" + 
									dist + "\t" + basename + "\t" + (lnk2.unique ? "1" : "0")));

					output.collect(new Text(lnk2.ctg), 
							new Text(Node.MATEEDGE + "\t" + ff + "\t" + lnk1.ctg + "\t" + 
									dist + "\t" + basename + "\t" + (lnk1.unique ? "1" : "0")));

				}
			}
		}
	}




	// Run Tool
	///////////////////////////////////////////////////////////////////////////	

	public RunningJob run(String inputPath, String outputPath) throws Exception
	{ 
		sLogger.info("Tool name: MateDist");
		sLogger.info(" - input: "  + inputPath);
		sLogger.info(" - output: " + outputPath);

		JobConf conf = new JobConf(Stats.class);
		conf.setJobName("MateDist " + inputPath);
		
		ContrailConfig.initializeConfiguration(conf);

		FileInputFormat.addInputPath(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(MateDistMapper.class);
		conf.setReducerClass(MateDistReducer.class);

		//delete the output directory if it exists already
		FileSystem.get(conf).delete(new Path(outputPath), true);

		return JobClient.runJob(conf);
	}


	// Parse Arguments and run
	///////////////////////////////////////////////////////////////////////////	

	public int run(String[] args) throws Exception 
	{
		String inputPath  = "/Users/mschatz/try/10-repeatscmp";
		String outputPath = "/users/mschatz/try/11-scaffold.1.edges";
		
		ContrailConfig.K = 21;
		ContrailConfig.INSERT_LEN = 210;
		ContrailConfig.MIN_CTG_LEN = 21;
		ContrailConfig.MIN_UNIQUE_COV = 10.0f;
		ContrailConfig.MAX_UNIQUE_COV = 30.0f;

		run(inputPath, outputPath);
		return 0;
	}


	// Main
	///////////////////////////////////////////////////////////////////////////	

	public static void main(String[] args) throws Exception 
	{
		int res = ToolRunner.run(new Configuration(), new MateDist(), args);
		System.exit(res);
	}
}
