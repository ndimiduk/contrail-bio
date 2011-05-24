package contrail;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Enumeration;

import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Appender;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Layout;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.TTCCLayout;
import org.apache.log4j.helpers.DateLayout;


public class Contrail extends Configured implements Tool
{
	public static String VERSION = "0.8.2";
	
	private static DecimalFormat df = new DecimalFormat("0.00");
	private static FileOutputStream logfile;
	private static PrintStream logstream;
	
	JobConf baseconf = new JobConf(Contrail.class);
	
    static String basic        = "00-basic";
	static String initial      = "01-initial";
	static String initialcmp   = "02-initialcmp";
	static String notips       = "03-notips";
	static String notipscmp    = "04-notipscmp";
	static String nobubbles    = "05-nobubbles";
	static String nobubblescmp = "06-nobubblescmp";
	static String lowcov       = "07-lowcov";
	static String lowcovcmp    = "08-lowcovcmp";
	static String repeats      = "09-repeats";
	static String repeatscmp   = "10-repeatscmp";
	static String scaff        = "11-scaffold";
	static String finalcmp     = "99-final";


	// Message Management
	///////////////////////////////////////////////////////////////////////////	
	
	long GLOBALNUMSTEPS = 0;
	long JOBSTARTTIME = 0;
	public void start(String desc)
	{
		msg(desc + ":\t");
		JOBSTARTTIME = System.currentTimeMillis();
		GLOBALNUMSTEPS++;
	}
	
	public void end(RunningJob job) throws IOException
	{
		long endtime = System.currentTimeMillis();
		long diff = (endtime - JOBSTARTTIME) / 1000;
		
		msg(job.getJobID() + " " + diff + " s");
		
		if (!job.isSuccessful())
		{
			System.out.println("Job was not successful");
			System.exit(1);
		}
	}
	
	public static void msg(String msg)
	{
		logstream.print(msg);
		System.out.print(msg);
	}
	
	public long counter(RunningJob job, String tag) throws IOException
	{
		return job.getCounters().findCounter("Contrail", tag).getValue();
	}


	
	// Stage Management
	///////////////////////////////////////////////////////////////////////////	

	boolean RUNSTAGE = false;
	private String CURRENTSTAGE;
	
	public boolean runStage(String stage)
	{
		CURRENTSTAGE = stage;
		
		if (ContrailConfig.STARTSTAGE == null || ContrailConfig.STARTSTAGE.equals(stage))
		{
			RUNSTAGE = true;
		}
		
		return RUNSTAGE;
	}
	
	public void checkDone()
	{
		if (ContrailConfig.STOPSTAGE != null && ContrailConfig.STOPSTAGE.equals(CURRENTSTAGE))
		{
			RUNSTAGE = false;
			msg("Stopping after " + ContrailConfig.STOPSTAGE + "\n");
			System.exit(0);
		}
	}

	
	// File Management
	///////////////////////////////////////////////////////////////////////////	
	
	public void cleanup(String path) throws IOException
	{
		FileSystem.get(baseconf).delete(new Path(path), true);
	}
	
	public void save_result(String base, String opath, String npath) throws IOException
	{
		//System.err.println("Renaming " + base + opath + " to " + base + npath);
		
		msg("Save result to " + npath + "\n\n");
		
		FileSystem.get(baseconf).delete(new Path(base+npath), true);
		FileSystem.get(baseconf).rename(new Path(base+opath), new Path(base+npath));
	}
	
	
	// Compute Graph Statistics
	///////////////////////////////////////////////////////////////////////////	
	
	public void computeStats(String base, String dir) throws Exception
	{
		start("Compute Stats " + dir);
		Stats stats = new Stats();
		RunningJob job = stats.run(base+dir, base+dir+".stats");
		end(job);
		
		msg("\n\nStats " + dir + "\n");
		msg("==================================================================================\n");
		
		FSDataInputStream statstream = FileSystem.get(baseconf).open(new Path(base+dir+".stats/part-00000"));
		BufferedReader b = new BufferedReader(new InputStreamReader(statstream));

		String s;
		while ((s = b.readLine()) != null)
		{
			msg(s);
			msg("\n");
		}
		
		msg("\n");
	}
	
	
	// convertFasta
	///////////////////////////////////////////////////////////////////////////	
	
	public void convertFasta(String basePath, String graphdir, String fastadir) throws Exception
	{
		start("convertFasta " + graphdir);
		Graph2Fasta g2f = new Graph2Fasta();
		RunningJob job = g2f.run(basePath + graphdir, basePath + fastadir);
		end(job);
		
		long nodes = counter(job, "nodes");
		msg ("  " + nodes + " converted\n");
	}
	
	
	
	// Build initial graph
	///////////////////////////////////////////////////////////////////////////	
	
	public void buildInitial(String inputPath, String basePath, String basic, String initial, String initialcmp) throws Exception
	{
		RunningJob job;
		
		if (ContrailConfig.RESTART_INITIAL == 0)
		{
			start("Build Initial");
			BuildGraph bg = new BuildGraph();
			job = bg.run(inputPath, basePath + basic);
			end(job);

			long nodecnt      = counter(job, "nodecount");
			long reads_goodbp = counter(job, "reads_goodbp");
			long reads_good   = counter(job, "reads_good");
			long reads_short  = counter(job, "reads_short");
			long reads_skip   = counter(job, "reads_skipped");

			long reads_all = reads_good + reads_short + reads_skip;

			if (reads_good == 0)
			{
				throw new IOException("No good reads");
			}

			String frac_reads = df.format(100*reads_good/reads_all);
			msg("  " + nodecnt + " nodes [" + reads_good +" (" + frac_reads + "%) good reads, " + reads_goodbp + " bp]\n");
		}
		else
		{
			msg("Skipping initial build\n");
		}

		// Quick merge
		start("  Quick Merge");
		QuickMerge qmerge = new QuickMerge();
		job = qmerge.run(basePath + basic, basePath + initial);
		end(job);

		msg("  " + counter(job, "saved") + " saved\n");

		compressChains(basePath, initial, initialcmp);
	}

	

	
	// Maximally compress chains
	///////////////////////////////////////////////////////////////////////////	
	public void compressChains(String basePath, String startname, String finalname) throws Exception
	{
		Compressible comp = new Compressible();
		
		QuickMark qmark   = new QuickMark();
		QuickMerge qmerge = new QuickMerge();

		PairMark pmark   = new PairMark();
		PairMerge pmerge = new PairMerge();
		
		int stage = 0;
		long compressible = 0;
		
		RunningJob job = null;
		
		if (ContrailConfig.RESTART_COMPRESS > 0)
		{
			stage = ContrailConfig.RESTART_COMPRESS;
			compressible = ContrailConfig.RESTART_COMPRESS_REMAIN;
			
			msg("  Restarting compression after stage " + stage + ":");
			
			ContrailConfig.RESTART_COMPRESS = 0;
			ContrailConfig.RESTART_COMPRESS_REMAIN = 0;
		}
		else
		{
			// Mark compressible nodes
			start("  Compressible");
			job = comp.run(basePath+startname, basePath+startname+"."+stage);
			compressible = counter(job, "compressible");
			end(job);
		}

		msg("  " + compressible + " compressible\n");

		long lastremaining = compressible;

		while (lastremaining > 0)
		{
			int prev = stage;
			stage++;

			String input  = basePath + startname + "." + Integer.toString(prev);
			String input0 = input + ".0";
			String output = basePath + startname + "." + Integer.toString(stage);

			long remaining = 0;

			if (lastremaining < ContrailConfig.HADOOP_LOCALNODES)
			{
				// Send all the compressible nodes to the same machine for serial processing
				start("  QMark " + stage);
				job = qmark.run(input, input0);
				end(job);

				msg("  " + counter(job, "compressibleneighborhood") + " marked\n");
				
				start("  QMerge " + stage);
				job = qmerge.run(input0, output);
				end(job);
				
				remaining = counter(job, "needcompress");
			}
			else
			{
				// Use the randomized algorithm
				double rand = Math.random();

				start("  Mark " + stage);
				job = pmark.run(input, input0, (int)(rand*10000000));
				end(job);

				msg("  " + counter(job, "mergestomake") + " marked\n");
			
				start("  Merge " + stage);
				job = pmerge.run(input0, output);
				end(job);
				
				remaining = counter(job,"needscompress");
			}
			
			cleanup(input);
			cleanup(input0);

			String percchange = df.format((lastremaining > 0) ? 100*(remaining - lastremaining) / lastremaining : 0);
			msg("  " + remaining + " remaining (" + percchange + "%)\n");

			lastremaining = remaining;
		}

		save_result(basePath, startname + "." + stage, finalname);
	}
	
	
	// Maximally remove tips
	///////////////////////////////////////////////////////////////////////////	
	
	public void removeTips(String basePath, String current, String prefix, String finalname) throws Exception
	{
		RemoveTips tips = new RemoveTips();
		
		int round = 0;
		long remaining = 1;
		
		if (ContrailConfig.RESTART_TIP > 0)
		{
			round = ContrailConfig.RESTART_TIP;
			ContrailConfig.RESTART_TIP = 0;
		}

		while (remaining > 0)
		{
			round++;

			String output = prefix + "." + round;
			long removed = 0;
			
			if (ContrailConfig.RESTART_TIP_REMAIN > 0)
			{
				remaining = ContrailConfig.RESTART_TIP_REMAIN;
				ContrailConfig.RESTART_TIP_REMAIN = 0;
				msg("Restart remove tips " + round + ":");
				removed = 123456789;
			}
			else
			{
				start("Remove Tips " + round);
				RunningJob job = tips.run(basePath+current, basePath+output);
				end(job);

				removed = counter(job, "tips_found");
				remaining = counter(job, "tips_kept");
			}

			msg("  " + removed + " tips found, " + remaining + " remaining\n");

			if (removed > 0)
			{
				if (round > 1) { cleanup(current); }

				current = output + ".cmp";
				compressChains(basePath, output, current);
				remaining = 1;
			}

			cleanup(output);
		}

		save_result(basePath, current, finalname);
		msg("\n");
	}
	
	
	// Maximally pop bubbles
	///////////////////////////////////////////////////////////////////////////	
	
	public long popallbubbles(String basePath, String basename, String intermediate, String finalname) throws Exception
	{
		long allpopped = 0;
		long popped    = 1;
		int round      = 1;
		
		FindBubbles finder = new FindBubbles();
		PopBubbles  popper = new PopBubbles();

		while (popped > 0)
		{
			String findname = intermediate + "." + round + ".f";
			String popname  = intermediate + "." + round;
			String cmpname  = intermediate + "." + round + ".cmp";
			
			start("Find Bubbles " + round);
			RunningJob job = finder.run(basePath+basename, basePath+findname);
			end(job);

			long potential = counter(job, "potentialbubbles");
			msg("  " + potential + " potential bubbles\n");
			
			start("  Pop " + round);
			job = popper.run(basePath+findname, basePath+popname);
			end(job);

			popped = counter(job, "bubblespopped");
			msg("  " + popped + " bubbles popped\n");

			cleanup(findname);

			if (popped > 0)
			{
				if (round > 1)
				{
					cleanup(basename);
				}

				compressChains(basePath, popname, cmpname);

				basename = cmpname;
				allpopped += popped;
				round++;
			}

			cleanup(popname);
		}

		// Copy the basename to the final name
		save_result(basePath, basename, finalname);
		msg("\n");

		return allpopped;
	}



	// Maximally remove low coverage nodes & compress
	///////////////////////////////////////////////////////////////////////////	
	
	public void removelowcov(String basePath, String nobubblescmp, String lowcov, String lowcovcmp) throws Exception
	{
		RemoveLowCoverage remlowcov = new RemoveLowCoverage();
		
		start("Remove Low Coverage");
		RunningJob job = remlowcov.run(basePath+nobubblescmp, basePath+lowcov);
		end(job);
		
		long lcremoved = counter(job, "lowcovremoved");
		msg("  " + lcremoved +" low coverage nodes removed\n");
		
		if (lcremoved > 0)
		{
			compressChains(basePath, lowcov, lowcov+".c");
			removeTips(basePath, lowcov+".c", lowcov+".t", lowcov+".tc");
			popallbubbles(basePath, lowcov+".tc", lowcov+".b", lowcovcmp);
		}
		else
		{
			save_result(basePath, nobubblescmp, lowcovcmp);
		}
	}

	
	
	// Resolve Repeats / Scaffolding
	///////////////////////////////////////////////////////////////////////////	
	
	public void resolveRepeats(String basePath, String current, String prefix, String finalname, boolean scaffold) throws Exception
	{
		long threadiblecnt = 1;
		int phase = 1;
		
		// short repeats
		ThreadRepeats repeatthreader = new ThreadRepeats();
		Threadible threadibler = new Threadible();
		ThreadResolve threadresolver = new ThreadResolve();
		
		// mate resolved repeats
		MateDist matedist = new MateDist();
		MateBundle matebundle = new MateBundle();
		MateHop matehop = new MateHop();
		MateHopFinalize matehopfinalize = new MateHopFinalize();
		MateFinalize matefinalize = new MateFinalize();
		MateCleanLinks matecleanlinks = new MateCleanLinks();

		UnrollTandem unroller = new UnrollTandem(); 

		RunningJob job;
		
		if (ContrailConfig.RESTART_SCAFF_PHASE > 1)
		{
			phase = ContrailConfig.RESTART_SCAFF_PHASE-1;
			current = prefix + "." + phase + ".popfin";
			phase++;
		}

		while (threadiblecnt > 0)
		{
			if (scaffold)
			{
				msg("Scaffolding phase " + phase + "\n");

				String edgefile   = prefix + "." + phase + ".edges";
				String bundlefile = prefix + "." + phase + ".bundles";
				String matepath   = prefix + "." + phase + ".matepath";
				String finalpath  = prefix + "." + phase + ".final";
				String scaffpath  = prefix + "." + phase + ".scaff";
				
				long allctg = 1;
				long uniquectg = 1;
				
				if ((ContrailConfig.RESTART_SCAFF_STAGE == null) ||
					ContrailConfig.RESTART_SCAFF_STAGE.equals("edges"))
				{
					ContrailConfig.RESTART_SCAFF_STAGE = null;
					
					// Find Mates
					start("  edges");
					job = matedist.run(basePath + current, basePath + edgefile);
					end(job);

					allctg          = counter(job, "nodes");
					uniquectg       = counter(job, "unique_ctg");

					long linking         = counter(job, "linking_edges");
					long internaldist    = counter(job, "internal_dist");
					//long internaldistsq  = counter(job, "internal_distsq");
					long internalcnt     = counter(job, "internal_mates");
					long internalinvalid = counter(job, "internal_invalid");
					
					float internalavg = internalcnt > 0 ? (float)internaldist/(float)internalcnt : 0.0f;
					//double variance    = internalcnt > 0 ?  (internaldistsq - (internaldist*internaldist)/internalcnt) : 0.0;
					//double internalstd = Math.sqrt(Math.abs(variance));

					msg("  " + linking + " linking edges, " + internalcnt + " internal " + 
						internalavg + " avg, " + internalinvalid + " invalid\n");
				}
				
				if ((ContrailConfig.RESTART_SCAFF_STAGE == null) ||
					ContrailConfig.RESTART_SCAFF_STAGE.equals("bundles"))
				{
					ContrailConfig.RESTART_SCAFF_STAGE = null;
					
					// Bundle mates
					start("  bundles");
					job = matebundle.run(basePath + current, basePath + edgefile, basePath + bundlefile);
					end(job);

					long ubundles = counter(job, "unique_bundles");

					msg ("  " + ubundles + " U-bundles " + uniquectg + " unique / " + allctg + " all contigs\n");
				}
				
				String curgraph = "";

				if ((ContrailConfig.RESTART_SCAFF_STAGE == null) ||
					ContrailConfig.RESTART_SCAFF_STAGE.equals("frontier"))
				{
					ContrailConfig.RESTART_SCAFF_STAGE = null;
				
					threadiblecnt = 0;

					// Perform frontier search for mate-consistent paths
					long active = 1; // assert there are active mate-threads to consider
					long stage  = ContrailConfig.RESTART_SCAFF_FRONTIER;
					ContrailConfig.RESTART_SCAFF_FRONTIER = 0;
					
					curgraph = bundlefile;
					
					if (stage > 0)
					{
						msg(" Restarting frontier search after stage: " + stage + "\n");
						curgraph = prefix + "." + phase + ".search" + stage;
					}

					while ((active > 0) && (stage < ContrailConfig.MAX_FRONTIER))
					{
						stage++;

						String prevgraph = curgraph;
						curgraph = prefix + "." + phase + ".search" + stage;

						start("  search " + stage);
						job = matehop.run(basePath + prevgraph, basePath + curgraph, stage==1);
						end(job);

						long shortcnt = counter(job, "foundshort");
						long longcnt  = counter(job, "foundlong");
						long invalid  = counter(job, "foundinvalid");
						long valid    = counter(job, "foundvalid");
						long toolong  = counter(job, "toolong");
						active        = counter(job, "active");

						msg(" active: "  + active + " toolong: " + toolong +
							" | valid: " + valid + " short: "   + shortcnt + " long: " + longcnt + 
							" invalid: " + invalid + "\n");
					}
				}
					
				if ((ContrailConfig.RESTART_SCAFF_STAGE == null) ||
					ContrailConfig.RESTART_SCAFF_STAGE.equals("update"))
				{
					if (ContrailConfig.RESTART_SCAFF_STAGE != null)
					{
						ContrailConfig.RESTART_SCAFF_STAGE = null;
						long stage = ContrailConfig.MAX_FRONTIER;
						
						if (ContrailConfig.RESTART_SCAFF_FRONTIER > 0)
						{
							stage = ContrailConfig.RESTART_SCAFF_FRONTIER;
							ContrailConfig.RESTART_SCAFF_FRONTIER = 0;
						}
							
						curgraph = prefix + "." + phase + ".search" + stage;
					}
				
					start("  update");
					job = matehopfinalize.run(basePath + curgraph, basePath + matepath);
					end(job);
				
					long bresolved = counter(job, "resolved_bundles");
					long eresolved = counter(job, "resolved_edges");
					long ambig     = counter(job, "total_ambiguous");

					msg(" " + bresolved + " bundles resolved, " + eresolved + " edges, " + ambig + " ambiguous\n");
				}
				
				long updates = 1;
				long deadedges = 1;
				
				if ((ContrailConfig.RESTART_SCAFF_STAGE == null) ||
					ContrailConfig.RESTART_SCAFF_STAGE.equals("finalize"))
				{
					ContrailConfig.RESTART_SCAFF_STAGE = null;

					// Record path
					start("  finalize");
					job = matefinalize.run(basePath + matepath, basePath + finalpath);
					end(job);

					updates = counter(job, "updates");
					msg("  " + updates + " nodes resolved\n");
				}
				
				if ((ContrailConfig.RESTART_SCAFF_STAGE == null) ||
					ContrailConfig.RESTART_SCAFF_STAGE.equals("clean"))
				{
					ContrailConfig.RESTART_SCAFF_STAGE = null;

					// Clean bogus links from unique nodes
					start("  clean");
					job = matecleanlinks.run(basePath + finalpath, basePath + scaffpath);
					end(job);

					deadedges = counter(job, "removed_edges");
					msg("  " + deadedges + " edges removed\n");
				}

				threadiblecnt = updates + deadedges;
				current = scaffpath;
			}
			else
			{
				String output = prefix + "." + phase + ".threads";
				
				if ((ContrailConfig.RESTART_SCAFF_STAGE == null) ||
					 ContrailConfig.RESTART_SCAFF_STAGE.equals("threadrepeats"))
				{
					ContrailConfig.RESTART_SCAFF_STAGE = null;

					// Find threadible nodes
					start("Thread Repeats " + phase);
					job = repeatthreader.run(basePath+current, basePath+output);
					end(job);

					threadiblecnt  = counter(job, "threadible");
					long xcut      = counter(job, "xcut");
					long half      = counter(job, "halfdecision");
					long deadend   = counter(job, "deadend");
					msg("  " + threadiblecnt +" threadible (" + xcut + " xcut, " + half + " half, " + deadend + " deadend)\n");
				}

				current = output;
			}

			if (threadiblecnt > 0)
			{
				// Mark threadible neighbors
				String threadible = prefix + "." + phase + ".threadible";
				String resolved = prefix + "." + phase + ".resolved";

				if ((ContrailConfig.RESTART_SCAFF_STAGE == null) ||
						 ContrailConfig.RESTART_SCAFF_STAGE.equals("threadible"))
				{
					ContrailConfig.RESTART_SCAFF_STAGE = null;
				
					start("  Threadible " + phase);
					job = threadibler.run(basePath+current, basePath+threadible);
					end(job);
					threadiblecnt = counter(job, "threadible");
					msg("  " + threadiblecnt + " threaded nodes\n");
				}
				
				long remaining = -1;
				
				if ((ContrailConfig.RESTART_SCAFF_STAGE == null) ||
						 ContrailConfig.RESTART_SCAFF_STAGE.equals("resolve"))
				{
					ContrailConfig.RESTART_SCAFF_STAGE = null;

					// Resolve a subset of threadible nodes
					start("  Resolve " + phase);
					job = threadresolver.run(basePath + threadible, basePath+resolved);
					end(job);
				
					remaining = counter(job, "needsplit");
					msg("  " + remaining + " remaining\n");
				}
				
				if (remaining == threadiblecnt)
				{
					msg("  Didn't thread any node, giving up\n");
					threadiblecnt = 0;
				}
				else
				{
					if ((ContrailConfig.RESTART_SCAFF_STAGE == null) ||
							 ContrailConfig.RESTART_SCAFF_STAGE.equals("compress"))
					{
						ContrailConfig.RESTART_SCAFF_STAGE = null;
						compressChains(basePath, resolved, prefix + "." + phase + ".cmp");
					}

					if ((ContrailConfig.RESTART_SCAFF_STAGE == null) ||
							 ContrailConfig.RESTART_SCAFF_STAGE.equals("removetips"))
					{						
						ContrailConfig.RESTART_SCAFF_STAGE = null;
						removeTips(basePath,	
							prefix + "." + phase + ".cmp", 
							prefix + "." + phase + ".tips",
							prefix + "." + phase + ".tipsfin");
					}

					if ((ContrailConfig.RESTART_SCAFF_STAGE == null) ||
							 ContrailConfig.RESTART_SCAFF_STAGE.equals("popbubbles"))
					{
						ContrailConfig.RESTART_SCAFF_STAGE = null;
						popallbubbles(basePath,
							prefix + "." + phase + ".tipsfin", 
							prefix + "." + phase + ".pop", 
							prefix + "." + phase + ".popfin");
					}

					current = prefix + "." + phase + ".popfin";

					computeStats(basePath, current);

					phase++;
					msg("\n");
				}
			}
		}

		
		// Unroll simple tandem repeats
		boolean UNROLL_TANDEM = false;
		
		if (scaffold && UNROLL_TANDEM)
		{
			msg("\n\n");

			String output = prefix + ".unroll";

			start("Unroll tandems");
			job = unroller.run(basePath + current, basePath + output);
			end(job);

			long unrolled = counter(job, "simpletandem");
			long tandem   = counter(job, "tandem");
			msg("  " + unrolled + " unrolled (" + tandem + " total)\n");

			current = output + ".cmp";
			compressChains(basePath, output, current);
		}

		
		// The current phase did nothing, so save away current
		save_result(basePath, current, finalname);
		msg("\n");
	}
	
	

	
	// Run an entire assembly
	///////////////////////////////////////////////////////////////////////////	

	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	public int run(String[] args) throws Exception 
	{  
	    // A few preconfigured datasets

		String dataset = "";
		//dataset = "arbrcrd";
		//dataset = "Ba10k";
		//dataset = "Ba100k";
		//dataset = "Ec500k";
		//dataset = "Ec500k.cor.21";
		//dataset = "Ec500k.21";
		//dataset = "10hop";
		//dataset = "15hop";
		//dataset = "20359.prb";
		//dataset = "202.dad";
		//dataset = "202.prb";
		
		if (dataset.equals("202.prb"))
		{
			ContrailConfig.hadoopReadPath = "/Users/mschatz/build/Contrail/data/202.prb.sfa";
			ContrailConfig.hadoopBasePath = "/users/mschatz/contrail/202.prb/";
		
			ContrailConfig.K = 21;
			ContrailConfig.LOW_COV_THRESH = 3.0f;
			ContrailConfig.MAX_LOW_COV_LEN = 50;
			ContrailConfig.MIN_THREAD_WEIGHT = 5;
		}
		else if (dataset.equals("202.dad"))
		{
			ContrailConfig.hadoopReadPath = "/Users/mschatz/build/Contrail/data/202.dad.sfa";
			ContrailConfig.hadoopBasePath = "/users/mschatz/contrail/202.dad/";
		
			ContrailConfig.K = 21;
			ContrailConfig.LOW_COV_THRESH = 3.0f;
			ContrailConfig.MAX_LOW_COV_LEN = 50;
			ContrailConfig.MIN_THREAD_WEIGHT = 5;
		}
		else if (dataset.equals("20359.prb"))
		{
			ContrailConfig.hadoopReadPath = "/Users/mschatz/build/Contrail/data/20359.prb.sfa";
			ContrailConfig.hadoopBasePath = "/users/mschatz/contrail/20359.prb/";
		
			ContrailConfig.K = 21;
			ContrailConfig.LOW_COV_THRESH = 3.0f;
			ContrailConfig.MAX_LOW_COV_LEN = 50;
			ContrailConfig.MIN_THREAD_WEIGHT = 5;
		}
		else if (dataset.equals("non"))
		{
			ContrailConfig.hadoopReadPath = "/Users/mschatz/build/Contrail/data/nonoverlap.sfa";
			ContrailConfig.hadoopBasePath = "/users/mschatz/contrail/non/";
		
			ContrailConfig.K = 25;
			ContrailConfig.LOW_COV_THRESH = 0.0f;
			ContrailConfig.MAX_LOW_COV_LEN = 50;
			ContrailConfig.MIN_THREAD_WEIGHT = 1;
			
			ContrailConfig.INSERT_LEN = 210;
			ContrailConfig.MIN_UNIQUE_COV = 10;
			ContrailConfig.MAX_UNIQUE_COV = 30;
		}
		else if (dataset.equals("Ba10k"))
		{
			ContrailConfig.hadoopReadPath = "/Users/mschatz/build/Contrail/data/Ba10k.sim.sfa";
			ContrailConfig.hadoopBasePath = "/users/mschatz/contrail/Ba10k/";
			
			ContrailConfig.K = 21;
			ContrailConfig.LOW_COV_THRESH = 5.0f;
			ContrailConfig.MAX_LOW_COV_LEN = 50;
			ContrailConfig.MIN_THREAD_WEIGHT = 5;

			ContrailConfig.INSERT_LEN = 210;
			ContrailConfig.MIN_CTG_LEN = ContrailConfig.K;
			ContrailConfig.MIN_UNIQUE_COV = 10;
			ContrailConfig.MAX_UNIQUE_COV = 30;
		}
		else if (dataset.equals("Ba100k"))
		{
			ContrailConfig.hadoopReadPath = "/Users/mschatz/build/Contrail/data/Ba100k.sim.sfa";
			ContrailConfig.hadoopBasePath = "/users/mschatz/contrail/Ba100k/";
			//ContrailConfig.STARTSTAGE = "scaffolding";
			
			ContrailConfig.K = 21;
			ContrailConfig.LOW_COV_THRESH = 5.0f;
			ContrailConfig.MAX_LOW_COV_LEN = 50;
			ContrailConfig.MIN_THREAD_WEIGHT = 5;

			ContrailConfig.INSERT_LEN = 210;
			ContrailConfig.MIN_CTG_LEN = ContrailConfig.K;
			ContrailConfig.MIN_UNIQUE_COV = 10;
			ContrailConfig.MAX_UNIQUE_COV = 30;
		}
		else if (dataset.equals("Ec10k"))
		{
			ContrailConfig.hadoopReadPath = "/Users/mschatz/build/Contrail/data/Ec10k.sim.sfa";
			ContrailConfig.hadoopBasePath = "/users/mschatz/contrail/Ec10k/";
			
			ContrailConfig.K = 21;
			ContrailConfig.LOW_COV_THRESH = 5.0f;
			ContrailConfig.MAX_LOW_COV_LEN = 50;
			ContrailConfig.MIN_THREAD_WEIGHT = 5;

			ContrailConfig.INSERT_LEN = 210;
			ContrailConfig.MIN_CTG_LEN = ContrailConfig.K;
			ContrailConfig.MIN_UNIQUE_COV = 10;
			ContrailConfig.MAX_UNIQUE_COV = 30;
		}
		else if (dataset.equals("Ec100k"))
		{
			ContrailConfig.hadoopReadPath = "/Users/mschatz/build/Contrail/data/Ec100k.sim.sfa";
			ContrailConfig.hadoopBasePath = "/users/mschatz/contrail/Ec100k/";
			ContrailConfig.STARTSTAGE = "scaffolding";
			
			ContrailConfig.K = 21;
			ContrailConfig.LOW_COV_THRESH = 5.0f;
			ContrailConfig.MAX_LOW_COV_LEN = 50;
			ContrailConfig.MIN_THREAD_WEIGHT = 5;

			ContrailConfig.INSERT_LEN = 210;
			ContrailConfig.MIN_CTG_LEN = ContrailConfig.K;
			ContrailConfig.MIN_UNIQUE_COV = 10;
			ContrailConfig.MAX_UNIQUE_COV = 30;
		}
		else if (dataset.equals("Ec200k"))
		{
			ContrailConfig.hadoopReadPath = "/Users/mschatz/build/Contrail/data/Ec200k.sim.sfa";
			ContrailConfig.hadoopBasePath = "/users/mschatz/contrail/Ec200k/";
		    ContrailConfig.STARTSTAGE = "scaffolding";
			
		    ContrailConfig.K = 21;
		    ContrailConfig.LOW_COV_THRESH = 5.0f;
		    ContrailConfig.MAX_LOW_COV_LEN = 50;
		    ContrailConfig.MIN_THREAD_WEIGHT = 5;

		    ContrailConfig.INSERT_LEN = 210;
		    ContrailConfig.MIN_CTG_LEN = ContrailConfig.K;
		    ContrailConfig.MIN_UNIQUE_COV = 10;
		    ContrailConfig.MAX_UNIQUE_COV = 30;
		}
		else if (dataset.equals("Ec500k.21"))
		{
			ContrailConfig.hadoopReadPath = "/Users/mschatz/build/Contrail/data/Ec500k.sim.sfa";
			ContrailConfig.hadoopBasePath = "/users/mschatz/contrail/Ec500k.21";
			//ContrailConfig.STARTSTAGE = "removeTips"; //"scaffolding";
			
			ContrailConfig.K = 21;
			ContrailConfig.LOW_COV_THRESH = 5.0f;
			ContrailConfig.MAX_LOW_COV_LEN = 50;
			ContrailConfig.MIN_THREAD_WEIGHT = 5;

			ContrailConfig.INSERT_LEN = 210;
			ContrailConfig.MIN_CTG_LEN = ContrailConfig.K;
			ContrailConfig.MIN_UNIQUE_COV = 10;
			ContrailConfig.MAX_UNIQUE_COV = 30;
		}
		else if (dataset.equals("Ec500k.cor.21"))
		{
			ContrailConfig.hadoopReadPath = "/Users/mschatz/build/Contrail/data/Ec500k.sim.cor.sfa";
			ContrailConfig.hadoopBasePath = "/users/mschatz/contrail/Ec500k.cor.21/";
			//ContrailConfig.STARTSTAGE = "scaffolding";
			
			ContrailConfig.K = 21;
			ContrailConfig.LOW_COV_THRESH = 5.0f;
			ContrailConfig.MAX_LOW_COV_LEN = 50;
			ContrailConfig.MIN_THREAD_WEIGHT = 5;

			ContrailConfig.INSERT_LEN = 210;
			ContrailConfig.MIN_CTG_LEN = ContrailConfig.K;
			ContrailConfig.MIN_UNIQUE_COV = 10;
			ContrailConfig.MAX_UNIQUE_COV = 30;
		}
		else if (dataset.equals("arbrcrd"))
		{
			ContrailConfig.hadoopReadPath = "/Users/mschatz/build/Contrail/data/arbrcrd.36.sfa";
			ContrailConfig.hadoopBasePath = "/users/mschatz/contrail/arbrcrd.new/";
			ContrailConfig.localBasePath = ContrailConfig.hadoopBasePath + "work";
			
			//ContrailConfig.RESTART_SCAFF_STEP = "update";
			//ContrailConfig.RESTART_SCAFF_FRONTIER = 3;
			//ContrailConfig.STARTSTAGE = "scaffolding";
			//ContrailConfig.STOPSTAGE = "buildInitial";
			
			ContrailConfig.K = 25;
			ContrailConfig.LOW_COV_THRESH = 5.0f;
			ContrailConfig.MAX_LOW_COV_LEN = 50;
			ContrailConfig.MIN_THREAD_WEIGHT = 5;
			
			ContrailConfig.INSERT_LEN = 100;
			ContrailConfig.MIN_CTG_LEN = ContrailConfig.K;
			ContrailConfig.MIN_UNIQUE_COV = 10.0f;
			ContrailConfig.MAX_UNIQUE_COV = 30.0f;
		}
		else if (dataset.equals("10hop"))
		{
			ContrailConfig.hadoopReadPath = "foo";
			ContrailConfig.hadoopBasePath = "/users/mschatz/contrail/10hop/";
			ContrailConfig.localBasePath = ContrailConfig.hadoopBasePath + "work";
			
			ContrailConfig.STARTSTAGE = "scaffolding";
			ContrailConfig.RESTART_SCAFF_STAGE = "update";
			ContrailConfig.RESTART_SCAFF_FRONTIER = 10;
			
			ContrailConfig.K = 29;
			ContrailConfig.LOW_COV_THRESH = 5.0f;
			ContrailConfig.MAX_LOW_COV_LEN = 50;
			ContrailConfig.MIN_THREAD_WEIGHT = 5;
			
			ContrailConfig.INSERT_LEN = 210;
			ContrailConfig.MIN_CTG_LEN = ContrailConfig.K;
			ContrailConfig.MIN_UNIQUE_COV = 15.0f;
			ContrailConfig.MAX_UNIQUE_COV = 40.0f;
		}
		else if (dataset.equals("15hop"))
		{
			ContrailConfig.hadoopReadPath = "foo";
			ContrailConfig.hadoopBasePath = "/users/mschatz/contrail/15hop/";
			ContrailConfig.localBasePath = ContrailConfig.hadoopBasePath + "work";
			
			ContrailConfig.STARTSTAGE = "scaffolding";
			ContrailConfig.RESTART_SCAFF_STAGE = "update";
			ContrailConfig.RESTART_SCAFF_FRONTIER = 6;
			
			ContrailConfig.K = 29;
			ContrailConfig.LOW_COV_THRESH = 5.0f;
			ContrailConfig.MAX_LOW_COV_LEN = 50;
			ContrailConfig.MIN_THREAD_WEIGHT = 5;
			
			ContrailConfig.INSERT_LEN = 210;
			ContrailConfig.MIN_CTG_LEN = ContrailConfig.K;
			ContrailConfig.MIN_UNIQUE_COV = 15.0f;
			ContrailConfig.MAX_UNIQUE_COV = 40.0f;
		}
		else
		{
			ContrailConfig.parseOptions(args);
		}
		
	    ContrailConfig.validateConfiguration();
		
		// Setup to use a file appender
	    BasicConfigurator.resetConfiguration();
		
		TTCCLayout lay = new TTCCLayout();
		lay.setDateFormat("yyyy-mm-dd HH:mm:ss.SSS");
		
	    FileAppender fa = new FileAppender(lay, ContrailConfig.localBasePath+"contrail.details.log", true);
	    fa.setName("File Appender");
	    fa.setThreshold(Level.INFO);
	    BasicConfigurator.configure(fa);
	    
	    logfile = new FileOutputStream(ContrailConfig.localBasePath+"contrail.log", true);
	    logstream = new PrintStream(logfile);
	    
		ContrailConfig.printConfiguration();
		
		// Time stamp
		DateFormat dfm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		msg("== Starting time " + dfm.format(new Date()) + "\n");
		long globalstarttime = System.currentTimeMillis();
		
		if (ContrailConfig.RUN_STATS != null)
		{
			computeStats("", ContrailConfig.RUN_STATS);
		}
		else if (ContrailConfig.CONVERT_FA != null)
		{
			convertFasta("", ContrailConfig.CONVERT_FA, ContrailConfig.CONVERT_FA + ".fa");
		}
		else
		{
			// Assembly Pipeline
		
			if (runStage("buildInitial"))
			{
				buildInitial(ContrailConfig.hadoopReadPath, ContrailConfig.hadoopBasePath, basic, initial, initialcmp);	
				computeStats(ContrailConfig.hadoopBasePath, initialcmp);
				checkDone();
			}

			if (runStage("removeTips"))
			{
				removeTips(ContrailConfig.hadoopBasePath, initialcmp, notips, notipscmp);
				computeStats(ContrailConfig.hadoopBasePath, notipscmp);
				checkDone();
			}

			if (runStage("popBubbles"))
			{
				popallbubbles(ContrailConfig.hadoopBasePath, notipscmp, nobubbles, nobubblescmp);
				computeStats(ContrailConfig.hadoopBasePath, nobubblescmp);
				checkDone();
			}

			if (runStage("lowcov"))
			{
				removelowcov(ContrailConfig.hadoopBasePath, nobubblescmp, lowcov, lowcovcmp);
				computeStats(ContrailConfig.hadoopBasePath, lowcovcmp);
				checkDone();
			}

			if (runStage("repeats"))
			{
				resolveRepeats(ContrailConfig.hadoopBasePath, lowcovcmp, repeats, repeatscmp, false);
				computeStats(ContrailConfig.hadoopBasePath, repeatscmp);
				checkDone();
			}

			if (runStage("scaffolding"))
			{
				if (ContrailConfig.INSERT_LEN > 0)
				{
					resolveRepeats(ContrailConfig.hadoopBasePath, repeatscmp, scaff, finalcmp, true);
					computeStats(ContrailConfig.hadoopBasePath, finalcmp);
				}
				else
				{
					save_result(ContrailConfig.hadoopBasePath, repeatscmp, finalcmp);
					save_result(ContrailConfig.hadoopBasePath, repeatscmp+".stats", finalcmp+".stats");
				}

				checkDone();
			}

			if (runStage("convertFasta"))
			{
				convertFasta(ContrailConfig.hadoopBasePath, finalcmp, finalcmp + ".fa");
				checkDone();
			}
		}
		
        // Final timestamp		
		long globalendtime = System.currentTimeMillis();
		long globalduration = (globalendtime - globalstarttime)/1000;
		msg("== Ending time " + dfm.format(new Date()) + "\n");
		msg("== Duration: " + globalduration + " s, " + GLOBALNUMSTEPS + " total steps\n");
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception 
	{
		int res = ToolRunner.run(new Configuration(), new Contrail(), args);
		System.exit(res);
	}
}
