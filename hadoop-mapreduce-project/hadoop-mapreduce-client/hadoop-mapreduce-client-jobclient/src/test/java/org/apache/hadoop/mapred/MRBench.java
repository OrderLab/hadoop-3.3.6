/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapred;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.text.SimpleDateFormat;

//
// import org.apache.hadoop.mapreduce.Job;
// import org.apache.hadoop.mapreduce.JobID;
// import org.apache.hadoop.mapreduce.TaskReport;
// import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.TaskReport;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.JobStatus;
/**
 * Runs a job multiple times and takes average of all runs.
 */
public class MRBench extends Configured implements Tool{
  
  private static final Logger LOG = LoggerFactory.getLogger(MRBench.class);
  private static final String DEFAULT_INPUT_SUB = "mr_input";
  private static final String DEFAULT_OUTPUT_SUB = "mr_output";

  private static Path BASE_DIR =
    new Path(System.getProperty("test.build.data","/benchmarks/MRBench"));
  private static Path INPUT_DIR = new Path(BASE_DIR, DEFAULT_INPUT_SUB);
  private static Path OUTPUT_DIR = new Path(BASE_DIR, DEFAULT_OUTPUT_SUB);
  public static final String ANSI_BLUE = "\u001B[34m";
  public static final String ANSI_RESET = "\u001B[0m";
  public enum Order {RANDOM, ASCENDING, DESCENDING};
  public JobID job_id;

  /**
   * Takes input format as text lines, runs some processing on it and 
   * writes out data as text again. 
   */
  public static class Map extends MapReduceBase
      implements Mapper<WritableComparable<?>, Text, Text, Text> {
    
    public void map(WritableComparable<?> key, Text value,
                    OutputCollector<Text, Text> output,
                    Reporter reporter) throws IOException 
    {
      String line = value.toString();
      output.collect(new Text(process(line)), new Text(""));
    }
    public String process(String line) {
      return line; 
    }
  }

  /**
   * Ignores the key and writes values to the output. 
   */
  public static class Reduce extends MapReduceBase
      implements Reducer<Text, Text, Text, Text> {
    
    public void reduce(Text key, Iterator<Text> values,
                       OutputCollector<Text, Text> output,
                       Reporter reporter) throws IOException
    {
      while(values.hasNext()) {
        output.collect(key, new Text(values.next().toString()));
      }
    }
  }

  /**
   * Generate a text file on the given filesystem with the given path name.
   * The text file will contain the given number of lines of generated data.
   * The generated data are string representations of numbers.  Each line
   * is the same length, which is achieved by padding each number with
   * an appropriate number of leading '0' (zero) characters.  The order of
   * generated data is one of ascending, descending, or random.
   */
  public void generateTextFile(FileSystem fs, Path inputFile, 
                                      long numLines, Order sortOrder) throws IOException 
  {
    LOG.info("creating control file: "+numLines+" numLines, "+sortOrder+" sortOrder");
    PrintStream output = null;
    try {
      output = new PrintStream(fs.create(inputFile));
      int padding = String.valueOf(numLines).length();
      switch(sortOrder) {
      case RANDOM:
        for (long l = 0; l < numLines; l++) {
          output.println(pad((new Random()).nextLong(), padding));
        }
        break; 
      case ASCENDING: 
        for (long l = 0; l < numLines; l++) {
          output.println(pad(l, padding));
        }
        break;
      case DESCENDING: 
        for (long l = numLines; l > 0; l--) {
          output.println(pad(l, padding));
        }
        break;
      }
    } finally {
      if (output != null)
        output.close();
    }
    LOG.info("created control file: " + inputFile);
  }
  
  /**
   * Convert the given number to a string and pad the number with 
   * leading '0' (zero) characters so that the string is exactly
   * the given length.
   */
  private static String pad(long number, int length) {
    String str = String.valueOf(number);
    StringBuffer value = new StringBuffer(); 
    for (int i = str.length(); i < length; i++) {
      value.append("0"); 
    }
    value.append(str); 
    return value.toString();
  }
  
  /**
   * Create the job configuration.
   */
  private JobConf setupJob(int numMaps, int numReduces, String jarFile) {
    JobConf jobConf = new JobConf(getConf());
    jobConf.setJarByClass(MRBench.class);
    FileInputFormat.addInputPath(jobConf, INPUT_DIR);
    
    jobConf.setInputFormat(TextInputFormat.class);
    jobConf.setOutputFormat(TextOutputFormat.class);
    
    jobConf.setOutputValueClass(Text.class);
    
    jobConf.setMapOutputKeyClass(Text.class);
    jobConf.setMapOutputValueClass(Text.class);
    
    if (null != jarFile) {
      jobConf.setJar(jarFile);
    }
    jobConf.setMapperClass(Map.class);
    jobConf.setReducerClass(Reduce.class);
    
    jobConf.setNumMapTasks(numMaps);
    jobConf.setNumReduceTasks(numReduces);
    jobConf
        .setBoolean("mapreduce.job.complete.cancel.delegation.tokens", false);
    return jobConf; 
  }
  
  /**
   * Runs a MapReduce task, given number of times. The input to each run
   * is the same file.
   */
  // Using mapred API
  private ArrayList<Long> runJobInSequence(JobConf masterJobConf, int numRuns) throws IOException {
    Random rand = new Random();
    ArrayList<Long> execTimes = new ArrayList<Long>(); 
    SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss,SSS");
    for (int i = 0; i < numRuns; i++) {
      // create a new job conf every time, reusing same object does not work 
      JobConf jobConf = new JobConf(masterJobConf);
      // reset the job jar because the copy constructor doesn't
      jobConf.setJar(masterJobConf.getJar());
      // give a new random name to output of the mapred tasks
      FileOutputFormat.setOutputPath(jobConf, 
                         new Path(OUTPUT_DIR, "output_" + rand.nextInt()));
      System.out.println(ANSI_BLUE + "getMapSpeculativeExecution: " + jobConf.getMapSpeculativeExecution() + ANSI_RESET);
      System.out.println(ANSI_BLUE + "getReduceSpeculativeExecution: " + jobConf.getReduceSpeculativeExecution() + ANSI_RESET);
      LOG.info("Running job " + i + ":" +
               " input=" + FileInputFormat.getInputPaths(jobConf)[0] + 
               " output=" + FileOutputFormat.getOutputPath(jobConf));
      
      // run the mapred task now 
      long curTime = System.currentTimeMillis();
      System.out.println(ANSI_BLUE+"##RUIMING## MRBench begins at " + curTime + " / " + sdf.format(new Date(curTime))+ANSI_RESET);
      // JobClient jobClient = new JobClient(new JobConf());
      JobClient jobClient = new JobClient(new JobConf());
      RunningJob currentJob = jobClient.submitJob(jobConf);
      job_id = currentJob.getID();
      System.out.println(ANSI_BLUE+"##RUIMING## job_id: " + job_id.toString() + ANSI_RESET);
      while(currentJob.isComplete() == false){ }
      long endTime = System.currentTimeMillis();
      System.out.println(ANSI_BLUE+"##RUIMING## MRBench ends at " + endTime + " / " + sdf.format(new Date(endTime)) + ANSI_RESET);
      System.out.println(ANSI_BLUE+"##RUIMING## MRBench duration (raw): " + sdf.format(new Date(curTime)) + " ~ " + sdf.format(new Date(endTime)) + ANSI_RESET);

      TaskReport[] mapTaskReports = jobClient.getMapTaskReports(job_id);
      TaskReport[] reduceTaskReports = jobClient.getReduceTaskReports(job_id);

      // Process task reports as needed
      for (TaskReport rpt : mapTaskReports) {
          long duration = rpt.getFinishTime() - rpt.getStartTime();
          System.out.println(ANSI_BLUE + "##RUIMING## TaskID:" + rpt.getTaskId() + "\tMapper duration: " + rpt.getFinishTime() + " - " +  rpt.getStartTime() + " = " + duration + " ms" + " ( From " + sdf.format(new Date(rpt.getStartTime())) + " to " + sdf.format(new Date(rpt.getFinishTime())) + " )" + "\tState: "+rpt.getState()+"\tProgress: "+rpt.getProgress() + ANSI_RESET);
      }

      for (TaskReport rpt : reduceTaskReports) {
          long duration = rpt.getFinishTime() - rpt.getStartTime();
          System.out.println(ANSI_BLUE + "##RUIMING## TaskID:" + rpt.getTaskId() + "\tReducer duration: " + rpt.getFinishTime() + " - " +  rpt.getStartTime() + " = " + duration + " ms" + " ( From " + sdf.format(new Date(rpt.getStartTime())) + " to " + sdf.format(new Date(rpt.getFinishTime())) + " )" + "\tState: "+rpt.getState()+"\tProgress: "+rpt.getProgress() + ANSI_RESET);
      }
      execTimes.add(new Long(System.currentTimeMillis() - curTime));
    }
    return execTimes;
  }

  // Using mapreduce API
  /*
  private ArrayList<Long> runJobInSequence(JobConf masterJobConf, int numRuns) throws IOException {
    Random rand = new Random();
    ArrayList<Long> execTimes = new ArrayList<Long>(); 
    
    for (int i = 0; i < numRuns; i++) {
      System.out.println("masterJobConf numMaps: " + masterJobConf.getNumMapTasks());
      JobConf jobConf = new JobConf(masterJobConf);
      jobConf.setJar(masterJobConf.getJar());
      // Set the output path with a random name
      FileOutputFormat.setOutputPath(jobConf, new Path(OUTPUT_DIR, "output_" + rand.nextInt()));
      Job job = Job.getInstance(jobConf);
      System.out.println("jobConf numMaps: " + jobConf.getNumMapTasks());
      LOG.info("Running job " + i + ":" +
               " input=" + FileInputFormat.getInputPaths(jobConf)[0] + 
               " output=" + FileOutputFormat.getOutputPath(jobConf));
      long curTime = System.currentTimeMillis();
      try {
          job.submit(); // Submit the job
      } catch (ClassNotFoundException e) {
          Thread.currentThread().interrupt(); // Restore the interrupted status
      } catch (InterruptedException e) {
          Thread.currentThread().interrupt(); // Restore the interrupted status
      }
      while (!job.isComplete()) {
          try {
              Thread.sleep(1000); // Sleep for a second
          } catch (InterruptedException e) {
              // Handle interruption (e.g., log or re-throw)
              Thread.currentThread().interrupt(); // Restore the interrupted status
          }
      }
      try {
          TaskReport[] mapTaskReports = job.getTaskReports(TaskType.MAP);
          TaskReport[] reduceTaskReports = job.getTaskReports(TaskType.REDUCE);

          // Process task reports as needed
          System.out.println("numMaps: " + mapTaskReports.length);
          System.out.println("numReduces: " + reduceTaskReports.length);
          for (TaskReport rpt : mapTaskReports) {
            long duration = rpt.getFinishTime() - rpt.getStartTime();
            System.out.println("TaskID:" + rpt.getTaskId() + "Mapper duration: " + rpt.getFinishTime() + " - " +  rpt.getStartTime() + " = " + duration + " ms");
          }

          for (TaskReport rpt : reduceTaskReports) {
            long duration = rpt.getFinishTime() - rpt.getStartTime();
            System.out.println("TaskID:" + rpt.getTaskId() + "Reducer duration: " + rpt.getFinishTime() + " - " +  rpt.getStartTime() + " = " + duration + " ms");
          }
      } catch (InterruptedException e) {
          // Handle the InterruptedException (e.g., log or re-throw)
          Thread.currentThread().interrupt(); // Restore the interrupted status
      }
      execTimes.add(new Long(System.currentTimeMillis() - curTime));
    }
    return execTimes;
  }
  */

  /**
   * <pre>
   * Usage: mrbench
   *    [-baseDir <base DFS path for output/input, default is /benchmarks/MRBench>]
   *    [-jar <local path to job jar file containing Mapper and Reducer implementations, default is current jar file>]
   *    [-numRuns <number of times to run the job, default is 1>]
   *    [-maps <number of maps for each run, default is 2>]
   *    [-reduces <number of reduces for each run, default is 1>]
   *    [-inputLines <number of input lines to generate, default is 1>]
   *    [-inputType <type of input to generate, one of ascending (default), descending, random>]
   *    [-verbose]
   * </pre>
   */
  public static void main (String[] args) throws Exception {
    int res = ToolRunner.run(new MRBench(), args);
    System.out.println("END-OF-MRBench");
    System.exit(res);
  }

  @Override
  public int run(String[] args) throws Exception {
    String version = "MRBenchmark.0.0.2";
    System.out.println(version);

    String usage = 
      "Usage: mrbench " +
      "[-baseDir <base DFS path for output/input, default is /benchmarks/MRBench>] " + 
      "[-jar <local path to job jar file containing Mapper and Reducer implementations, default is current jar file>] " + 
      "[-numRuns <number of times to run the job, default is 1>] " +
      "[-maps <number of maps for each run, default is 2>] " +
      "[-reduces <number of reduces for each run, default is 1>] " +
      "[-inputLines <number of input lines to generate, default is 1>] " +
      "[-inputType <type of input to generate, one of ascending (default), descending, random>] " + 
      "[-verbose]";
    
    String jarFile = null;
    long inputLines = 1; 
    int numRuns = 1;
    int numMaps = 2; 
    int numReduces = 1;
    boolean verbose = false;         
    Order inputSortOrder = Order.ASCENDING;     
    for (int i = 0; i < args.length; i++) { // parse command line
      if (args[i].equals("-jar")) {
        jarFile = args[++i];
      } else if (args[i].equals("-numRuns")) {
        numRuns = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-baseDir")) {
        BASE_DIR = new Path(args[++i]);
        INPUT_DIR = new Path(BASE_DIR, DEFAULT_INPUT_SUB);
        OUTPUT_DIR = new Path(BASE_DIR, DEFAULT_OUTPUT_SUB);
      } else if (args[i].equals("-maps")) {
        numMaps = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-reduces")) {
        numReduces = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-inputLines")) {
        inputLines = Long.parseLong(args[++i]);
      } else if (args[i].equals("-inputType")) {
        String s = args[++i]; 
        if (s.equalsIgnoreCase("ascending")) {
          inputSortOrder = Order.ASCENDING;
        } else if (s.equalsIgnoreCase("descending")) {
          inputSortOrder = Order.DESCENDING; 
        } else if (s.equalsIgnoreCase("random")) {
          inputSortOrder = Order.RANDOM;
        } else {
          inputSortOrder = null;
        }
      } else if (args[i].equals("-verbose")) {
        verbose = true;
      } else {
        System.err.println(usage);
        System.exit(-1);
      }
    }
    
    if (numRuns < 1 ||  // verify args
        numMaps < 1 ||
        numReduces < 1 ||
        inputLines < 0 ||
        inputSortOrder == null)
      {
        System.err.println(usage);
        return -1;
      }

    JobConf jobConf = setupJob(numMaps, numReduces, jarFile);
    FileSystem fs = BASE_DIR.getFileSystem(jobConf);
    Path inputFile = new Path(INPUT_DIR, "input_" + (new Random()).nextInt() + ".txt");
    generateTextFile(fs, inputFile, inputLines, inputSortOrder);

    ArrayList<Long> execTimes = new ArrayList<Long>();
    try {
      execTimes = runJobInSequence(jobConf, numRuns);
    } finally {
      // delete all generated data -- should we really do this?
      // we don't know how much of the path was created for the run but this
      // cleans up as much as we can
      fs.delete(OUTPUT_DIR, true);
      fs.delete(INPUT_DIR, true);
    }
    if (verbose) {
      // Print out a report 
      System.out.println("Total MapReduce jobs executed: " + numRuns);
      System.out.println("Total lines of data per job: " + inputLines);
      System.out.println("Maps per job: " + numMaps);
      System.out.println("Reduces per job: " + numReduces);
    }
    int i = 0;
    long totalTime = 0; 
    for (Long time : execTimes) {
      totalTime += time.longValue(); 
      // if (verbose) {
      System.out.println("Total milliseconds for task: " + (++i) + 
                           " = " +  time);
      // }
    }
    long avgTime = totalTime / numRuns; 
    // long avgTime = 0;   
    System.out.println("DataLines\tMaps\tReduces\tAvgTime (milliseconds)");
    System.out.println(inputLines + "\t\t" + numMaps + "\t" + 
                       numReduces + "\t" + avgTime);
    LOG.info("END-OF-MRBench");
    return 0;
  }
  
}
