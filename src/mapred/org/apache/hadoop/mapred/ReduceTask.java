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

import static org.apache.hadoop.mapred.Task.Counter.COMBINE_INPUT_RECORDS;
import static org.apache.hadoop.mapred.Task.Counter.COMBINE_OUTPUT_RECORDS;
import static org.apache.hadoop.mapred.Task.Counter.MAP_OUTPUT_BYTES;
import static org.apache.hadoop.mapred.Task.Counter.MAP_OUTPUT_RECORDS;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLConnection;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ChecksumFileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSError;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapred.IFile.InMemoryReader;
import org.apache.hadoop.mapred.IFile.Reader;
import org.apache.hadoop.mapred.IFile.Writer;
//import org.apache.hadoop.mapred.MapTask.MapBufferTooSmallException;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapTask.MapOutputBuffer;
import org.apache.hadoop.mapred.MapTask.MapOutputCollector;
import org.apache.hadoop.mapred.MapTask.MapOutputBuffer.BlockingBuffer;
import org.apache.hadoop.mapred.MapTask.MapOutputBuffer.Buffer;
import org.apache.hadoop.mapred.MapTask.MapOutputBuffer.InMemValBytes;
import org.apache.hadoop.mapred.MapTask.MapOutputBuffer.MRResultIterator;
import org.apache.hadoop.mapred.MapTask.MapOutputBuffer.SpillThread;
import org.apache.hadoop.mapred.Merger.Segment;
import org.apache.hadoop.mapred.SortedRanges.SkipRangeIterator;
import org.apache.hadoop.mapred.Task.CombineOutputCollector;
import org.apache.hadoop.mapred.Task.CombinerRunner;
import org.apache.hadoop.mapred.Task.TaskReporter;
import org.apache.hadoop.mapred.TaskTracker.TaskInProgress;
//import org.apache.hadoop.mapred.buffer.impl.JOutputBuffer;
import org.apache.hadoop.mapred.iterative.IDistanceMeasure;
import org.apache.hadoop.mapred.iterative.LoopReduceCacheFilter;
import org.apache.hadoop.mapred.iterative.LoopReduceCacheSwitch;
import org.apache.hadoop.mapred.iterative.LoopReduceOutputCacheFilter;
import org.apache.hadoop.mapred.iterative.LoopReduceOutputCacheSwitch;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.IndexedSorter;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.QuickSort;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

/** A Reduce task. */
class ReduceTask extends Task {
	private final static int APPROX_HEADER_LENGTH = 150;
	public static final int Reduce_OUTPUT_INDEX_RECORD_LENGTH = 24;
	
	
	static { // register a ctor
		WritableFactories.setFactory(ReduceTask.class, new WritableFactory() {
			public Writable newInstance() {
				return new ReduceTask();
			}
		});
	}
//	***********Leon******************************
	/*protected JOutputBuffer outputBuffer = null;
	protected Class inputKeyClass;
    protected Class inputValClass;
    protected Class outputKeyClass;
    protected Class outputValClass;
    protected Class priorityClass;
	protected Class<? extends CompressionCodec> mapOutputCodecClass = null;
	protected Class<? extends CompressionCodec> reduceOutputCodecClass = null;
	
	private Reporter reporter;*/
//******************************************************************************************
	
	private static final Log LOG = LogFactory
			.getLog(ReduceTask.class.getName());
	private int numReduces;
	private ReduceCopier reduceCopier;
	ReduceOutputBuffer collector1=null;
	private CompressionCodec codec;

	{
		getProgress().setStatus("reduce");
		setPhase(TaskStatus.Phase.SHUFFLE); // phase to start with
	}

	private Progress copyPhase;
	private Progress sortPhase;
	private Progress reducePhase;
	private Counters.Counter reduceShuffleBytes = getCounters().findCounter(
			Counter.REDUCE_SHUFFLE_BYTES);
	private Counters.Counter reduceInputKeyCounter = getCounters().findCounter(
			Counter.REDUCE_INPUT_GROUPS);
	private Counters.Counter reduceInputValueCounter = getCounters()
			.findCounter(Counter.REDUCE_INPUT_RECORDS);
	private Counters.Counter reduceOutputCounter = getCounters().findCounter(
			Counter.REDUCE_OUTPUT_RECORDS);
	private Counters.Counter reduceCombineOutputCounter = getCounters()
			.findCounter(Counter.COMBINE_OUTPUT_RECORDS);

	/**
	 * HaLoop: loop cache switch
	 */
	private LoopReduceCacheSwitch loopCacheControl;

	/**
	 * filter what to cache and what not
	 */
	private LoopReduceCacheFilter loopCacheFilter;

	/**
	 * reduce output cache
	 */
	private FSDataOutputStream reduceOutputCache;

	/**
	 * reduce output index
	 */
	private FSDataOutputStream reduceOutputIndex;

	/**
	 * input from reduce output cache
	 */
	private FSDataInputStream reduceOutputCacheInput;

	/**
	 * input of index from reduce output cache
	 */
	private FSDataInputStream reduceOutputCacheInputIndex;

	/**
	 * reduce output cache switch
	 */
	private LoopReduceOutputCacheSwitch reduceOutputCacheSwitch;

	/**
	 * reduce output cache filter
	 */
	private LoopReduceOutputCacheFilter reduceOutputCacheFilter;

	/**
	 * the result distance measure
	 */
	private IDistanceMeasure resultDistance;

	// A custom comparator for map output files. Here the ordering is determined
	// by the file's size and path. In case of files with same size and
	// different
	// file paths, the first parameter is considered smaller than the second
	// one.
	// In case of files with same size and path are considered equal.
	private Comparator<FileStatus> mapOutputFileComparator = new Comparator<FileStatus>() {
		public int compare(FileStatus a, FileStatus b) {
			if (a.getLen() < b.getLen())
				return -1;
			else if (a.getLen() == b.getLen())
				if (a.getPath().toString().equals(b.getPath().toString()))
					return 0;
				else
					return -1;
			else
				return 1;
		}
	};

	// A sorted set for keeping a set of map output files on disk
	private final SortedSet<FileStatus> mapOutputFilesOnDisk = new TreeSet<FileStatus>(
			mapOutputFileComparator);

	public ReduceTask() {
		super();
	}

	public ReduceTask(String jobFile, TaskAttemptID taskId, int partition,
			int numReduces) {
		super(jobFile, taskId, partition);
		this.numReduces = numReduces;
	}

	private CompressionCodec initCodec() {
		// check if map-outputs are to be compressed
		if (conf.getCompressMapOutput()) {
			Class<? extends CompressionCodec> codecClass = conf
					.getMapOutputCompressorClass(DefaultCodec.class);
			return ReflectionUtils.newInstance(codecClass, conf);
		}

		return null;
	}

	@Override
	public TaskRunner createRunner(TaskTracker tracker, TaskInProgress tip)
			throws IOException {
		return new ReduceTaskRunner(tip, tracker, this.conf);
	}

	@Override
	public boolean isMapTask() {
		return false;
	}

	public int getNumMaps() {
		return numReduces;
	}

	/**
	 * Localize the given JobConf to be specific for this task.
	 */
	@Override
	public void localizeConfiguration(JobConf conf) throws IOException {
		super.localizeConfiguration(conf);
		conf.setNumReduceTasks(numReduces);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);

		out.writeInt(numReduces); // write the number of reduces
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);

		numReduces = in.readInt();
	}

	// Get the input files for the reducer.
	private Path[] getMapFiles(FileSystem fs, boolean isLocal)
			throws IOException {
		List<Path> fileList = new ArrayList<Path>();
		if (isLocal) {
			// for local jobs
			for (int i = 0; i < numReduces; ++i) {
				fileList.add(mapOutputFile.getInputFile(i, getTaskID(), round));
			}
		} else {
			// for non local jobs
			for (FileStatus filestatus : mapOutputFilesOnDisk) {
				fileList.add(filestatus.getPath());
			}
		}
		return fileList.toArray(new Path[0]);
	}

	private class ReduceValuesIterator<KEY, VALUE> extends
			ValuesIterator<KEY, VALUE> {
		public ReduceValuesIterator(RawKeyValueIterator in,
				RawComparator<KEY> comparator, Class<KEY> keyClass,
				Class<VALUE> valClass, Configuration conf, Progressable reporter)
				throws IOException {
			super(in, comparator, keyClass, valClass, conf, reporter);
		}

		@Override
		public VALUE next() {
			reduceInputValueCounter.increment(1);
			return moveToNext();
		}

		protected VALUE moveToNext() {
			return super.next();
		}

		public void informReduceProgress() {
			reducePhase.set(super.in.getProgress().get()); // update progress
			reporter.progress();
		}
	}

	private class SkippingReduceValuesIterator<KEY, VALUE> extends
			ReduceValuesIterator<KEY, VALUE> {
		private SkipRangeIterator skipIt;
		private TaskUmbilicalProtocol umbilical;
		private Counters.Counter skipGroupCounter;
		private Counters.Counter skipRecCounter;
		private long grpIndex = -1;
		private Class<KEY> keyClass;
		private Class<VALUE> valClass;
		private SequenceFile.Writer skipWriter;
		private boolean toWriteSkipRecs;
		private boolean hasNext;
		private TaskReporter reporter;

		public SkippingReduceValuesIterator(RawKeyValueIterator in,
				RawComparator<KEY> comparator, Class<KEY> keyClass,
				Class<VALUE> valClass, Configuration conf,
				TaskReporter reporter, TaskUmbilicalProtocol umbilical)
				throws IOException {
			super(in, comparator, keyClass, valClass, conf, reporter);
			this.umbilical = umbilical;
			this.skipGroupCounter = reporter
					.getCounter(Counter.REDUCE_SKIPPED_GROUPS);
			this.skipRecCounter = reporter
					.getCounter(Counter.REDUCE_SKIPPED_RECORDS);
			this.toWriteSkipRecs = toWriteSkipRecs()
					&& SkipBadRecords.getSkipOutputPath(conf) != null;
			this.keyClass = keyClass;
			this.valClass = valClass;
			this.reporter = reporter;
			skipIt = getSkipRanges().skipRangeIterator();
			mayBeSkip();
		}

		void nextKey() throws IOException {
			super.nextKey();
			mayBeSkip();
		}

		public boolean more() {
			return super.more() && hasNext;
		}

		private void mayBeSkip() throws IOException {
			hasNext = skipIt.hasNext();
			if (!hasNext) {
				LOG.warn("Further groups got skipped.");
				return;
			}
			grpIndex++;
			long nextGrpIndex = skipIt.next();
			long skip = 0;
			long skipRec = 0;
			while (grpIndex < nextGrpIndex && super.more()) {
				while (hasNext()) {
					VALUE value = moveToNext();
					if (toWriteSkipRecs) {
						writeSkippedRec(getKey(), value);
					}
					skipRec++;
				}
				super.nextKey();
				grpIndex++;
				skip++;
			}

			// close the skip writer once all the ranges are skipped
			if (skip > 0 && skipIt.skippedAllRanges() && skipWriter != null) {
				skipWriter.close();
			}
			skipGroupCounter.increment(skip);
			skipRecCounter.increment(skipRec);
			reportNextRecordRange(umbilical, grpIndex);
		}

		@SuppressWarnings("unchecked")
		private void writeSkippedRec(KEY key, VALUE value) throws IOException {
			if (skipWriter == null) {
				Path skipDir = SkipBadRecords.getSkipOutputPath(conf);
				Path skipFile = new Path(skipDir, getTaskID().toString());
				skipWriter = SequenceFile.createWriter(
						skipFile.getFileSystem(conf), conf, skipFile, keyClass,
						valClass, CompressionType.BLOCK, reporter);
			}
			skipWriter.append(key, value);
		}
	}

	// private MemoryMappedFileInput indexInput = null;
	private FSDataInputStream indexInput = null;

	// private MemoryMappedFileInput fileInput = null;
	private FSDataInputStream fileInput = null;

	private boolean inRecovery = false;

	private TaskAttemptID recoveryID = null;

	private boolean recovered = false;

	class RecoveryReporter implements Runnable {

		private Reporter reporter;

		public RecoveryReporter(Reporter reporter) {
			this.reporter = reporter;
		}

		@Override
		public void run() {
			while (!recovered) {
				try {
					reporter.progress();
					// reporter.
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public void run(JobConf job, final TaskUmbilicalProtocol umbilical)
			throws IOException, InterruptedException, ClassNotFoundException {

		long start = System.currentTimeMillis();

		this.umbilical = umbilical;
		job.setBoolean("mapred.skip.on", isSkipping());

		if (isMapOrReduce()) {
			copyPhase = getProgress().addPhase("copy");
			sortPhase = getProgress().addPhase("sort");
			reducePhase = getProgress().addPhase("reduce");
		}
		// start thread that will handle communication with parent
		TaskReporter reporter = new TaskReporter(getProgress(), umbilical);
		reporter.startCommunicationThread();
		boolean useNewApi = job.getUseNewReducer();
		initialize(job, getJobID(), reporter, useNewApi);

		// check if it is a cleanupJobTask
		if (jobCleanup) {
			runJobCleanupTask(umbilical, reporter);
			return;
		}
		if (jobSetup) {
			runJobSetupTask(umbilical, reporter);
			return;
		}
		if (taskCleanup) {
			runTaskCleanupTask(umbilical, reporter);
			return;
		}

		// Initialize the codec
		codec = initCodec();

		/**
		 * HaLoop: initialize loopCacheControl
		 */
		if (job.isIterative() && loopCacheControl == null) {
			Class<? extends LoopReduceCacheSwitch> cacheControl = job
					.getLoopReduceCacheSwitch();
			loopCacheControl = ReflectionUtils.newInstance(cacheControl, job);
		}

		/**
		 * HaLoop: set up loop filter
		 */
		if (job.isIterative() && loopCacheFilter == null) {
			Class<? extends LoopReduceCacheFilter> cacheFilter = job
					.getLoopReduceCacheFilter();
			loopCacheFilter = ReflectionUtils.newInstance(cacheFilter, job);
		}

		/**
		 * HaLoop: initial reducer output cache switch
		 */
		if (job.isIterative() && reduceOutputCacheSwitch == null) {
			Class<? extends LoopReduceOutputCacheSwitch> outputCacheControl = job
					.getLoopReduceOutputCacheSwitch();
			reduceOutputCacheSwitch = ReflectionUtils.newInstance(
					outputCacheControl, job);
		}

		/**
		 * HaLoop: reducer output cache filter
		 */
		if (job.isIterative() && reduceOutputCacheFilter == null) {
			Class<? extends LoopReduceOutputCacheFilter> outputCacheFilter = job
					.getLoopReduceOutputCacheFilter();
			reduceOutputCacheFilter = ReflectionUtils.newInstance(
					outputCacheFilter, job);
		}

		/**
		 * if we need to recover for a dead task tracker
		 */
		if (job.isIterative()
				&& getTaskFailure()
				&& !loopCacheControl.isCacheWritten(job,
						job.getCurrentIteration(), job.getCurrentStep())) {
			this.inRecovery = true;
			int id = 0;
			TaskID tid = this.getTaskID().getTaskID();
			this.recoveryID = new TaskAttemptID(tid, id);

			RecoverReducerTask rrt = new RecoverReducerTask(this.getJobFile(),
					recoveryID, this.getPartition(), this.numReduces);
			TaskUmbilicalProtocol ut = new DummyTaskUmbilicalProtocol();
			JobConf recoverJob = job.duplicate();
			rrt.setConf(recoverJob);
			rrt.setCurrentIteration(this.iteration);
			rrt.setCurrentStep(this.step);
			rrt.setRecoverFromTaskTracker(this.getRecoverFromTaskTracker());
			rrt.setRound();
			rrt.initialize(recoverJob, getJobID(), reporter, useNewApi);

			// keep reporting, to avoid another speculative execution
			RecoveryReporter rreporter = new RecoveryReporter(reporter);
			Thread reportThread = new Thread(rreporter);
			reportThread.start();
			// run recovery
			rrt.run(recoverJob, ut);
			this.recovered = true;
		}

		/*--------- Open Memory Files ---------------*/
		if (job.isIterative()
				&& loopCacheControl.isCacheRead(job, iteration, step)) {
			/**
			 * find the latest cached iteration/step
			 */
			int numSteps = job.getNumberOfLoopBodySteps();
			int cachedIteration = iteration;
			int cachedStep = step;
			for (int latest = round; latest >= 0; latest--) {
				if (cachedStep > 0) {
					cachedStep--;
				} else {
					cachedStep = numSteps - 1;
					cachedIteration--;
				}
				if (loopCacheControl.isCacheWritten(job, cachedIteration,
						cachedStep))
					break;
			}

			int pass = cachedIteration * numSteps + cachedStep;
			System.out.println("cached pass " + pass);
			Path filePath;
			Path indexFilePath;
			if (!inRecovery) {
				filePath = mapOutputFile.getReduceCacheFileForWrite(
						getTaskID(), -1, pass);
				indexFilePath = mapOutputFile.getCacheIndexFileForWrite(
						getTaskID(), -1, pass);
			} else {
				filePath = mapOutputFile.getReduceCacheFileForWrite(
						this.recoveryID, -1, pass);
				indexFilePath = mapOutputFile.getCacheIndexFileForWrite(
						this.recoveryID, -1, pass);
			}

			/*********************** reset the file input stream ****************/
			FileSystem localFs = FileSystem.getLocal(conf);
			FileSystem lfs = ((LocalFileSystem) localFs).getRaw();

			try {
				fileInput = lfs.open(filePath);
				indexInput = lfs.open(indexFilePath);
			} catch (Exception e) {
				this.inRecovery = true;
				int id = 0;
				TaskID tid = this.getTaskID().getTaskID();
				this.recoveryID = new TaskAttemptID(tid, id);

				RecoverReducerTask rrt = new RecoverReducerTask(
						this.getJobFile(), recoveryID, this.getPartition(),
						this.numReduces);
				TaskUmbilicalProtocol ut = new DummyTaskUmbilicalProtocol();
				JobConf recoverJob = job.duplicate();
				rrt.setConf(recoverJob);
				rrt.setCurrentIteration(this.iteration);
				rrt.setCurrentStep(this.step);
				rrt.setRecoverFromTaskTracker(this.getRecoverFromTaskTracker());
				rrt.setRound();
				rrt.initialize(recoverJob, getJobID(), reporter, useNewApi);

				// keep reporting, to avoid another speculative execution
				RecoveryReporter rreporter = new RecoveryReporter(reporter);
				Thread reportThread = new Thread(rreporter);
				reportThread.start();
				// run recovery
				rrt.run(recoverJob, ut);
				this.recovered = true;
				this.inRecovery = false;
				fileInput = lfs.open(filePath);
				indexInput = lfs.open(indexFilePath);
			}
		}

		/**
		 * if it is an iterative job and reduce output cache read enabled
		 */
		if (job.isIterative()
				&& this.reduceOutputCacheSwitch.isCacheRead(job, iteration,
						step)) {
			/**
			 * find the latest cached iteration/step
			 */
			int numSteps = job.getNumberOfLoopBodySteps();
			int cachedIteration = iteration;
			int cachedStep = step;
			for (int latest = round; latest >= 0; latest--) {
				if (cachedStep > 0) {
					cachedStep--;
				} else {
					cachedStep = numSteps - 1;
					cachedIteration--;
				}
				if (reduceOutputCacheSwitch.isCacheWritten(job,
						cachedIteration, cachedStep))
					break;
			}

			int pass = cachedIteration * numSteps + cachedStep;

			Path filePath;
			Path indexFilePath;

			filePath = mapOutputFile.getReduceOutputCacheFileForWrite(
					getTaskID(), -1, pass);
			indexFilePath = mapOutputFile.getOutputCacheIndexFileForWrite(
					getTaskID(), -1, pass);

			/*********************** reset the file input stream ****************/
			FileSystem localFs = FileSystem.getLocal(conf);
			FileSystem lfs = ((LocalFileSystem) localFs).getRaw();

			System.out.println("read from cache " + filePath);
			this.reduceOutputCacheInput = lfs.open(filePath);
			this.reduceOutputCacheInputIndex = lfs.open(indexFilePath);
		}

		if (job.isIterative()
				&& this.reduceOutputCacheSwitch.isCacheWritten(job, iteration,
						step)) {
			int numSteps = job.getNumberOfLoopBodySteps();
			int pass = iteration * numSteps + step;

			Path filePath;
			Path indexFilePath;

			filePath = mapOutputFile.getReduceOutputCacheFileForWrite(
					getTaskID(), -1, pass);
			indexFilePath = mapOutputFile.getOutputCacheIndexFileForWrite(
					getTaskID(), -1, pass);

			/*********************** reset the file input stream ****************/
			FileSystem localFs = FileSystem.getLocal(conf);
			FileSystem lfs = ((LocalFileSystem) localFs).getRaw();

			this.reduceOutputCache = lfs.create(filePath);
			this.reduceOutputIndex = lfs.create(indexFilePath);
			System.out.println("write to cache " + filePath);
		}

		boolean isLocal = "local"
				.equals(job.get("mapred.job.tracker", "local"));
		if (!isLocal) {
			reduceCopier = new ReduceCopier(umbilical, job, reporter);
			if (!reduceCopier.fetchOutputs()) {
				if (reduceCopier.mergeThrowable instanceof FSError) {
					throw (FSError) reduceCopier.mergeThrowable;
				}
				throw new IOException("Task: " + getTaskID()
						+ " - The reduce copier failed",
						reduceCopier.mergeThrowable);
			}
		}
		copyPhase.complete(); // copy is already complete
		setPhase(TaskStatus.Phase.SORT);
		statusUpdate(umbilical);

		final FileSystem rfs = FileSystem.getLocal(job).getRaw();
		RawKeyValueIterator rIter = isLocal ? Merger.merge(job, rfs, job
				.getOutputKeyClass(), job.getOutputValueClass(), codec,
				getMapFiles(rfs, true), !conf.getKeepFailedTaskFiles(), job
						.getInt("io.sort.factor", 100), new Path(getTaskID()
						.toString()), job.getOutputKeyComparator(), reporter,
				spilledRecordsCounter, null) : reduceCopier.createKVIterator(
				job, rfs, reporter);

		// free up the data structures
		mapOutputFilesOnDisk.clear();

		sortPhase.complete(); // sort is complete
		setPhase(TaskStatus.Phase.REDUCE);
		statusUpdate(umbilical);
		Class keyClass = job.getMapOutputKeyClass();
		Class valueClass = job.getMapOutputValueClass();
		RawComparator comparator = job.getOutputValueGroupingComparator();

		if (useNewApi) {
			runNewReducer(job, umbilical, reporter, rIter, comparator,
					keyClass, valueClass);
		} else {
			runOldReducer(job, umbilical, reporter, rIter, comparator,
					keyClass, valueClass);
		}
		done(umbilical, reporter);

		long end = System.currentTimeMillis();

		System.out.println("reducer running time overall: " + (end - start)
				+ "ms");
	}

	/**
	 * default value for cache flag is true
	 */
	private boolean cacheFlag = true;

	private int cachedIndexRecords = 3000;

	private int cachedKeys = 300;

	private int numRead = 0;

	private List cachedKeyList = new ArrayList();

	private int numValues = 0;

	private float distance = 0f;

	/**
	 * HaLoop: reducer output collector
	 * 
	 * @author yingyib
	 * 
	 * @param <K>
	 * @param <V>
	 */
	class ReducerOutputCollector<K, V> implements OutputCollector<K, V> {
		private RecordWriter<K, V> out;
		private TaskReporter reporter;

		private SerializationFactory serializationFactory = new SerializationFactory(
				ReduceTask.this.conf);

		private Serializer<V> valSerializer;
		private Serializer<K> indexKeySerializer;
		private Serializer<LongWritable> indexPositionSerializer;
		private Serializer<IntWritable> sizeSerializer;

		private Deserializer<V> valDeserializer;
		private Deserializer<K> indexKeyDeserializer;
		private Deserializer<LongWritable> indexPositionDeserializer;
		private Deserializer<IntWritable> sizeDeserializer;

		private K currentKey;
		private V currentValue;
		private LongWritable currentPos = new LongWritable(0);
		private IntWritable currentLength = new IntWritable(0);
		boolean available;

		public ReducerOutputCollector(RecordWriter<K, V> out,
				TaskReporter reporter) {

			indexPositionSerializer = serializationFactory
					.getSerializer(LongWritable.class);
			sizeSerializer = serializationFactory
					.getSerializer(IntWritable.class);
			resultDistance = ReflectionUtils.newInstance(
					conf.getDistanceMeasure(), conf);

			this.out = out;
			this.reporter = reporter;
		}

		public void collect(K key, V value) throws IOException {

			if (conf.isIterative()
					&& (reduceOutputCacheSwitch.isCacheRead(conf, iteration,
							step) || reduceOutputCacheSwitch.isCacheWritten(
							conf, iteration, step))) {
				if (conf.isIterative()
						&& ReduceTask.this.reduceOutputCacheSwitch
								.isCacheWritten(conf, iteration, step)
						&& valSerializer == null) {
					valSerializer = serializationFactory
							.getSerializer((Class<V>) value.getClass());
					indexKeySerializer = serializationFactory
							.getSerializer((Class<K>) key.getClass());

					/**
					 * open serializers: for writting cache to disk
					 */
					valSerializer.open(ReduceTask.this.reduceOutputCache);
					indexKeySerializer.open(ReduceTask.this.reduceOutputIndex);
					indexPositionSerializer
							.open(ReduceTask.this.reduceOutputIndex);
					sizeSerializer.open(ReduceTask.this.reduceOutputIndex);
				}

				if (conf.isIterative()
						&& reduceOutputCacheSwitch.isCacheRead(conf, iteration,
								step) && indexKeyDeserializer == null) {
					valDeserializer = serializationFactory
							.getDeserializer((Class<V>) value.getClass());
					indexKeyDeserializer = serializationFactory
							.getDeserializer((Class<K>) key.getClass());
					indexPositionDeserializer = serializationFactory
							.getDeserializer(LongWritable.class);
					sizeDeserializer = serializationFactory
							.getDeserializer(IntWritable.class);

					/**
					 * open deserializers: for reading cache from disk
					 */
					valDeserializer
							.open(ReduceTask.this.reduceOutputCacheInput);
					indexKeyDeserializer
							.open(ReduceTask.this.reduceOutputCacheInputIndex);
					indexPositionDeserializer
							.open(ReduceTask.this.reduceOutputCacheInputIndex);
					sizeDeserializer
							.open(ReduceTask.this.reduceOutputCacheInputIndex);

					/**
					 * create key and value instance
					 */
					currentKey = ReflectionUtils.newInstance(
							(Class<K>) key.getClass(), conf);
					currentValue = ReflectionUtils.newInstance(
							(Class<V>) value.getClass(), conf);
				}

				if (conf.isIterative()
						&& reduceOutputCacheSwitch.isCacheWritten(conf,
								iteration, step)) {
					long start = reduceOutputCache.getPos();
					valSerializer.serialize(value);
					long end = reduceOutputCache.getPos();
					int size = (int) (end - start);

					currentPos.set(start);
					currentLength.set(size);

					indexKeySerializer.serialize(key);
					indexPositionSerializer.serialize(currentPos);
					sizeSerializer.serialize(currentLength);
				}

				if (conf.isIterative()
						&& reduceOutputCacheSwitch.isCacheRead(conf, iteration,
								step)) {
					do {
						available = nextKey();
					} while (!currentKey.equals(key) && available);
					if (available)
						distance += resultDistance.getDistance(key, value,
								currentKey, currentValue);
				}
			}

			out.write(key, value);
			reduceOutputCounter.increment(1);
			// indicate that progress update needs to be sent
			reporter.progress();
		}

		private boolean nextKey() throws IOException {
			if (reduceOutputCacheInput.available() > 0) {
				indexKeyDeserializer.deserialize(currentKey);
				indexPositionDeserializer.deserialize(currentPos);
				sizeDeserializer.deserialize(currentLength);
				reduceOutputCacheInput.seek(currentPos.get()
						+ currentLength.get());
				valDeserializer.deserialize(currentValue);
				return true;
			} else
				return false;
		}
	}

	/**
	 * persist the distance to hdfs
	 */
	private void persistentDistance() {
		try {
			FileSystem fs = FileSystem.get(conf);
			Path path = new Path(MRConstants.SCHEDULE_LOG_DIR + "/"
					+ this.getJobID() + "/distance/" + this.getPartition());
			FSDataOutputStream dist = fs.create(path);
			dist.writeFloat(distance);
			dist.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * HaLoop reduce cache logic is mainly in this method
	 * 
	 * @param <INKEY>
	 * @param <INVALUE>
	 * @param <OUTKEY>
	 * @param <OUTVALUE>
	 * @param job
	 * @param umbilical
	 * @param reporter
	 * @param rIter
	 * @param comparator
	 * @param keyClass
	 * @param valueClass
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	private <INKEY, INVALUE, OUTKEY, OUTVALUE> void runOldReducer(JobConf job,
			TaskUmbilicalProtocol umbilical, final TaskReporter reporter,
			RawKeyValueIterator rIter, RawComparator<INKEY> comparator,
			Class<INKEY> keyClass, Class<INVALUE> valueClass)
			throws IOException {
		System.out.println("job file: " + getJobFile());

		/**
		 * cached keys that are kept in memory
		 */
		cachedKeys = cachedIndexRecords / 10;

		/**
		 * cached key list in memory
		 */
		for (int i = 0; i < cachedKeys; i++) {
			INKEY key = ReflectionUtils.newInstance(keyClass, conf);
			cachedKeyList.add(key);
		}

		// print for debug purpose
		System.out.println("round: " + round + " partition: " + getPartition());
		System.out.println("conf iteration: " + job.getCurrentIteration()
				+ " step: " + job.getCurrentStep());

		Reducer<INKEY, INVALUE, OUTKEY, OUTVALUE> reducer = ReflectionUtils
				.newInstance(job.getReducerClass(), job);
		reducer.configure(job);

		// make output collector
		String finalName = getOutputName(getPartition(), round);
        //LOG.info("@@@@@@@@@@@@@@@@@123"+finalName.toString());
		FileSystem fs = FileSystem.get(job);
		RecordWriter<OUTKEY, OUTVALUE> out =null;
		
		/*final RecordWriter<OUTKEY, OUTVALUE> out = job.getOutputFormat()
				.getRecordWriter(fs, job, finalName, reporter);
		OutputCollector<OUTKEY, OUTVALUE> collector = new ReducerOutputCollector<OUTKEY, OUTVALUE>(
				out, reporter);*/

		// apply reduce function
		try {
			// increment processed counter only if skipping feature is enabled
			boolean incrProcCount = SkipBadRecords.getReducerMaxSkipGroups(job) > 0
					&& SkipBadRecords.getAutoIncrReducerProcCount(job);

			ReduceValuesIterator<INKEY, INVALUE> values = isSkipping() ? new SkippingReduceValuesIterator<INKEY, INVALUE>(
					rIter, comparator, keyClass, valueClass, job, reporter,
					umbilical) : new ReduceValuesIterator<INKEY, INVALUE>(
					rIter, job.getOutputValueGroupingComparator(), keyClass,
					valueClass, job, reporter);
			values.informReduceProgress();
			
			/********************* HaLoop code ********************************/
			List<INKEY> keyList = new ArrayList<INKEY>();
			long writePos = 0;
			SerializationFactory serializationFactory = new SerializationFactory(
					job);
			Serializer<INVALUE> valSerializer = serializationFactory
					.getSerializer(valueClass);
			Serializer<INKEY> indexKeySerializer = serializationFactory
					.getSerializer(keyClass);
			Serializer<LongWritable> indexPositionSerializer = serializationFactory
					.getSerializer(LongWritable.class);
			Serializer<IntWritable> sizeSerializer = serializationFactory
					.getSerializer(IntWritable.class);

			FSDataOutputStream fileOutput = new FSDataOutputStream(null);
			FSDataOutputStream indexOutput = new FSDataOutputStream(null);
			
			if (job.isIterative()
					&& loopCacheControl.isCacheWritten(job, iteration, step)) {
				Path filePath;
				Path indexPath;
				if (!this.inRecovery) {
					filePath = mapOutputFile.getReduceCacheFileForWrite(
							getTaskID(), -1, round);
					indexPath = mapOutputFile.getCacheIndexFileForWrite(
							getTaskID(), -1, round);
				} else {
					filePath = mapOutputFile.getReduceCacheFileForWrite(
							this.recoveryID, -1, round);
					indexPath = mapOutputFile.getCacheIndexFileForWrite(
							this.recoveryID, -1, round);
				}

				FileSystem localFs = FileSystem.getLocal(conf);
				FileSystem lfs = ((LocalFileSystem) localFs).getRaw();

				fileOutput = lfs.create(filePath);
				indexOutput = lfs.create(indexPath);

				valSerializer.open(fileOutput);
				indexKeySerializer.open(indexOutput);
				indexPositionSerializer.open(indexOutput);
				sizeSerializer.open(indexOutput);
			}

			HashMap<INKEY, LongWritable> keyPos = new HashMap<INKEY, LongWritable>();
			HashMap<INKEY, LongWritable> keyNextPos = new HashMap<INKEY, LongWritable>();
			HashMap<INKEY, IntWritable> keySize = new HashMap<INKEY, IntWritable>();

			Deserializer<INKEY> indexKeyDeserializer = serializationFactory
					.getDeserializer(keyClass);
			Deserializer<LongWritable> indexPositionDeserializer = serializationFactory
					.getDeserializer(LongWritable.class);
			Deserializer<IntWritable> sizeDeserializer = serializationFactory
					.getDeserializer(IntWritable.class);

			INKEY currentLargest = null;
			
			if (job.isIterative()
					&& loopCacheControl.isCacheRead(job, iteration, step)) {
				indexKeyDeserializer.open(indexInput);
				indexPositionDeserializer.open(indexInput);
				sizeDeserializer.open(indexInput);

				/************ load index **************/
				currentLargest = loadPartialIndex(job, keyClass, keyPos,
						keyNextPos, keySize, indexInput, indexKeyDeserializer,
						indexPositionDeserializer, sizeDeserializer, keyList);

			}
			/***********************************************************************/
			
			DataOutputBuffer bb = new DataOutputBuffer();
			DataInputBuffer ib = new DataInputBuffer();
			Serializer<INVALUE> ssl = serializationFactory
					.getSerializer(valueClass);
			Deserializer<INVALUE> dsl = serializationFactory
					.getDeserializer(valueClass);
			
			ssl.open(bb);
			dsl.open(ib);
			
			/************* open cached file *************/
			Deserializer<INVALUE> valDeserializer = null;
			if (job.isIterative()
					&& loopCacheControl.isCacheRead(job, iteration, step)&&step==1) {
				valDeserializer = serializationFactory
						.getDeserializer(valueClass);
				valDeserializer.open(fileInput);
			}
			
			long iterationStart = System.currentTimeMillis();
			reduceTime = 0;
			long smallTime = 0;
			
			INVALUE cacheValue = ReflectionUtils.newInstance(valueClass, job);

			List<INVALUE> emptyList = new ArrayList<INVALUE>();
			System.out.println("reducer class name: "
					+ reducer.getClass().getName());
			
            //*****************************************************************leon*************			
			//while (values.more()) {
				long starttime = System.currentTimeMillis();
			
				
				if (conf.isIterative()
						&& loopCacheControl
								.isCacheWritten(job, iteration, step)&&conf.getNumIterations()!=iteration) {
					LOG.info("round:   01   conf.isIterative()" );
					// write cache, in a loop job
					while(values.more()){
						reduceInputKeyCounter.increment(1);
						INKEY key;
					key = values.getKey();
					indexKeySerializer.serialize(key);
					indexPositionSerializer
							.serialize(new LongWritable(writePos));

					numValues = 0;
					CacheWriterIterator<INKEY, INVALUE> valueIterator = new CacheWriterIterator<INKEY, INVALUE>(
							key, valSerializer, values);

					// System.out.println("call reducer function here: ");
					try {
						collector1 = new ReduceOutputBuffer(umbilical, job, reporter);
					} catch (ClassNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					//reducer.reduce(key, valueIterator, collector1, reporter);
					System.out.println("#########"+key.getClass().toString()+"********");
					reducer.reduce(key, valueIterator, collector1, reporter);
					writePos = fileOutput.getPos();
					try {
						collector1.flush();
					} catch (ClassNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					if (incrProcCount) {
						reporter.incrCounter(SkipBadRecords.COUNTER_GROUP,
								SkipBadRecords.COUNTER_REDUCE_PROCESSED_GROUPS, 1);
					}
					//LOG.info("round:   05" );
					values.nextKey();
					values.informReduceProgress();
					long endtime = System.currentTimeMillis();
					reduceTime += (endtime - starttime);
				}
				} else if (!conf.isIterative()) {
					LOG.info("round:   02   !conf.isIterative()" );
					  out = job.getOutputFormat()
					.getRecordWriter(fs, job, finalName, reporter);
			OutputCollector<OUTKEY, OUTVALUE> collector = new ReducerOutputCollector<OUTKEY, OUTVALUE>(
					out, reporter);
					while(values.more()){
						reduceInputKeyCounter.increment(1);
						//INKEY key;
					// usual job: none-loop traditional hadoop job
					reducer.reduce(values.getKey(), values, collector, reporter);
					//LOG.info("round:   03"+values.getKey().toString() );
					if (incrProcCount) {
						reporter.incrCounter(SkipBadRecords.COUNTER_GROUP,
								SkipBadRecords.COUNTER_REDUCE_PROCESSED_GROUPS, 1);
					}
					//LOG.info("round:   05" );
					values.nextKey();
					values.informReduceProgress();
					long endtime = System.currentTimeMillis();
					reduceTime += (endtime - starttime);
					}
					LOG.info("round:   04" );
	
//***********************************leon*******************************8					
				}else if(conf.isIterative()&&(conf.getNumIterations()-1)==iteration){
					LOG.info("conf.isIterative()&&conf.getNumIterations()==iteration" );
					  out = job.getOutputFormat()
					.getRecordWriter(fs, job, finalName, reporter);
			OutputCollector<OUTKEY, OUTVALUE> collector = new ReducerOutputCollector<OUTKEY, OUTVALUE>(
					out, reporter);
					while(values.more()){
						reduceInputKeyCounter.increment(1);
						//INKEY key;
					// usual job: none-loop traditional hadoop job
					reducer.reduce(values.getKey(), values, collector, reporter);
					 //LOG.info("round:   03"+values.getKey().toString() );
					if (incrProcCount) {
						reporter.incrCounter(SkipBadRecords.COUNTER_GROUP,
								SkipBadRecords.COUNTER_REDUCE_PROCESSED_GROUPS, 1);
					}
					//LOG.info("round:   04" );
					values.nextKey();
					values.informReduceProgress();
					long endtime = System.currentTimeMillis();
					reduceTime += (endtime - starttime);
					}
					//LOG.info("round:   05" );
				} 
//*********************************leon***************************************************				
		         else {
					LOG.info("round:   else" +conf.getNumIterations()+"    "+iteration);
					 /*out = job.getOutputFormat()
					.getRecordWriter(fs, job, finalName, reporter);
			OutputCollector<OUTKEY, OUTVALUE> collector = new ReducerOutputCollector<OUTKEY, OUTVALUE>(
					out, reporter);*/
					// read cache, in a loop job
					try {
						collector1 = new ReduceOutputBuffer(umbilical, job, reporter);
						
					} catch (ClassNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					while(values.more()){
						reduceInputKeyCounter.increment(1);
						
				
					/*if (loopCacheControl.isCacheRead(job, iteration, step))
						cacheFlag = true;
					else
						cacheFlag = false;*/

					// read the cache and call reduce function
						
					INKEY searchKey = values.getKey();
					//LOG.info("ReduceTask" +" value: "+searchKey);
					/*Iterator<INVALUE> cacheIterator = new VIterator<INVALUE>(
							null, -2, null, null);

					if (cacheFlag) {

//						------------  load partial index --------------
						while (comparator.compare(searchKey, currentLargest) > 0
								&& !isEnd) {

							currentLargest = loadPartialIndex(job, keyClass,
									keyPos, keyNextPos, keySize, indexInput,
									indexKeyDeserializer,
									indexPositionDeserializer,
									sizeDeserializer, keyList);
						}

//						------------  load actual data   --------------
						LongWritable searchKeyPos = keyPos.get(searchKey);
						if (searchKeyPos != null) {
							long start = searchKeyPos.get();
							long end = keyNextPos.get(searchKey).get();

							fileInput.seek(start);

							cacheIterator = new VIterator<INVALUE>(
									valDeserializer, (int) end, fileInput,
									cacheValue);

						}
					}*/

					// merge the two iterator, and call reduce function
					reducer.reduce(searchKey, values,collector1, reporter);
					
					//LOG.info("round:   04" );
					values.nextKey();
					values.informReduceProgress();
				}
					try {
						collector1.flush();
					} catch (ClassNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}finally{
						collector1.close();
					}
					//LOG.info("round:   05" );
					if (incrProcCount) {
						reporter.incrCounter(SkipBadRecords.COUNTER_GROUP,
								SkipBadRecords.COUNTER_REDUCE_PROCESSED_GROUPS, 1);
					}
					
					long endtime = System.currentTimeMillis();
					reduceTime += (endtime - starttime);
				}

				/*if (incrProcCount) {
					reporter.incrCounter(SkipBadRecords.COUNTER_GROUP,
							SkipBadRecords.COUNTER_REDUCE_PROCESSED_GROUPS, 1);
				}
				LOG.info("round:   05" );
				values.nextKey();
				values.informReduceProgress();
				long endtime = System.currentTimeMillis();
				reduceTime += (endtime - starttime);*/
			//}  	
//*****************************************************************leon*************
			/*//int a=0;
			while (values.more()) {
			//	a++;
				long starttime = System.currentTimeMillis();
				reduceInputKeyCounter.increment(1);
				INKEY key;
				

				// Yingyi's code: build the cache;
				if (conf.isIterative()
						&& loopCacheControl
								.isCacheWritten(job, iteration, step)) {
					// write cache, in a loop job
					key = values.getKey();
					indexKeySerializer.serialize(key);
					indexPositionSerializer
							.serialize(new LongWritable(writePos));

					numValues = 0;
					CacheWriterIterator<INKEY, INVALUE> valueIterator = new CacheWriterIterator<INKEY, INVALUE>(
							key, valSerializer, values);

					// System.out.println("call reducer function here: ");
					try {
						collector1 = new ReduceOutputBuffer(umbilical, job, reporter);
					} catch (ClassNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					//reducer.reduce(key, valueIterator, collector1, reporter);
					System.out.println("#########"+key.getClass().toString()+"********");
					reducer.reduce(key, valueIterator, collector1, reporter);
					writePos = fileOutput.getPos();
					try {
						collector1.flush();
					} catch (ClassNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				} else if (!conf.isIterative()) {
					// usual job: none-loop traditional hadoop job
					reducer.reduce(values.getKey(), values, collector, reporter);
					LOG.info("round:   03  "+values.getKey().toString()+"  a:" );
				} else {
					// read cache, in a loop job
					if (loopCacheControl.isCacheRead(job, iteration, step))
						cacheFlag = true;
					else
						cacheFlag = false;

					// read the cache and call reduce function
					INKEY searchKey = values.getKey();
					Iterator<INVALUE> cacheIterator = new VIterator<INVALUE>(
							null, -2, null, null);

					if (cacheFlag) {

						//------------  load partial index --------------
						while (comparator.compare(searchKey, currentLargest) > 0
								&& !isEnd) {

							currentLargest = loadPartialIndex(job, keyClass,
									keyPos, keyNextPos, keySize, indexInput,
									indexKeyDeserializer,
									indexPositionDeserializer,
									sizeDeserializer, keyList);
						}

						//------------  load actual data   --------------
						LongWritable searchKeyPos = keyPos.get(searchKey);
						if (searchKeyPos != null) {
							long start = searchKeyPos.get();
							long end = keyNextPos.get(searchKey).get();

							fileInput.seek(start);

							cacheIterator = new VIterator<INVALUE>(
									valDeserializer, (int) end, fileInput,
									cacheValue);

						}
					}

					// merge the two iterator, and call reduce function
					reducer.reduce(searchKey, new CombineIterator(values,
							cacheIterator), collector, reporter);

				}

				if (incrProcCount) {
					reporter.incrCounter(SkipBadRecords.COUNTER_GROUP,
							SkipBadRecords.COUNTER_REDUCE_PROCESSED_GROUPS, 1);
				}
				values.nextKey();
				values.informReduceProgress();
				long endtime = System.currentTimeMillis();
				reduceTime += (endtime - starttime);
			}*/

				
			if (conf.isIterative() == true
					&& loopCacheControl.isCacheWritten(job, iteration, step)) {
				fileOutput.close();
				indexOutput.close();
			}

			if (conf.isIterative() == true
					&& loopCacheControl.isCacheRead(job, iteration, step)) {
				indexInput.close();
				fileInput.close();
			}

			if (conf.isIterative() == true
					&& this.reduceOutputCacheSwitch.isCacheRead(job, iteration,
							step)) {
				reduceOutputCacheInput.close();
				reduceOutputCacheInputIndex.close();
			}

			if (conf.isIterative() == true
					&& this.reduceOutputCacheSwitch.isCacheWritten(job,
							iteration, step)) {
				reduceOutputCache.close();
				reduceOutputIndex.close();
			}

			// write reduce-local distance
			this.persistentDistance();
			//LOG.info("round:   06" );
			System.out.println("iteration " + round + ":" + reduceTime + "ms");
			System.out.println("branch time: " + smallTime);

			long iterationEnd = System.currentTimeMillis();
			System.out.println("total time: " + (iterationEnd - iterationStart)
					+ "ms");
			cachedKeyList.clear();

			// Clean up: repeated in catch block below
			reducer.close();
			if(out!=null){
			out.close(reporter);
			}
			//LOG.info("round:   end" );
			// End of clean up.
		} catch (IOException ioe) {
			try {
				reducer.close();
			} catch (IOException ignored) {
			}

			try {
				if(out!=null){
				out.close(reporter);
				}
			} catch (IOException ignored) {
			}

			throw ioe;
		}
	}

	private class CacheWriterIterator<K, T> extends ValuesIterator<K, T> {
		Serializer<T> serializer = null;

		ValuesIterator<K, T> values = null;
		int count = 0;
		K key = null;

		public CacheWriterIterator(K k, Serializer<T> ser, Iterator<T> vals) {
			key = k;
			serializer = ser;
			values = (ValuesIterator<K, T>) vals;
		}

		public T next() {
			numValues++;
			T value = values.next();
			count++;

			try {
				if (loopCacheFilter.isCache(key, value, count))
					serializer.serialize(value);
				return value;
			} catch (IOException e) {
				e.printStackTrace();
				return null;
			}
		}

		public boolean more() {
			return values.more();
		}

		public boolean hasNext() {
			if (serializer == null || values == null)
				return false;
			return values.hasNext();
		}

		public void remove() {
		}
	}

	private class VIterator<T> implements Iterator<T> {
		Deserializer<T> deserializer = null;
		int keyEnd = -1;
		FSDataInputStream input = null;
		T cacheValue = null;

		public VIterator(Deserializer<T> des, int end, FSDataInputStream in,
				T cache) throws IOException {
			deserializer = des;
			keyEnd = end;
			input = in;
			cacheValue = cache;
		}

		@Override
		public T next() {
			try {
				deserializer.deserialize(cacheValue);
				return cacheValue;
			} catch (IOException e) {
				e.printStackTrace();
				return null;
			}
		}

		@Override
		public boolean hasNext() {
			// TODO Auto-generated method stub
			try {
				if (deserializer == null)
					return false;
				if (keyEnd == -1)
					keyEnd = input.available();
				return (int) input.getPos() < keyEnd;
			} catch (Exception e) {
				e.printStackTrace();
				return false;
			}
		}

		@Override
		public void remove() {
			// TODO Auto-generated method stub

		}
	}

	private long reduceTime = 0;

	private boolean isEnd = false;

	private <INKEY> INKEY loadPartialIndex(JobConf job, Class<INKEY> keyClass,
			HashMap<INKEY, LongWritable> keyPos,
			HashMap<INKEY, LongWritable> keyNextPos,
			HashMap<INKEY, IntWritable> keySize, FSDataInputStream indexInput,
			Deserializer<INKEY> indexKeyDeserializer,
			Deserializer<LongWritable> indexPositionDeserializer,
			Deserializer<IntWritable> sizeDeserializer, List<INKEY> keyList)
			throws IOException {
		this.numRead++;

		LongWritable pos;
		INKEY nextKey;
		LongWritable nextPos;
		IntWritable valueSize;
		indexKeyDeserializer.open(indexInput);
		indexPositionDeserializer.open(indexInput);
		sizeDeserializer.open(indexInput);

		keyNextPos.clear();
		keyPos.clear();
		keyList.clear();
		keySize.clear();

		INKEY currentKey = null;
		int target = cachedKeyList.size() - 1;
		int currentNum = 0;
		long backPos = 0;

		try {
			// nextKey = ReflectionUtils.newInstance(keyClass, job);
			nextKey = (INKEY) cachedKeyList.get(currentNum);
			// valueSize = new IntWritable();
			nextPos = new LongWritable(0);
			if (indexInput.available() > 0) {
				indexKeyDeserializer.deserialize(nextKey);
				indexPositionDeserializer.deserialize(nextPos);
				// sizeDeserializer.deserialize(valueSize);
			}
			while (indexInput.available() > 0 && currentNum < target) {
				// System.out.println(nextKey + " " + nextPos + " " + " size "
				// + valueSize);
				currentKey = nextKey;
				pos = nextPos;

				keyPos.put(currentKey, pos);
				// keySize.put(currentKey, valueSize);
				keyList.add(currentKey);
				// System.out.println("1 current key "+currentKey);
				currentNum++;

				// nextKey = ReflectionUtils.newInstance(keyClass, job);
				nextKey = (INKEY) cachedKeyList.get(currentNum);
				nextPos = new LongWritable();
				// valueSize = new IntWritable();

				backPos = indexInput.getPos();
				indexKeyDeserializer.deserialize(nextKey);
				indexPositionDeserializer.deserialize(nextPos);
				// sizeDeserializer.deserialize(valueSize);

				keyNextPos.put(currentKey, nextPos);
			}

			keyPos.put(nextKey, nextPos);

			keyList.add(nextKey);
			currentNum++;

			if (indexInput.available() > 0) {
				indexKeyDeserializer.deserialize(currentKey);
				indexPositionDeserializer.deserialize(nextPos);

				keyNextPos.put(nextKey, nextPos);
				indexInput.seek(backPos);
				// System.out.println("2 largest: "+nextKey);
				return nextKey;
			} else {
				keyNextPos.put(nextKey, new LongWritable(-1));
				isEnd = true;
				return nextKey;
			}

		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	static class NewTrackingRecordWriter<K, V> extends
			org.apache.hadoop.mapreduce.RecordWriter<K, V> {
		private final org.apache.hadoop.mapreduce.RecordWriter<K, V> real;
		private final org.apache.hadoop.mapreduce.Counter outputRecordCounter;

		NewTrackingRecordWriter(
				org.apache.hadoop.mapreduce.RecordWriter<K, V> real,
				org.apache.hadoop.mapreduce.Counter recordCounter) {
			this.real = real;
			this.outputRecordCounter = recordCounter;
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException,
				InterruptedException {
			real.close(context);
		}

		@Override
		public void write(K key, V value) throws IOException,
				InterruptedException {
			real.write(key, value);
			outputRecordCounter.increment(1);
		}
	}

	@SuppressWarnings("unchecked")
	private <INKEY, INVALUE, OUTKEY, OUTVALUE> void runNewReducer(JobConf job,
			final TaskUmbilicalProtocol umbilical, final TaskReporter reporter,
			RawKeyValueIterator rIter, RawComparator<INKEY> comparator,
			Class<INKEY> keyClass, Class<INVALUE> valueClass)
			throws IOException, InterruptedException, ClassNotFoundException {
		// wrap value iterator to report progress.
		final RawKeyValueIterator rawIter = rIter;
		rIter = new RawKeyValueIterator() {
			public void close() throws IOException {
				rawIter.close();
			}

			public DataInputBuffer getKey() throws IOException {
				return rawIter.getKey();
			}

			public Progress getProgress() {
				return rawIter.getProgress();
			}

			public DataInputBuffer getValue() throws IOException {
				return rawIter.getValue();
			}

			public boolean next() throws IOException {
				boolean ret = rawIter.next();
				reducePhase.set(rawIter.getProgress().get());
				reporter.progress();
				return ret;
			}
		};
		// make a task context so we can get the classes
		org.apache.hadoop.mapreduce.TaskAttemptContext taskContext = new org.apache.hadoop.mapreduce.TaskAttemptContext(
				job, getTaskID());
		// make a reducer
		org.apache.hadoop.mapreduce.Reducer<INKEY, INVALUE, OUTKEY, OUTVALUE> reducer = (org.apache.hadoop.mapreduce.Reducer<INKEY, INVALUE, OUTKEY, OUTVALUE>) ReflectionUtils
				.newInstance(taskContext.getReducerClass(), job);
		org.apache.hadoop.mapreduce.RecordWriter<OUTKEY, OUTVALUE> output = (org.apache.hadoop.mapreduce.RecordWriter<OUTKEY, OUTVALUE>) outputFormat
				.getRecordWriter(taskContext);
		job.setBoolean("mapred.skip.on", isSkipping());
		org.apache.hadoop.mapreduce.Reducer.Context reducerContext = createReduceContext(
				reducer, job, getTaskID(), rIter, reduceInputValueCounter,
				output, committer, reporter, comparator, keyClass, valueClass);
		reducer.run(reducerContext);
		output.close(reducerContext);
	}

	class ReduceCopier<K, V> implements MRConstants {

		/** Reference to the umbilical object */
		private TaskUmbilicalProtocol umbilical;
		private final TaskReporter reporter;

		/** Reference to the task object */

		/** Number of ms before timing out a copy */
		private static final int STALLED_COPY_TIMEOUT = 3 * 60 * 1000;

		/** Max events to fetch in one go from the tasktracker */
		private static final int MAX_EVENTS_TO_FETCH = 10000;

		/**
		 * our reduce task instance
		 */
		private ReduceTask reduceTask;

		/**
		 * the list of map outputs currently being copied
		 */
		private List<MapOutputLocation> scheduledCopies;

		/**
		 * the results of dispatched copy attempts
		 */
		private List<CopyResult> copyResults;

		/**
		 * the number of outputs to copy in parallel
		 */
		private int numCopiers;

		/**
		 * a number that is set to the max #fetches we'd schedule and then pause
		 * the schduling
		 */
		private int maxInFlight;

		/**
		 * the amount of time spent on fetching one map output before
		 * considering it as failed and notifying the jobtracker about it.
		 */
		private int maxBackoff;

		/**
		 * busy hosts from which copies are being backed off Map of host -> next
		 * contact time
		 */
		private Map<String, Long> penaltyBox;

		/**
		 * the set of unique hosts from which we are copying
		 */
		private Set<String> uniqueHosts;

		/**
		 * A reference to the RamManager for writing the map outputs to.
		 */

		private ShuffleRamManager ramManager;

		/**
		 * A reference to the local file system for writing the map outputs to.
		 */
		private FileSystem localFileSys;

		private FileSystem rfs;
		/**
		 * Number of files to merge at a time
		 */
		private int ioSortFactor;

		/**
		 * A reference to the throwable object (if merge throws an exception)
		 */
		private volatile Throwable mergeThrowable;

		/**
		 * A flag to indicate when to exit localFS merge
		 */
		private volatile boolean exitLocalFSMerge = false;

		/**
		 * A flag to indicate when to exit getMapEvents thread
		 */
		private volatile boolean exitGetMapEvents = false;

		/**
		 * When we accumulate maxInMemOutputs number of files in ram, we
		 * merge/spill
		 */
		private final int maxInMemOutputs;

		/**
		 * Usage threshold for in-memory output accumulation.
		 */
		private final float maxInMemCopyPer;

		/**
		 * Maximum memory usage of map outputs to merge from memory into the
		 * reduce, in bytes.
		 */
		private final long maxInMemReduce;

		/**
		 * The threads for fetching the files.
		 */
		private List<MapOutputCopier> copiers = null;

		/**
		 * The object for metrics reporting.
		 */
		private ShuffleClientMetrics shuffleClientMetrics = null;

		/**
		 * the minimum interval between tasktracker polls
		 */
		private static final long MIN_POLL_INTERVAL = 1000;

		/**
		 * a list of map output locations for fetch retrials
		 */
		private List<MapOutputLocation> retryFetches = new ArrayList<MapOutputLocation>();

		/**
		 * The set of required map outputs
		 */
		private Set<TaskID> copiedMapOutputs = Collections
				.synchronizedSet(new TreeSet<TaskID>());

		/**
		 * The set of obsolete map taskids.
		 */
		private Set<TaskAttemptID> obsoleteMapIds = Collections
				.synchronizedSet(new TreeSet<TaskAttemptID>());

		private Random random = null;

		/**
		 * the max of all the map completion times
		 */
		private int maxMapRuntime;

		/**
		 * Maximum number of fetch-retries per-map.
		 */
		private volatile int maxFetchRetriesPerMap;

		/**
		 * Combiner runner, if a combiner is needed
		 */
		private CombinerRunner combinerRunner;

		/**
		 * Resettable collector used for combine.
		 */
		private CombineOutputCollector combineCollector = null;

		/**
		 * Maximum percent of failed fetch attempt before killing the reduce
		 * task.
		 */
		private static final float MAX_ALLOWED_FAILED_FETCH_ATTEMPT_PERCENT = 0.5f;

		/**
		 * Minimum percent of progress required to keep the reduce alive.
		 */
		private static final float MIN_REQUIRED_PROGRESS_PERCENT = 0.5f;

		/**
		 * Maximum percent of shuffle execution time required to keep the
		 * reducer alive.
		 */
		private static final float MAX_ALLOWED_STALL_TIME_PERCENT = 0.5f;

		/**
		 * Minimum number of map fetch retries.
		 */
		private static final int MIN_FETCH_RETRIES_PER_MAP = 2;

		/**
		 * Maximum no. of unique maps from which we failed to fetch map-outputs
		 * even after {@link #maxFetchRetriesPerMap} retries; after this the
		 * reduce task is failed.
		 */
		private int maxFailedUniqueFetches = 5;

		/**
		 * The maps from which we fail to fetch map-outputs even after
		 * {@link #maxFetchRetriesPerMap} retries.
		 */
		Set<TaskID> fetchFailedMaps = new TreeSet<TaskID>();

		/**
		 * A map of taskId -> no. of failed fetches
		 */
		Map<TaskAttemptID, Integer> mapTaskToFailedFetchesMap = new HashMap<TaskAttemptID, Integer>();

		/**
		 * Initial backoff interval (milliseconds)
		 */
		private static final int BACKOFF_INIT = 4000;

		/**
		 * The interval for logging in the shuffle
		 */
		private static final int MIN_LOG_TIME = 60000;

		/**
		 * List of in-memory map-outputs.
		 */
		private final List<MapOutput> mapOutputsFilesInMemory = Collections
				.synchronizedList(new LinkedList<MapOutput>());

		/**
		 * The map for (Hosts, List of MapIds from this Host) maintaining map
		 * output locations
		 */
		private final Map<String, List<MapOutputLocation>> mapLocations = new ConcurrentHashMap<String, List<MapOutputLocation>>();

		/**
		 * This class contains the methods that should be used for
		 * metrics-reporting the specific metrics for shuffle. This class
		 * actually reports the metrics for the shuffle client (the ReduceTask),
		 * and hence the name ShuffleClientMetrics.
		 */
		class ShuffleClientMetrics implements Updater {
			private MetricsRecord shuffleMetrics = null;
			private int numFailedFetches = 0;
			private int numSuccessFetches = 0;
			private long numBytes = 0;
			private int numThreadsBusy = 0;

			ShuffleClientMetrics(JobConf conf) {
				MetricsContext metricsContext = MetricsUtil
						.getContext("mapred");
				this.shuffleMetrics = MetricsUtil.createRecord(metricsContext,
						"shuffleInput");
				this.shuffleMetrics.setTag("user", conf.getUser());
				this.shuffleMetrics.setTag("jobName", conf.getJobName());
				this.shuffleMetrics.setTag("jobId", ReduceTask.this.getJobID()
						.toString());
				this.shuffleMetrics.setTag("taskId", getTaskID().toString());
				this.shuffleMetrics.setTag("sessionId", conf.getSessionId());
				metricsContext.registerUpdater(this);
			}

			public synchronized void inputBytes(long numBytes) {
				this.numBytes += numBytes;
			}

			public synchronized void failedFetch() {
				++numFailedFetches;
			}

			public synchronized void successFetch() {
				++numSuccessFetches;
			}

			public synchronized void threadBusy() {
				++numThreadsBusy;
			}

			public synchronized void threadFree() {
				--numThreadsBusy;
			}

			public void doUpdates(MetricsContext unused) {
				synchronized (this) {
					shuffleMetrics.incrMetric("shuffle_input_bytes", numBytes);
					shuffleMetrics.incrMetric("shuffle_failed_fetches",
							numFailedFetches);
					shuffleMetrics.incrMetric("shuffle_success_fetches",
							numSuccessFetches);
					if (numCopiers != 0) {
						shuffleMetrics.setMetric(
								"shuffle_fetchers_busy_percent",
								100 * ((float) numThreadsBusy / numCopiers));
					} else {
						shuffleMetrics.setMetric(
								"shuffle_fetchers_busy_percent", 0);
					}
					numBytes = 0;
					numSuccessFetches = 0;
					numFailedFetches = 0;
				}
				shuffleMetrics.update();
			}
		}

		/** Represents the result of an attempt to copy a map output */
		private class CopyResult {

			// the map output location against which a copy attempt was made
			private final MapOutputLocation loc;

			// the size of the file copied, -1 if the transfer failed
			private final long size;

			// a flag signifying whether a copy result is obsolete
			private static final int OBSOLETE = -2;

			CopyResult(MapOutputLocation loc, long size) {
				this.loc = loc;
				this.size = size;
			}

			public boolean getSuccess() {
				return size >= 0;
			}

			public boolean isObsolete() {
				return size == OBSOLETE;
			}

			public long getSize() {
				return size;
			}

			public String getHost() {
				return loc.getHost();
			}

			public MapOutputLocation getLocation() {
				return loc;
			}
		}

		private int nextMapOutputCopierId = 0;

		/**
		 * Abstraction to track a map-output.
		 */
		private class MapOutputLocation {
			TaskAttemptID taskAttemptId;
			TaskID taskId;
			String ttHost;
			URL taskOutput;

			public MapOutputLocation(TaskAttemptID taskAttemptId,
					String ttHost, URL taskOutput) {
				this.taskAttemptId = taskAttemptId;
				this.taskId = this.taskAttemptId.getTaskID();
				this.ttHost = ttHost;
				this.taskOutput = taskOutput;
			}

			public TaskAttemptID getTaskAttemptId() {
				return taskAttemptId;
			}

			public TaskID getTaskId() {
				return taskId;
			}

			public String getHost() {
				return ttHost;
			}

			public URL getOutputLocation() {
				return taskOutput;
			}
		}

		/** Describes the output of a map; could either be on disk or in-memory. */
		private class MapOutput {
			final TaskID mapId;
			final TaskAttemptID mapAttemptId;

			final Path file;
			final Configuration conf;

			byte[] data;
			final boolean inMemory;
			long compressedSize;

			public MapOutput(TaskID mapId, TaskAttemptID mapAttemptId,
					Configuration conf, Path file, long size) {
				this.mapId = mapId;
				this.mapAttemptId = mapAttemptId;

				this.conf = conf;
				this.file = file;
				this.compressedSize = size;

				this.data = null;

				this.inMemory = false;
			}

			public MapOutput(TaskID mapId, TaskAttemptID mapAttemptId,
					byte[] data, int compressedLength) {
				this.mapId = mapId;
				this.mapAttemptId = mapAttemptId;

				this.file = null;
				this.conf = null;

				this.data = data;
				this.compressedSize = compressedLength;

				this.inMemory = true;
			}

			public void discard() throws IOException {
				if (inMemory) {
					data = null;
				} else {
					FileSystem fs = file.getFileSystem(conf);
					fs.delete(file, true);
				}
			}
		}

		class ShuffleRamManager implements RamManager {
			/*
			 * Maximum percentage of the in-memory limit that a single shuffle
			 * can consume
			 */
			private static final float MAX_SINGLE_SHUFFLE_SEGMENT_FRACTION = 0.25f;

			/*
			 * Maximum percentage of shuffle-threads which can be stalled
			 * simultaneously after which a merge is triggered.
			 */
			private static final float MAX_STALLED_SHUFFLE_THREADS_FRACTION = 0.75f;

			private final int maxSize;
			private final int maxSingleShuffleLimit;

			private int size = 0;

			private Object dataAvailable = new Object();
			private int fullSize = 0;
			private int numPendingRequests = 0;
			private int numRequiredMapOutputs = 0;
			private int numClosed = 0;
			private boolean closed = false;

			public ShuffleRamManager(Configuration conf) throws IOException {
				final float maxInMemCopyUse = conf.getFloat(
						"mapred.job.shuffle.input.buffer.percent", 0.70f);
				if (maxInMemCopyUse > 1.0 || maxInMemCopyUse < 0.0) {
					throw new IOException(
							"mapred.job.shuffle.input.buffer.percent"
									+ maxInMemCopyUse);
				}
				maxSize = (int) Math.min(Runtime.getRuntime().maxMemory()
						* maxInMemCopyUse, Integer.MAX_VALUE);
				maxSingleShuffleLimit = (int) (maxSize * MAX_SINGLE_SHUFFLE_SEGMENT_FRACTION);
				LOG.info("ShuffleRamManager: MemoryLimit=" + maxSize
						+ ", MaxSingleShuffleLimit=" + maxSingleShuffleLimit);
			}

			public synchronized boolean reserve(int requestedSize,
					InputStream in) throws InterruptedException {
				// Wait till the request can be fulfilled...
				while ((size + requestedSize) > maxSize) {

					// Close the input...
					if (in != null) {
						try {
							in.close();
						} catch (IOException ie) {
							LOG.info("Failed to close connection with: " + ie);
						} finally {
							in = null;
						}
					}

					// Track pending requests
					synchronized (dataAvailable) {
						++numPendingRequests;
						dataAvailable.notify();
					}

					// Wait for memory to free up
					wait();

					// Track pending requests
					synchronized (dataAvailable) {
						--numPendingRequests;
					}
				}

				size += requestedSize;

				return (in != null);
			}

			public synchronized void unreserve(int requestedSize) {
				size -= requestedSize;

				synchronized (dataAvailable) {
					fullSize -= requestedSize;
					--numClosed;
				}

				// Notify the threads blocked on RamManager.reserve
				notifyAll();
			}

			public boolean waitForDataToMerge() throws InterruptedException {
				boolean done = false;
				synchronized (dataAvailable) {
					// Start in-memory merge if manager has been closed or...
					while (!closed
							&&
							// In-memory threshold exceeded and at least two
							// segments
							// have been fetched
							(getPercentUsed() < maxInMemCopyPer || numClosed < 2)
							&&
							// More than "mapred.inmem.merge.threshold" map
							// outputs
							// have been fetched into memory
							(maxInMemOutputs <= 0 || numClosed < maxInMemOutputs)
							&&
							// More than MAX... threads are blocked on the
							// RamManager
							// or the blocked threads are the last map outputs
							// to be
							// fetched. If numRequiredMapOutputs is zero, either
							// setNumCopiedMapOutputs has not been called (no
							// map ouputs
							// have been fetched, so there is nothing to merge)
							// or the
							// last map outputs being transferred without
							// contention, so a merge would be premature.
							(numPendingRequests < numCopiers
									* MAX_STALLED_SHUFFLE_THREADS_FRACTION && (0 == numRequiredMapOutputs || numPendingRequests < numRequiredMapOutputs))) {
						dataAvailable.wait();
					}
					done = closed;
				}
				return done;
			}

			public void closeInMemoryFile(int requestedSize) {
				synchronized (dataAvailable) {
					fullSize += requestedSize;
					++numClosed;
					dataAvailable.notify();
				}
			}

			public void setNumCopiedMapOutputs(int numRequiredMapOutputs) {
				synchronized (dataAvailable) {
					this.numRequiredMapOutputs = numRequiredMapOutputs;
					dataAvailable.notify();
				}
			}

			public void close() {
				synchronized (dataAvailable) {
					closed = true;
					LOG.info("Closed ram manager");
					dataAvailable.notify();
				}
			}

			private float getPercentUsed() {
				return (float) fullSize / maxSize;
			}

			int getMemoryLimit() {
				return maxSize;
			}

			boolean canFitInMemory(long requestedSize) {
				return (requestedSize < Integer.MAX_VALUE && requestedSize < maxSingleShuffleLimit);
			}
		}

		/** Copies map outputs as they become available */
		private class MapOutputCopier extends Thread {
			// basic/unit connection timeout (in milliseconds)
			private final static int UNIT_CONNECT_TIMEOUT = 30 * 1000;
			// default read timeout (in milliseconds)
			private final static int DEFAULT_READ_TIMEOUT = 3 * 60 * 1000;

			private MapOutputLocation currentLocation = null;
			private int id = nextMapOutputCopierId++;
			private Reporter reporter;

			// Decompression of map-outputs
			private CompressionCodec codec = null;
			private Decompressor decompressor = null;

			public MapOutputCopier(JobConf job, Reporter reporter) {
				setName("MapOutputCopier " + reduceTask.getTaskID() + "." + id);
				LOG.debug(getName() + " created");
				this.reporter = reporter;

				if (job.getCompressMapOutput()) {
					Class<? extends CompressionCodec> codecClass = job
							.getMapOutputCompressorClass(DefaultCodec.class);
					codec = ReflectionUtils.newInstance(codecClass, job);
					decompressor = CodecPool.getDecompressor(codec);
				}
			}

			/**
			 * Fail the current file that we are fetching
			 * 
			 * @return were we currently fetching?
			 */
			public synchronized boolean fail() {
				if (currentLocation != null) {
					finish(-1);
					return true;
				} else {
					return false;
				}
			}

			/**
			 * Get the current map output location.
			 */
			public synchronized MapOutputLocation getLocation() {
				return currentLocation;
			}

			private synchronized void start(MapOutputLocation loc) {
				currentLocation = loc;
			}

			private synchronized void finish(long size) {
				if (currentLocation != null) {
					LOG.debug(getName() + " finishing " + currentLocation
							+ " =" + size);
					synchronized (copyResults) {
						copyResults.add(new CopyResult(currentLocation, size));
						copyResults.notify();
					}
					currentLocation = null;
				}
			}

			/**
			 * Loop forever and fetch map outputs as they become available. The
			 * thread exits when it is interrupted by {@link ReduceTaskRunner}
			 */
			@Override
			public void run() {
				while (true) {
					try {
						MapOutputLocation loc = null;
						long size = -1;

						synchronized (scheduledCopies) {
							while (scheduledCopies.isEmpty()) {
								scheduledCopies.wait();
							}
							loc = scheduledCopies.remove(0);
						}

						try {
							shuffleClientMetrics.threadBusy();
							start(loc);
							size = copyOutput(loc);
							shuffleClientMetrics.successFetch();
						} catch (IOException e) {
							LOG.warn(reduceTask.getTaskID() + " copy failed: "
									+ loc.getTaskAttemptId() + " from "
									+ loc.getHost());
							LOG.warn(StringUtils.stringifyException(e));
							shuffleClientMetrics.failedFetch();

							// Reset
							size = -1;
						} finally {
							shuffleClientMetrics.threadFree();
							finish(size);
						}
					} catch (InterruptedException e) {
						break; // ALL DONE
					} catch (FSError e) {
						LOG.error("Task: " + reduceTask.getTaskID()
								+ " - FSError: "
								+ StringUtils.stringifyException(e));
						try {
							umbilical.fsError(reduceTask.getTaskID(),
									e.getMessage());
						} catch (IOException io) {
							LOG.error("Could not notify TT of FSError: "
									+ StringUtils.stringifyException(io));
						}
					} catch (Throwable th) {
						String msg = getTaskID()
								+ " : Map output copy failure : "
								+ StringUtils.stringifyException(th);
						reportFatalError(getTaskID(), th, msg);
					}
				}

				if (decompressor != null) {
					CodecPool.returnDecompressor(decompressor);
				}

			}

			/**
			 * Copies a a map output from a remote host, via HTTP.
			 * 
			 * @param currentLocation
			 *            the map output location to be copied
			 * @return the path (fully qualified) of the copied file
			 * @throws IOException
			 *             if there is an error copying the file
			 * @throws InterruptedException
			 *             if the copier should give up
			 */
			private long copyOutput(MapOutputLocation loc) throws IOException,
					InterruptedException {
				// check if we still need to copy the output from this location
				if (copiedMapOutputs.contains(loc.getTaskId())
						|| obsoleteMapIds.contains(loc.getTaskAttemptId())) {
					return CopyResult.OBSOLETE;
				}

				// a temp filename. If this file gets created in ramfs, we're
				// fine,
				// else, we will check the localFS to find a suitable final
				// location
				// for this path
				TaskAttemptID reduceId = reduceTask.getTaskID();
				Path filename = new Path("/"
						+ TaskTracker.getIntermediateOutputDir(reduceId
								.getJobID().toString(), reduceId.toString())
						+ "/map_" + loc.getTaskId().getId() + ".out");

				// Copy the map output to a temp file whose name is unique to
				// this attempt
				Path tmpMapOutput = new Path(filename + "-" + id);

				// Copy the map output
				MapOutput mapOutput = getMapOutput(loc, tmpMapOutput, reduceId
						.getTaskID().getId());
				if (mapOutput == null) {
					throw new IOException("Failed to fetch map-output for "
							+ loc.getTaskAttemptId() + " from " + loc.getHost());
				}

				// The size of the map-output
				long bytes = mapOutput.compressedSize;

				// lock the ReduceTask while we do the rename
				synchronized (ReduceTask.this) {
					if (copiedMapOutputs.contains(loc.getTaskId())) {
						mapOutput.discard();
						return CopyResult.OBSOLETE;
					}

					// Special case: discard empty map-outputs
					if (bytes == 0) {
						try {
							mapOutput.discard();
						} catch (IOException ioe) {
							LOG.info("Couldn't discard output of "
									+ loc.getTaskId());
						}

						// Note that we successfully copied the map-output
						noteCopiedMapOutput(loc.getTaskId());

						return bytes;
					}

					// Process map-output
					if (mapOutput.inMemory) {
						// Save it in the synchronized list of map-outputs
						mapOutputsFilesInMemory.add(mapOutput);
					} else {
						// Rename the temporary file to the final file;
						// ensure it is on the same partition
						tmpMapOutput = mapOutput.file;
						filename = new Path(tmpMapOutput.getParent(),
								filename.getName());
						if (!localFileSys.rename(tmpMapOutput, filename)) {
							localFileSys.delete(tmpMapOutput, true);
							bytes = -1;
							throw new IOException(
									"Failed to rename map output "
											+ tmpMapOutput + " to " + filename);
						}

						synchronized (mapOutputFilesOnDisk) {
							addToMapOutputFilesOnDisk(localFileSys
									.getFileStatus(filename));
						}
					}

					// Note that we successfully copied the map-output
					noteCopiedMapOutput(loc.getTaskId());
				}

				return bytes;
			}

			/**
			 * Save the map taskid whose output we just copied. This function
			 * assumes that it has been synchronized on ReduceTask.this.
			 * 
			 * @param taskId
			 *            map taskid
			 */
			private void noteCopiedMapOutput(TaskID taskId) {
				copiedMapOutputs.add(taskId);
				ramManager.setNumCopiedMapOutputs(numReduces
						- copiedMapOutputs.size());
			}

			/**
			 * Get the map output into a local file (either in the inmemory fs
			 * or on the local fs) from the remote server. We use the file
			 * system so that we generate checksum files on the data.
			 * 
			 * @param mapOutputLoc
			 *            map-output to be fetched
			 * @param filename
			 *            the filename to write the data into
			 * @param connectionTimeout
			 *            number of milliseconds for connection timeout
			 * @param readTimeout
			 *            number of milliseconds for read timeout
			 * @return the path of the file that got created
			 * @throws IOException
			 *             when something goes wrong
			 */
			private MapOutput getMapOutput(MapOutputLocation mapOutputLoc,
					Path filename, int reduce) throws IOException,
					InterruptedException {
				// Connect
				URLConnection connection = mapOutputLoc.getOutputLocation()
						.openConnection();
				InputStream input = getInputStream(connection,
						STALLED_COPY_TIMEOUT, DEFAULT_READ_TIMEOUT);

				// Validate header from map output
				TaskAttemptID mapId = null;
				try {
					mapId = TaskAttemptID.forName(connection
							.getHeaderField(FROM_MAP_TASK));
				} catch (IllegalArgumentException ia) {
					LOG.warn("Invalid map id ", ia);
					return null;
				}
				TaskAttemptID expectedMapId = mapOutputLoc.getTaskAttemptId();
				if (!mapId.equals(expectedMapId)) {
					LOG.warn("data from wrong map:" + mapId
							+ " arrived to reduce task " + reduce
							+ ", where as expected map output should be from "
							+ expectedMapId);
					return null;
				}

				long decompressedLength = Long.parseLong(connection
						.getHeaderField(RAW_MAP_OUTPUT_LENGTH));
				long compressedLength = Long.parseLong(connection
						.getHeaderField(MAP_OUTPUT_LENGTH));

				if (compressedLength < 0 || decompressedLength < 0) {
					LOG.warn(getName()
							+ " invalid lengths in map output header: id: "
							+ mapId + " compressed len: " + compressedLength
							+ ", decompressed len: " + decompressedLength);
					return null;
				}
				int forReduce = (int) Integer.parseInt(connection
						.getHeaderField(FOR_REDUCE_TASK));

				if (forReduce != reduce) {
					LOG.warn("data for the wrong reduce: " + forReduce
							+ " with compressed len: " + compressedLength
							+ ", decompressed len: " + decompressedLength
							+ " arrived to reduce task " + reduce);
					return null;
				}
				LOG.info("header: " + mapId + ", compressed len: "
						+ compressedLength + ", decompressed len: "
						+ decompressedLength);

				// We will put a file in memory if it meets certain criteria:
				// 1. The size of the (decompressed) file should be less than
				// 25% of
				// the total inmem fs
				// 2. There is space available in the inmem fs

				// Check if this map-output can be saved in-memory
				boolean shuffleInMemory = ramManager
						.canFitInMemory(decompressedLength);

				// Shuffle
				MapOutput mapOutput = null;

				// close in-memory shuffling for comparison
				shuffleInMemory = false;
				if (shuffleInMemory) {
					LOG.info("Shuffling " + decompressedLength + " bytes ("
							+ compressedLength + " raw bytes) "
							+ "into RAM from "
							+ mapOutputLoc.getTaskAttemptId());

					mapOutput = shuffleInMemory(mapOutputLoc, connection,
							input, (int) decompressedLength,
							(int) compressedLength);
				} else {
					LOG.info("Shuffling " + decompressedLength + " bytes ("
							+ compressedLength + " raw bytes) "
							+ "into Local-FS from "
							+ mapOutputLoc.getTaskAttemptId());

					mapOutput = shuffleToDisk(mapOutputLoc, input, filename,
							compressedLength);
				}

				return mapOutput;
			}

			/**
			 * The connection establishment is attempted multiple times and is
			 * given up only on the last failure. Instead of connecting with a
			 * timeout of X, we try connecting with a timeout of x < X but
			 * multiple times.
			 */
			private InputStream getInputStream(URLConnection connection,
					int connectionTimeout, int readTimeout) throws IOException {
				int unit = 0;
				if (connectionTimeout < 0) {
					throw new IOException("Invalid timeout " + "[timeout = "
							+ connectionTimeout + " ms]");
				} else if (connectionTimeout > 0) {
					unit = (UNIT_CONNECT_TIMEOUT > connectionTimeout) ? connectionTimeout
							: UNIT_CONNECT_TIMEOUT;
				}
				// set the read timeout to the total timeout
				connection.setReadTimeout(readTimeout);
				// set the connect timeout to the unit-connect-timeout
				connection.setConnectTimeout(unit);
				while (true) {
					try {
						return connection.getInputStream();
					} catch (IOException ioe) {
						// update the total remaining connect-timeout
						connectionTimeout -= unit;

						// throw an exception if we have waited for timeout
						// amount of time
						// note that the updated value if timeout is used here
						if (connectionTimeout == 0) {
							throw ioe;
						}

						// reset the connect timeout for the last try
						if (connectionTimeout < unit) {
							unit = connectionTimeout;
							// reset the connect time out for the final connect
							connection.setConnectTimeout(unit);
						}
					}
				}
			}

			private MapOutput shuffleInMemory(MapOutputLocation mapOutputLoc,
					URLConnection connection, InputStream input,
					int mapOutputLength, int compressedLength)
					throws IOException, InterruptedException {
				// Reserve ram for the map-output
				boolean createdNow = ramManager.reserve(mapOutputLength, input);

				// Reconnect if we need to
				if (!createdNow) {
					// Reconnect
					try {
						connection = mapOutputLoc.getOutputLocation()
								.openConnection();
						input = getInputStream(connection,
								STALLED_COPY_TIMEOUT, DEFAULT_READ_TIMEOUT);
					} catch (IOException ioe) {
						LOG.info("Failed reopen connection to fetch map-output from "
								+ mapOutputLoc.getHost());

						// Inform the ram-manager
						ramManager.closeInMemoryFile(mapOutputLength);
						ramManager.unreserve(mapOutputLength);

						throw ioe;
					}
				}

				IFileInputStream checksumIn = new IFileInputStream(input,
						compressedLength);

				input = checksumIn;

				// Are map-outputs compressed?
				if (codec != null) {
					decompressor.reset();
					input = codec.createInputStream(input, decompressor);
				}

				// Copy map-output into an in-memory buffer
				byte[] shuffleData = new byte[mapOutputLength];
				MapOutput mapOutput = new MapOutput(mapOutputLoc.getTaskId(),
						mapOutputLoc.getTaskAttemptId(), shuffleData,
						compressedLength);

				int bytesRead = 0;
				try {
					int n = input.read(shuffleData, 0, shuffleData.length);
					while (n > 0) {
						bytesRead += n;
						shuffleClientMetrics.inputBytes(n);

						// indicate we're making progress
						reporter.progress();
						n = input.read(shuffleData, bytesRead,
								(shuffleData.length - bytesRead));
					}

					LOG.info("Read " + bytesRead
							+ " bytes from map-output for "
							+ mapOutputLoc.getTaskAttemptId());

					input.close();
				} catch (IOException ioe) {
					LOG.info(
							"Failed to shuffle from "
									+ mapOutputLoc.getTaskAttemptId(), ioe);

					// Inform the ram-manager
					ramManager.closeInMemoryFile(mapOutputLength);
					ramManager.unreserve(mapOutputLength);

					// Discard the map-output
					try {
						mapOutput.discard();
					} catch (IOException ignored) {
						LOG.info("Failed to discard map-output from "
								+ mapOutputLoc.getTaskAttemptId(), ignored);
					}
					mapOutput = null;

					// Close the streams
					IOUtils.cleanup(LOG, input);

					// Re-throw
					throw ioe;
				}

				// Close the in-memory file
				// ramManager.closeInMemoryFile(mapOutputLength);
				LOG.info("so far so good");
				ramManager.closeInMemoryFile(bytesRead);

				// Sanity check
				// if (bytesRead != mapOutputLength) {
				// // Inform the ram-manager
				// ramManager.unreserve(mapOutputLength);
				//
				// // Discard the map-output
				// try {
				// mapOutput.discard();
				// } catch (IOException ignored) {
				// // IGNORED because we are cleaning up
				// LOG.info("Failed to discard map-output from "
				// + mapOutputLoc.getTaskAttemptId(), ignored);
				// }
				// mapOutput = null;
				//
				// throw new IOException("Incomplete map output received for "
				// + mapOutputLoc.getTaskAttemptId() + " from "
				// + mapOutputLoc.getOutputLocation() + " ("
				// + bytesRead + " instead of " + mapOutputLength
				// + ")");
				// }

				// TODO: Remove this after a 'fix' for HADOOP-3647
				if (mapOutputLength > 0) {
					DataInputBuffer dib = new DataInputBuffer();
					dib.reset(shuffleData, 0, shuffleData.length);
					LOG.info("Rec #1 from " + mapOutputLoc.getTaskAttemptId()
							+ " -> (" + WritableUtils.readVInt(dib) + ", "
							+ WritableUtils.readVInt(dib) + ") from "
							+ mapOutputLoc.getHost());
				} else {
					LOG.info("map output length: " + mapOutputLength);
				}

				return mapOutput;
			}

			private MapOutput shuffleToDisk(MapOutputLocation mapOutputLoc,
					InputStream input, Path filename, long mapOutputLength)
					throws IOException {
				// Find out a suitable location for the output on
				// local-filesystem
				Path localFilename = lDirAlloc.getLocalPathForWrite(filename
						.toUri().getPath(), mapOutputLength, conf);

				MapOutput mapOutput = new MapOutput(mapOutputLoc.getTaskId(),
						mapOutputLoc.getTaskAttemptId(), conf,
						localFileSys.makeQualified(localFilename),
						mapOutputLength);

				// Copy data to local-disk
				OutputStream output = null;
				long bytesRead = 0;
				try {
					output = rfs.create(localFilename);

					byte[] buf = new byte[64 * 1024];
					int n = input.read(buf, 0, buf.length);
					while (n > 0) {
						bytesRead += n;
						shuffleClientMetrics.inputBytes(n);
						output.write(buf, 0, n);

						// indicate we're making progress
						reporter.progress();
						n = input.read(buf, 0, buf.length);
					}

					LOG.info("Read " + bytesRead
							+ " bytes from map-output for "
							+ mapOutputLoc.getTaskAttemptId());

					output.close();
					input.close();
				} catch (IOException ioe) {
					LOG.info(
							"Failed to shuffle from "
									+ mapOutputLoc.getTaskAttemptId(), ioe);

					// Discard the map-output
					try {
						mapOutput.discard();
					} catch (IOException ignored) {
						LOG.info("Failed to discard map-output from "
								+ mapOutputLoc.getTaskAttemptId(), ignored);
					}
					mapOutput = null;

					// Close the streams
					IOUtils.cleanup(LOG, input, output);

					// Re-throw
					throw ioe;
				}

				// Sanity check
				if (bytesRead != mapOutputLength) {
					try {
						mapOutput.discard();
					} catch (Exception ioe) {
						// IGNORED because we are cleaning up
						LOG.info("Failed to discard map-output from "
								+ mapOutputLoc.getTaskAttemptId(), ioe);
					} catch (Throwable t) {
						String msg = getTaskID()
								+ " : Failed in shuffle to disk :"
								+ StringUtils.stringifyException(t);
						reportFatalError(getTaskID(), t, msg);
					}
					mapOutput = null;

					throw new IOException("Incomplete map output received for "
							+ mapOutputLoc.getTaskAttemptId() + " from "
							+ mapOutputLoc.getOutputLocation() + " ("
							+ bytesRead + " instead of " + mapOutputLength
							+ ")");
				}

				return mapOutput;

			}

		} // MapOutputCopier

		private void configureClasspath(JobConf conf) throws IOException {

			// get the task and the current classloader which will become the
			// parent
			Task task = ReduceTask.this;
			ClassLoader parent = conf.getClassLoader();

			// get the work directory which holds the elements we are
			// dynamically
			// adding to the classpath
			File workDir = new File(task.getJobFile()).getParentFile();
			ArrayList<URL> urllist = new ArrayList<URL>();

			// add the jars and directories to the classpath
			String jar = conf.getJar();
			if (jar != null) {
				File jobCacheDir = new File(new Path(jar).getParent()
						.toString());

				File[] libs = new File(jobCacheDir, "lib").listFiles();
				if (libs != null) {
					for (int i = 0; i < libs.length; i++) {
						urllist.add(libs[i].toURL());
					}
				}
				urllist.add(new File(jobCacheDir, "classes").toURL());
				urllist.add(jobCacheDir.toURL());

			}
			urllist.add(workDir.toURL());

			// create a new classloader with the old classloader as its parent
			// then set that classloader as the one used by the current jobconf
			URL[] urls = urllist.toArray(new URL[urllist.size()]);
			URLClassLoader loader = new URLClassLoader(urls, parent);
			conf.setClassLoader(loader);
		}

		public ReduceCopier(TaskUmbilicalProtocol umbilical, JobConf conf,
				TaskReporter reporter) throws ClassNotFoundException,
				IOException {

			configureClasspath(conf);
			this.reporter = reporter;
			this.shuffleClientMetrics = new ShuffleClientMetrics(conf);
			this.umbilical = umbilical;
			this.reduceTask = ReduceTask.this;

			this.scheduledCopies = new ArrayList<MapOutputLocation>(100);
			this.copyResults = new ArrayList<CopyResult>(100);
			this.numCopiers = conf.getInt("mapred.reduce.parallel.copies", 5);
			this.maxInFlight = 4 * numCopiers;
			this.maxBackoff = conf.getInt("mapred.reduce.copy.backoff", 300);
			Counters.Counter combineInputCounter = reporter
					.getCounter(Task.Counter.COMBINE_INPUT_RECORDS);
			this.combinerRunner = CombinerRunner.create(conf, getTaskID(),
					combineInputCounter, reporter, null);
			if (combinerRunner != null) {
				combineCollector = new CombineOutputCollector(
						reduceCombineOutputCounter);
			}

			this.ioSortFactor = conf.getInt("io.sort.factor", 10);
			// the exponential backoff formula
			// backoff (t) = init * base^(t-1)
			// so for max retries we get
			// backoff(1) + .... + backoff(max_fetch_retries) ~ max
			// solving which we get
			// max_fetch_retries ~ log((max * (base - 1) / init) + 1) /
			// log(base)
			// for the default value of max = 300 (5min) we get
			// max_fetch_retries = 6
			// the order is 4,8,16,32,64,128. sum of which is 252 sec = 4.2 min

			// optimizing for the base 2
			this.maxFetchRetriesPerMap = Math
					.max(MIN_FETCH_RETRIES_PER_MAP,
							getClosestPowerOf2((this.maxBackoff * 1000 / BACKOFF_INIT) + 1));
			this.maxFailedUniqueFetches = Math.min(numReduces,
					this.maxFailedUniqueFetches);
			this.maxInMemOutputs = conf.getInt("mapred.inmem.merge.threshold",
					1000);
			this.maxInMemCopyPer = conf.getFloat(
					"mapred.job.shuffle.merge.percent", 0.66f);
			final float maxRedPer = conf.getFloat(
					"mapred.job.reduce.input.buffer.percent", 0f);
			if (maxRedPer > 1.0 || maxRedPer < 0.0) {
				throw new IOException("mapred.job.reduce.input.buffer.percent"
						+ maxRedPer);
			}
			this.maxInMemReduce = (int) Math.min(Runtime.getRuntime()
					.maxMemory() * maxRedPer, Integer.MAX_VALUE);

			// Setup the RamManager
			ramManager = new ShuffleRamManager(conf);

			localFileSys = FileSystem.getLocal(conf);

			rfs = ((LocalFileSystem) localFileSys).getRaw();

			// hosts -> next contact time
			this.penaltyBox = new LinkedHashMap<String, Long>();

			// hostnames
			this.uniqueHosts = new HashSet<String>();

			// Seed the random number generator with a reasonably globally
			// unique seed
			long randomSeed = System.nanoTime()
					+ (long) Math.pow(this.reduceTask.getPartition(),
							(this.reduceTask.getPartition() % 10));
			this.random = new Random(randomSeed);
			this.maxMapRuntime = 0;
		}

		private boolean busyEnough(int numInFlight) {
			return numInFlight > maxInFlight;
		}

		public boolean fetchOutputs() throws IOException {
			int totalFailures = 0;
			int numInFlight = 0, numCopied = 0;
			DecimalFormat mbpsFormat = new DecimalFormat("0.00");
			final Progress copyPhase = reduceTask.getProgress().phase();
			LocalFSMerger localFSMergerThread = null;
			InMemFSMergeThread inMemFSMergeThread = null;
			GetMapEventsThread getMapEventsThread = null;

			for (int i = 0; i < numReduces; i++) {
				copyPhase.addPhase(); // add sub-phase per file
			}

			copiers = new ArrayList<MapOutputCopier>(numCopiers);

			// start all the copying threads
			for (int i = 0; i < numCopiers; i++) {
				MapOutputCopier copier = new MapOutputCopier(conf, reporter);
				copiers.add(copier);
				copier.start();
			}

			// start the on-disk-merge thread
			localFSMergerThread = new LocalFSMerger(
					(LocalFileSystem) localFileSys);
			// start the in memory merger thread
			inMemFSMergeThread = new InMemFSMergeThread();
			localFSMergerThread.start();
			inMemFSMergeThread.start();

			// start the map events thread
			getMapEventsThread = new GetMapEventsThread();
			getMapEventsThread.start();

			// start the clock for bandwidth measurement
			long startTime = System.currentTimeMillis();
			long currentTime = startTime;
			long lastProgressTime = startTime;
			long lastOutputTime = 0;

			// loop until we get all required outputs
			while (copiedMapOutputs.size() < numReduces && mergeThrowable == null) {

				currentTime = System.currentTimeMillis();
				boolean logNow = false;
				if (currentTime - lastOutputTime > MIN_LOG_TIME) {
					lastOutputTime = currentTime;
					logNow = true;
				}
				if (logNow) {
					LOG.info(reduceTask.getTaskID() + " Need another "
							+ (numReduces - copiedMapOutputs.size())
							+ " map output(s) " + "where " + numInFlight
							+ " is already in progress");
				}

				// Put the hash entries for the failed fetches.
				Iterator<MapOutputLocation> locItr = retryFetches.iterator();

				while (locItr.hasNext()) {
					MapOutputLocation loc = locItr.next();
					List<MapOutputLocation> locList = mapLocations.get(loc
							.getHost());

					// Check if the list exists. Map output location mapping is
					// cleared
					// once the jobtracker restarts and is rebuilt from scratch.
					// Note that map-output-location mapping will be recreated
					// and hence
					// we continue with the hope that we might find some
					// locations
					// from the rebuild map.
					if (locList != null) {
						// Add to the beginning of the list so that this map is
						// tried again before the others and we can hasten the
						// re-execution of this map should there be a problem
						locList.add(0, loc);
					}
				}

				if (retryFetches.size() > 0) {
					LOG.info(reduceTask.getTaskID() + ": " + "Got "
							+ retryFetches.size()
							+ " map-outputs from previous failures");
				}
				// clear the "failed" fetches hashmap
				retryFetches.clear();

				// now walk through the cache and schedule what we can
				int numScheduled = 0;
				int numDups = 0;

				synchronized (scheduledCopies) {

					// Randomize the map output locations to prevent
					// all reduce-tasks swamping the same tasktracker
					List<String> hostList = new ArrayList<String>();
					hostList.addAll(mapLocations.keySet());

					Collections.shuffle(hostList, this.random);

					Iterator<String> hostsItr = hostList.iterator();

					while (hostsItr.hasNext()) {

						String host = hostsItr.next();

						List<MapOutputLocation> knownOutputsByLoc = mapLocations
								.get(host);

						// Check if the list exists. Map output location mapping
						// is
						// cleared once the jobtracker restarts and is rebuilt
						// from
						// scratch.
						// Note that map-output-location mapping will be
						// recreated and
						// hence we continue with the hope that we might find
						// some
						// locations from the rebuild map and add then for
						// fetching.
						if (knownOutputsByLoc == null
								|| knownOutputsByLoc.size() == 0) {
							continue;
						}

						// Identify duplicate hosts here
						if (uniqueHosts.contains(host)) {
							numDups += knownOutputsByLoc.size();
							continue;
						}

						Long penaltyEnd = penaltyBox.get(host);
						boolean penalized = false;

						if (penaltyEnd != null) {
							if (currentTime < penaltyEnd.longValue()) {
								penalized = true;
							} else {
								penaltyBox.remove(host);
							}
						}

						if (penalized)
							continue;

						synchronized (knownOutputsByLoc) {

							locItr = knownOutputsByLoc.iterator();

							while (locItr.hasNext()) {

								MapOutputLocation loc = locItr.next();

								// Do not schedule fetches from OBSOLETE maps
								if (obsoleteMapIds.contains(loc
										.getTaskAttemptId())) {
									locItr.remove();
									continue;
								}

								uniqueHosts.add(host);
								scheduledCopies.add(loc);
								locItr.remove(); // remove from knownOutputs
								numInFlight++;
								numScheduled++;

								break; // we have a map from this host
							}
						}
					}
					scheduledCopies.notifyAll();
				}

				if (numScheduled > 0 || logNow) {
					LOG.info(reduceTask.getTaskID() + " Scheduled "
							+ numScheduled + " outputs (" + penaltyBox.size()
							+ " slow hosts and" + numDups + " dup hosts)");
				}

				if (penaltyBox.size() > 0 && logNow) {
					LOG.info("Penalized(slow) Hosts: ");
					for (String host : penaltyBox.keySet()) {
						LOG.info(host + " Will be considered after: "
								+ ((penaltyBox.get(host) - currentTime) / 1000)
								+ " seconds.");
					}
				}

				// if we have no copies in flight and we can't schedule anything
				// new, just wait for a bit
				try {
					if (numInFlight == 0 && numScheduled == 0) {
						// we should indicate progress as we don't want TT to
						// think
						// we're stuck and kill us
						reporter.progress();
						Thread.sleep(5000);
					}
				} catch (InterruptedException e) {
				} // IGNORE

				while (numInFlight > 0 && mergeThrowable == null) {
					LOG.debug(reduceTask.getTaskID() + " numInFlight = "
							+ numInFlight);
					// the call to getCopyResult will either
					// 1) return immediately with a null or a valid CopyResult
					// object,
					// or
					// 2) if the numInFlight is above maxInFlight, return with a
					// CopyResult object after getting a notification from a
					// fetcher thread,
					// So, when getCopyResult returns null, we can be sure that
					// we aren't busy enough and we should go and get more
					// mapcompletion
					// events from the tasktracker
					CopyResult cr = getCopyResult(numInFlight);

					if (cr == null) {
						break;
					}

					if (cr.getSuccess()) { // a successful copy
						numCopied++;
						lastProgressTime = System.currentTimeMillis();
						reduceShuffleBytes.increment(cr.getSize());

						long secsSinceStart = (System.currentTimeMillis() - startTime) / 1000 + 1;
						float mbs = ((float) reduceShuffleBytes.getCounter())
								/ (1024 * 1024);
						float transferRate = mbs / secsSinceStart;

						copyPhase.startNextPhase();
						copyPhase.setStatus("copy (" + numCopied + " of "
								+ numReduces + " at "
								+ mbpsFormat.format(transferRate) + " MB/s)");

						// Note successful fetch for this mapId to invalidate
						// (possibly) old fetch-failures
						fetchFailedMaps.remove(cr.getLocation().getTaskId());
					} else if (cr.isObsolete()) {
						// ignore

						// Note successful fetch for this mapId to invalidate
						// (possibly) old fetch-failures
						// fetchFailedMaps.remove(cr.getLocation().getTaskId());

						LOG.info(reduceTask.getTaskID()
								+ " Ignoring obsolete copy result for Map Task: "
								+ cr.getLocation().getTaskAttemptId()
								+ " from host: " + cr.getHost());
					} else {
						retryFetches.add(cr.getLocation());

						// note the failed-fetch
						TaskAttemptID mapTaskId = cr.getLocation()
								.getTaskAttemptId();
						TaskID mapId = cr.getLocation().getTaskId();

						totalFailures++;
						Integer noFailedFetches = mapTaskToFailedFetchesMap
								.get(mapTaskId);
						noFailedFetches = (noFailedFetches == null) ? 1
								: (noFailedFetches + 1);
						mapTaskToFailedFetchesMap.put(mapTaskId,
								noFailedFetches);
						LOG.info("Task " + getTaskID() + ": Failed fetch #"
								+ noFailedFetches + " from " + mapTaskId);

						// did the fetch fail too many times?
						// using a hybrid technique for notifying the
						// jobtracker.
						// a. the first notification is sent after max-retries
						// b. subsequent notifications are sent after 2 retries.
						if ((noFailedFetches >= maxFetchRetriesPerMap)
								&& ((noFailedFetches - maxFetchRetriesPerMap) % 2) == 0) {
							synchronized (ReduceTask.this) {
								taskStatus.addFetchFailedMap(mapTaskId);
								LOG.info("Failed to fetch map-output from "
										+ mapTaskId
										+ " even after MAX_FETCH_RETRIES_PER_MAP retries... "
										+ " reporting to the JobTracker");
							}
						}
						// note unique failed-fetch maps
						if (noFailedFetches == maxFetchRetriesPerMap) {
							fetchFailedMaps.add(mapId);

							// did we have too many unique failed-fetch maps?
							// and did we fail on too many fetch attempts?
							// and did we progress enough
							// or did we wait for too long without any progress?

							// check if the reducer is healthy
							boolean reducerHealthy = (((float) totalFailures / (totalFailures + numCopied)) < MAX_ALLOWED_FAILED_FETCH_ATTEMPT_PERCENT);

							// check if the reducer has progressed enough
							boolean reducerProgressedEnough = (((float) numCopied / numReduces) >= MIN_REQUIRED_PROGRESS_PERCENT);

							// check if the reducer is stalled for a long time
							// duration for which the reducer is stalled
							int stallDuration = (int) (System
									.currentTimeMillis() - lastProgressTime);
							// duration for which the reducer ran with progress
							int shuffleProgressDuration = (int) (lastProgressTime - startTime);
							// min time the reducer should run without getting
							// killed
							int minShuffleRunDuration = (shuffleProgressDuration > maxMapRuntime) ? shuffleProgressDuration
									: maxMapRuntime;
							boolean reducerStalled = (((float) stallDuration / minShuffleRunDuration) >= MAX_ALLOWED_STALL_TIME_PERCENT);

							// kill if not healthy and has insufficient progress
							if ((fetchFailedMaps.size() >= maxFailedUniqueFetches || fetchFailedMaps
									.size() == (numReduces - copiedMapOutputs
									.size()))
									&& !reducerHealthy
									&& (!reducerProgressedEnough || reducerStalled)) {
								LOG.fatal("Shuffle failed with too many fetch failures "
										+ "and insufficient progress!"
										+ "Killing task " + getTaskID() + ".");
								umbilical.shuffleError(getTaskID(),
										"Exceeded MAX_FAILED_UNIQUE_FETCHES;"
												+ " bailing-out.");
							}
						}

						// back off exponentially until num_retries <=
						// max_retries
						// back off by max_backoff/2 on subsequent failed
						// attempts
						currentTime = System.currentTimeMillis();
						int currentBackOff = noFailedFetches <= maxFetchRetriesPerMap ? BACKOFF_INIT
								* (1 << (noFailedFetches - 1))
								: (this.maxBackoff * 1000 / 2);
						penaltyBox.put(cr.getHost(), currentTime
								+ currentBackOff);
						LOG.warn(reduceTask.getTaskID() + " adding host "
								+ cr.getHost()
								+ " to penalty box, next contact in "
								+ (currentBackOff / 1000) + " seconds");
					}
					uniqueHosts.remove(cr.getHost());
					numInFlight--;
				}
			}

			// all done, inform the copiers to exit
			exitGetMapEvents = true;
			try {
				getMapEventsThread.join();
				LOG.info("getMapsEventsThread joined.");
			} catch (InterruptedException ie) {
				LOG.info("getMapsEventsThread threw an exception: "
						+ StringUtils.stringifyException(ie));
			}

			synchronized (copiers) {
				synchronized (scheduledCopies) {
					for (MapOutputCopier copier : copiers) {
						copier.interrupt();
					}
					copiers.clear();
				}
			}

			// copiers are done, exit and notify the waiting merge threads
			synchronized (mapOutputFilesOnDisk) {
				exitLocalFSMerge = true;
				mapOutputFilesOnDisk.notify();
			}

			ramManager.close();

			// Do a merge of in-memory files (if there are any)
			if (mergeThrowable == null) {
				try {
					// Wait for the on-disk merge to complete
					localFSMergerThread.join();
					LOG.info("Interleaved on-disk merge complete: "
							+ mapOutputFilesOnDisk.size() + " files left.");

					// wait for an ongoing merge (if it is in flight) to
					// complete
					inMemFSMergeThread.join();
					LOG.info("In-memory merge complete: "
							+ mapOutputsFilesInMemory.size() + " files left.");
				} catch (InterruptedException ie) {
					LOG.warn(reduceTask.getTaskID()
							+ " Final merge of the inmemory files threw an exception: "
							+ StringUtils.stringifyException(ie));
					// check if the last merge generated an error
					if (mergeThrowable != null) {
						mergeThrowable = ie;
					}
					return false;
				}
			}
			return mergeThrowable == null && copiedMapOutputs.size() == numReduces;
		}

		private long createInMemorySegments(
				List<Segment<K, V>> inMemorySegments, long leaveBytes)
				throws IOException {
			long totalSize = 0L;
			synchronized (mapOutputsFilesInMemory) {
				// fullSize could come from the RamManager, but files can be
				// closed but not yet present in mapOutputsFilesInMemory
				long fullSize = 0L;
				for (MapOutput mo : mapOutputsFilesInMemory) {
					fullSize += mo.data.length;
				}
				while (fullSize > leaveBytes) {
					MapOutput mo = mapOutputsFilesInMemory.remove(0);
					totalSize += mo.data.length;
					fullSize -= mo.data.length;
					Reader<K, V> reader = new InMemoryReader<K, V>(ramManager,
							mo.mapAttemptId, mo.data, 0, mo.data.length);
					Segment<K, V> segment = new Segment<K, V>(reader, true);
					inMemorySegments.add(segment);
				}
			}
			return totalSize;
		}

		/**
		 * Create a RawKeyValueIterator from copied map outputs. All copying
		 * threads have exited, so all of the map outputs are available either
		 * in memory or on disk. We also know that no merges are in progress, so
		 * synchronization is more lax, here.
		 * 
		 * The iterator returned must satisfy the following constraints: 1.
		 * Fewer than io.sort.factor files may be sources 2. No more than
		 * maxInMemReduce bytes of map outputs may be resident in memory when
		 * the reduce begins
		 * 
		 * If we must perform an intermediate merge to satisfy (1), then we can
		 * keep the excluded outputs from (2) in memory and include them in the
		 * first merge pass. If not, then said outputs must be written to disk
		 * first.
		 */
		@SuppressWarnings("unchecked")
		private RawKeyValueIterator createKVIterator(JobConf job,
				FileSystem fs, Reporter reporter) throws IOException {

			// merge config params
			Class<K> keyClass = (Class<K>) job.getMapOutputKeyClass();
			Class<V> valueClass = (Class<V>) job.getMapOutputValueClass();
			boolean keepInputs = job.getKeepFailedTaskFiles();
			final Path tmpDir = new Path(getTaskID().toString());
			final RawComparator<K> comparator = (RawComparator<K>) job
					.getOutputKeyComparator();

			// segments required to vacate memory
			List<Segment<K, V>> memDiskSegments = new ArrayList<Segment<K, V>>();
			long inMemToDiskBytes = 0;
			if (mapOutputsFilesInMemory.size() > 0) {
				TaskID mapId = mapOutputsFilesInMemory.get(0).mapId;
				inMemToDiskBytes = createInMemorySegments(memDiskSegments,
						maxInMemReduce);
				final int numMemDiskSegments = memDiskSegments.size();
				if (numMemDiskSegments > 0
						&& ioSortFactor > mapOutputFilesOnDisk.size()) {
					// must spill to disk, but can't retain in-mem for
					// intermediate merge
					final Path outputPath = mapOutputFile.getInputFileForWrite(
							mapId, reduceTask.getTaskID(), inMemToDiskBytes,
							round);
					final RawKeyValueIterator rIter = Merger.merge(job, fs,
							keyClass, valueClass, memDiskSegments,
							numMemDiskSegments, tmpDir, comparator, reporter,
							spilledRecordsCounter, null);
					final Writer writer = new Writer(job, fs, outputPath,
							keyClass, valueClass, codec, null);
					try {
						Merger.writeFile(rIter, writer, reporter, job);
						addToMapOutputFilesOnDisk(fs.getFileStatus(outputPath));
					} catch (Exception e) {
						if (null != outputPath) {
							fs.delete(outputPath, true);
						}
						throw new IOException("Final merge failed", e);
					} finally {
						if (null != writer) {
							writer.close();
						}
					}
					LOG.info("Merged " + numMemDiskSegments + " segments, "
							+ inMemToDiskBytes + " bytes to disk to satisfy "
							+ "reduce memory limit");
					inMemToDiskBytes = 0;
					memDiskSegments.clear();
				} else if (inMemToDiskBytes != 0) {
					LOG.info("Keeping " + numMemDiskSegments + " segments, "
							+ inMemToDiskBytes + " bytes in memory for "
							+ "intermediate, on-disk merge");
				}
			}

			// segments on disk
			List<Segment<K, V>> diskSegments = new ArrayList<Segment<K, V>>();
			long onDiskBytes = inMemToDiskBytes;
			Path[] onDisk = getMapFiles(fs, false);
			for (Path file : onDisk) {
				onDiskBytes += fs.getFileStatus(file).getLen();
				diskSegments.add(new Segment<K, V>(job, fs, file, codec,
						keepInputs));
			}
			LOG.info("Merging " + onDisk.length + " files, " + onDiskBytes
					+ " bytes from disk");
			Collections.sort(diskSegments, new Comparator<Segment<K, V>>() {
				public int compare(Segment<K, V> o1, Segment<K, V> o2) {
					if (o1.getLength() == o2.getLength()) {
						return 0;
					}
					return o1.getLength() < o2.getLength() ? -1 : 1;
				}
			});

			// build final list of segments from merged backed by disk + in-mem
			List<Segment<K, V>> finalSegments = new ArrayList<Segment<K, V>>();
			long inMemBytes = createInMemorySegments(finalSegments, 0);
			LOG.info("Merging " + finalSegments.size() + " segments, "
					+ inMemBytes + " bytes from memory into reduce");
			if (0 != onDiskBytes) {
				final int numInMemSegments = memDiskSegments.size();
				diskSegments.addAll(0, memDiskSegments);
				memDiskSegments.clear();
				RawKeyValueIterator diskMerge = Merger.merge(job, fs, keyClass,
						valueClass, codec, diskSegments, ioSortFactor,
						numInMemSegments, tmpDir, comparator, reporter, false,
						spilledRecordsCounter, null);
				diskSegments.clear();
				if (0 == finalSegments.size()) {
					return diskMerge;
				}
				finalSegments.add(new Segment<K, V>(new RawKVIteratorReader(
						diskMerge, onDiskBytes), true));
			}
			return Merger.merge(job, fs, keyClass, valueClass, finalSegments,
					finalSegments.size(), tmpDir, comparator, reporter,
					spilledRecordsCounter, null);
		}

		class RawKVIteratorReader extends IFile.Reader<K, V> {

			private final RawKeyValueIterator kvIter;

			public RawKVIteratorReader(RawKeyValueIterator kvIter, long size)
					throws IOException {
				super(null, null, size, null, spilledRecordsCounter);
				this.kvIter = kvIter;
			}

			public boolean next(DataInputBuffer key, DataInputBuffer value)
					throws IOException {
				if (kvIter.next()) {
					final DataInputBuffer kb = kvIter.getKey();
					final DataInputBuffer vb = kvIter.getValue();
					final int kp = kb.getPosition();
					final int klen = kb.getLength() - kp;
					key.reset(kb.getData(), kp, klen);
					final int vp = vb.getPosition();
					final int vlen = vb.getLength() - vp;
					value.reset(vb.getData(), vp, vlen);
					bytesRead += klen + vlen;
					return true;
				}
				return false;
			}

			public long getPosition() throws IOException {
				return bytesRead;
			}

			public void close() throws IOException {
				kvIter.close();
			}
		}

		private CopyResult getCopyResult(int numInFlight) {
			synchronized (copyResults) {
				while (copyResults.isEmpty()) {
					try {
						// The idea is that if we have scheduled enough, we can
						// wait until
						// we hear from one of the copiers.
						if (busyEnough(numInFlight)) {
							copyResults.wait();
						} else {
							return null;
						}
					} catch (InterruptedException e) {
					}
				}
				return copyResults.remove(0);
			}
		}

		private void addToMapOutputFilesOnDisk(FileStatus status) {
			synchronized (mapOutputFilesOnDisk) {
				mapOutputFilesOnDisk.add(status);
				mapOutputFilesOnDisk.notify();
			}
		}

		/**
		 * Starts merging the local copy (on disk) of the map's output so that
		 * most of the reducer's input is sorted i.e overlapping shuffle and
		 * merge phases.
		 */
		private class LocalFSMerger extends Thread {
			private LocalFileSystem localFileSys;

			public LocalFSMerger(LocalFileSystem fs) {
				this.localFileSys = fs;
				setName("Thread for merging on-disk files");
				setDaemon(true);
			}

			@SuppressWarnings("unchecked")
			public void run() {
				try {
					LOG.info(reduceTask.getTaskID() + " Thread started: "
							+ getName());
					while (!exitLocalFSMerge) {
						synchronized (mapOutputFilesOnDisk) {
							while (!exitLocalFSMerge
									&& mapOutputFilesOnDisk.size() < (2 * ioSortFactor - 1)) {
								LOG.info(reduceTask.getTaskID()
										+ " Thread waiting: " + getName());
								mapOutputFilesOnDisk.wait();
							}
						}
						if (exitLocalFSMerge) {// to avoid running one extra
							// time in the end
							break;
						}
						List<Path> reduceFiles = new ArrayList<Path>();
						long approxOutputSize = 0;
						int bytesPerSum = reduceTask.getConf().getInt(
								"io.bytes.per.checksum", 512);
						LOG.info(reduceTask.getTaskID() + "We have  "
								+ mapOutputFilesOnDisk.size()
								+ " map outputs on disk. "
								+ "Triggering merge of " + ioSortFactor
								+ " files");
						// 1. Prepare the list of files to be merged. This list
						// is prepared
						// using a list of map output files on disk. Currently
						// we merge
						// io.sort.factor files into 1.
						synchronized (mapOutputFilesOnDisk) {
							for (int i = 0; i < ioSortFactor; ++i) {
								FileStatus filestatus = mapOutputFilesOnDisk
										.first();
								mapOutputFilesOnDisk.remove(filestatus);
								reduceFiles.add(filestatus.getPath());
								approxOutputSize += filestatus.getLen();
							}
						}

						// sanity check
						if (reduceFiles.size() == 0) {
							return;
						}

						// add the checksum length
						approxOutputSize += ChecksumFileSystem
								.getChecksumLength(approxOutputSize,
										bytesPerSum);

						// 2. Start the on-disk merge process
						Path outputPath = lDirAlloc.getLocalPathForWrite(
								reduceFiles.get(0).toString(), approxOutputSize,
								conf).suffix(".merged");
						Writer writer = new Writer(conf, rfs, outputPath,
								conf.getMapOutputKeyClass(),
								conf.getMapOutputValueClass(), codec, null);
						RawKeyValueIterator iter = null;
						Path tmpDir = new Path(reduceTask.getTaskID()
								.toString());
						try {
							iter = Merger
									.merge(conf, rfs, conf
											.getMapOutputKeyClass(), conf
											.getMapOutputValueClass(), codec,
											reduceFiles.toArray(new Path[reduceFiles
													.size()]), true,
											ioSortFactor, tmpDir, conf
													.getOutputKeyComparator(),
											reporter, spilledRecordsCounter,
											null);

							Merger.writeFile(iter, writer, reporter, conf);
							writer.close();
						} catch (Exception e) {
							localFileSys.delete(outputPath, true);
							throw new IOException(
									StringUtils.stringifyException(e));
						}

						synchronized (mapOutputFilesOnDisk) {
							addToMapOutputFilesOnDisk(localFileSys
									.getFileStatus(outputPath));
						}

						LOG.info(reduceTask.getTaskID()
								+ " Finished merging "
								+ reduceFiles.size()
								+ " map output files on disk of total-size "
								+ approxOutputSize
								+ "."
								+ " Local output file is "
								+ outputPath
								+ " of size "
								+ localFileSys.getFileStatus(outputPath)
										.getLen());
					}
				} catch (Exception e) {
					LOG.warn(reduceTask.getTaskID()
							+ " Merging of the local FS files threw an exception: "
							+ StringUtils.stringifyException(e));
					if (mergeThrowable == null) {
						mergeThrowable = e;
					}
				} catch (Throwable t) {
					String msg = getTaskID()
							+ " : Failed to merge on the local FS"
							+ StringUtils.stringifyException(t);
					reportFatalError(getTaskID(), t, msg);
				}
			}
		}

		private class InMemFSMergeThread extends Thread {

			public InMemFSMergeThread() {
				setName("Thread for merging in memory files");
				setDaemon(true);
			}

			public void run() {
				LOG.info(reduceTask.getTaskID() + " Thread started: "
						+ getName());
				try {
					boolean exit = false;
					do {
						exit = ramManager.waitForDataToMerge();
						if (!exit) {
							doInMemMerge();
						}
					} while (!exit);
				} catch (Exception e) {
					LOG.warn(reduceTask.getTaskID()
							+ " Merge of the inmemory files threw an exception: "
							+ StringUtils.stringifyException(e));
					ReduceCopier.this.mergeThrowable = e;
				} catch (Throwable t) {
					String msg = getTaskID() + " : Failed to merge in memory"
							+ StringUtils.stringifyException(t);
					reportFatalError(getTaskID(), t, msg);
				}
			}

			@SuppressWarnings("unchecked")
			private void doInMemMerge() throws IOException {
				if (mapOutputsFilesInMemory.size() == 0) {
					return;
				}

				// name this output file same as the name of the first file that
				// is
				// there in the current list of inmem files (this is guaranteed
				// to
				// be absent on the disk currently. So we don't overwrite a
				// prev.
				// created spill). Also we need to create the output file now
				// since
				// it is not guaranteed that this file will be present after
				// merge
				// is called (we delete empty files as soon as we see them
				// in the merge method)

				// figure out the mapId
				TaskID mapId = mapOutputsFilesInMemory.get(0).mapId;

				List<Segment<K, V>> inMemorySegments = new ArrayList<Segment<K, V>>();
				long mergeOutputSize = createInMemorySegments(inMemorySegments,
						0);
				int noInMemorySegments = inMemorySegments.size();

				Path outputPath = mapOutputFile.getInputFileForWrite(mapId,
						reduceTask.getTaskID(), mergeOutputSize, round);
				
				Writer writer = new Writer(conf, rfs, outputPath,
						conf.getMapOutputKeyClass(),
						conf.getMapOutputValueClass(), codec, null);

				RawKeyValueIterator rIter = null;
				try {
					LOG.info("Initiating in-memory merge with "
							+ noInMemorySegments + " segments...");

					rIter = Merger.merge(conf, rfs,
							(Class<K>) conf.getMapOutputKeyClass(),
							(Class<V>) conf.getMapOutputValueClass(),
							inMemorySegments, inMemorySegments.size(),
							new Path(reduceTask.getTaskID().toString()),
							conf.getOutputKeyComparator(), reporter,
							spilledRecordsCounter, null);

					if (combinerRunner == null) {
						Merger.writeFile(rIter, writer, reporter, conf);
					} else {
						combineCollector.setWriter(writer);
						combinerRunner.combine(rIter, combineCollector);
					}
					writer.close();

					LOG.info(reduceTask.getTaskID() + " Merge of the "
							+ noInMemorySegments + " files in-memory complete."
							+ " Local file is " + outputPath + " of size "
							+ localFileSys.getFileStatus(outputPath).getLen());
				} catch (Exception e) {
					// make sure that we delete the ondisk file that we created
					// earlier when we invoked cloneFileAttributes
					localFileSys.delete(outputPath, true);
					throw (IOException) new IOException(
							"Intermediate merge failed").initCause(e);
				}

				// Note the output of the merge
				FileStatus status = localFileSys.getFileStatus(outputPath);
				synchronized (mapOutputFilesOnDisk) {
					addToMapOutputFilesOnDisk(status);
				}
			}
		}

		private class GetMapEventsThread extends Thread {

			private IntWritable fromEventId = new IntWritable(0);
			private static final long SLEEP_TIME = 1000;

			public GetMapEventsThread() {
				setName("Thread for polling Map Completion Events");
				setDaemon(true);
			}

			@Override
			public void run() {

				LOG.info(reduceTask.getTaskID() + " Thread started: "
						+ getName());

				do {
					try {
						// LOG.info("get events from " + fromEventId.get());
						int numNewMaps = getMapCompletionEvents();
						if (numNewMaps > 0) {
							LOG.info(reduceTask.getTaskID() + ": " + "Got "
									+ numNewMaps + " new map-outputs");
						}
						Thread.sleep(SLEEP_TIME);
					} catch (InterruptedException e) {
						LOG.warn(reduceTask.getTaskID()
								+ " GetMapEventsThread returning after an "
								+ " interrupted exception");
						return;
					} catch (Throwable t) {
						String msg = reduceTask.getTaskID()
								+ " GetMapEventsThread Ignoring exception : "
								+ StringUtils.stringifyException(t);
						reportFatalError(getTaskID(), t, msg);
					}
				} while (!exitGetMapEvents);

				LOG.info("GetMapEventsThread exiting");

			}

			/**
			 * Queries the {@link TaskTracker} for a set of map-completion
			 * events from a given event ID.
			 * 
			 * @throws IOException
			 */
			private int getMapCompletionEvents() throws IOException {

				int numNewMaps = 0;

				MapTaskCompletionEventsUpdate update = umbilical
						.getMapCompletionEvents(reduceTask.getJobID(),
								fromEventId.get(), MAX_EVENTS_TO_FETCH,
								reduceTask.getTaskID());
				LOG.info("query completion from " + fromEventId.get());
				TaskCompletionEvent events[] = update
						.getMapTaskCompletionEvents();

				// for (int i = 0; i < events.length; i++)
				// System.out.println(events[i] + " from "
				// + events[i].getTaskTrackerHttp());

				// Check if the reset is required.
				// Since there is no ordering of the task completion events at
				// the
				// reducer, the only option to sync with the new jobtracker is
				// to reset
				// the events index
				if (update.shouldReset()) {
					fromEventId.set(0);
					obsoleteMapIds.clear(); // clear the obsolete map
					mapLocations.clear(); // clear the map locations mapping
				}

				// Update the last seen event ID
				fromEventId.set(fromEventId.get() + events.length);

				// Process the TaskCompletionEvents:
				// 1. Save the SUCCEEDED maps in knownOutputs to fetch the
				// outputs.
				// 2. Save the OBSOLETE/FAILED/KILLED maps in obsoleteOutputs to
				// stop
				// fetching from those maps.
				// 3. Remove TIPFAILED maps from neededOutputs since we don't
				// need their
				// outputs at all.
				for (TaskCompletionEvent event : events) {

					LOG.info("completion message from: "
							+ event.getTaskTrackerHttp());

					switch (event.getTaskStatus()) {
					case SUCCEEDED:

					{
						URI u = URI.create(event.getTaskTrackerHttp());
						String host = u.getHost();
						TaskAttemptID taskId = event.getTaskAttemptId();
						int duration = event.getTaskRunTime();
						if (duration > maxMapRuntime) {
							maxMapRuntime = duration;
							// adjust max-fetch-retries based on
							// max-map-run-time
							maxFetchRetriesPerMap = Math
									.max(MIN_FETCH_RETRIES_PER_MAP,
											getClosestPowerOf2((maxMapRuntime / BACKOFF_INIT) + 1));
						}
						URL mapOutputLocation = new URL(
								event.getTaskTrackerHttp() + "/mapOutput?job="
										+ taskId.getJobID() + "&map=" + taskId
										+ "&reduce=" + getPartition()
										+ "&iteration=" + round);
						List<MapOutputLocation> loc = mapLocations.get(host);
						if (loc == null) {
							loc = Collections
									.synchronizedList(new LinkedList<MapOutputLocation>());
							mapLocations.put(host, loc);
						}
						loc.add(new MapOutputLocation(taskId, host,
								mapOutputLocation));
						LOG.info(mapOutputLocation.toString());
						numNewMaps++;
					}
						break;
					case FAILED:
					case KILLED:
					case OBSOLETE: {
						obsoleteMapIds.add(event.getTaskAttemptId());
						LOG.info("Ignoring obsolete output of "
								+ event.getTaskStatus() + " map-task: '"
								+ event.getTaskAttemptId() + "'");
					}
						break;
					case TIPFAILED: {
						copiedMapOutputs.add(event.getTaskAttemptId()
								.getTaskID());
						LOG.info("Ignoring output of failed map TIP: '"
								+ event.getTaskAttemptId() + "'");
					}
						break;
					}
				}
				return numNewMaps;
			}
		}
	}

	/**
	 * Return the exponent of the power of two closest to the given positive
	 * value, or zero if value leq 0. This follows the observation that the msb
	 * of a given value is also the closest power of two, unless the bit
	 * following it is set.
	 */
	private static int getClosestPowerOf2(int value) {
		if (value <= 0)
			throw new IllegalArgumentException("Undefined for " + value);
		final int hob = Integer.highestOneBit(value);
		return Integer.numberOfTrailingZeros(hob)
				+ (((hob >>> 1) & value) == 0 ? 0 : 1);
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
 class ReduceOutputBuffer<K extends Object, V extends Object> implements
	OutputCollector<K, V>, IndexedSortable	{

	private final int partitions;
	private final JobConf job;
	private final TaskReporter reporter;
	private final Class<K> keyClass;
	private final Class<V> valClass;
	private final RawComparator<K> comparator;
	private final SerializationFactory serializationFactory;
	private final Serializer<K> keySerializer;
	private final Serializer<V> valSerializer;
	private final CombinerRunner<K, V> combinerRunner;
	private final CombineOutputCollector<K, V> combineCollector;

	// Compression for map-outputs
	private CompressionCodec codec = null;

	// k/v accounting
	private volatile int kvstart = 0; // marks beginning of spill
	private volatile int kvend = 0; // marks beginning of collectable
	private int kvindex = 0; // marks end of collected
	private final int[] kvoffsets; // indices into kvindices
	private final int[] kvindices; // partition, k/v offsets into kvbuffer
	private volatile int bufstart = 0; // marks beginning of spill
	private volatile int bufend = 0; // marks beginning of collectable
	private volatile int bufvoid = 0; // marks the point where we should
	// stop
	// reading at the end of the buffer
	private int bufindex = 0; // marks end of collected
	private int bufmark = 0; // marks end of record
	private byte[] kvbuffer; // main output buffer
	private static final int PARTITION = 0; // partition offset in acct
	private static final int KEYSTART = 1; // key offset in acct
	private static final int VALSTART = 2; // val offset in acct
	private static final int ACCTSIZE = 3; // total #fields in acct
	private static final int RECSIZE = (ACCTSIZE + 1) * 4; // acct bytes per
	// record

	// spill accounting
	private volatile int numSpills = 0;
	private volatile Throwable sortSpillException = null;
	private final int softRecordLimit;
	private final int softBufferLimit;
	private final int minSpillsForCombine;
	private final IndexedSorter sorter;
	private final ReentrantLock spillLock = new ReentrantLock();
	private final Condition spillDone = spillLock.newCondition();
	private final Condition spillReady = spillLock.newCondition();
	private final BlockingBuffer bb = new BlockingBuffer();
	private volatile boolean spillThreadRunning = false;
	private final SpillThread spillThread = new SpillThread();

	private final FileSystem localFs;
	private final FileSystem rfs;
    private  final FileSystem dfs;
	private final Counters.Counter mapOutputByteCounter;
	private final Counters.Counter mapOutputRecordCounter;
	private final Counters.Counter combineOutputCounter;

	private ArrayList<SpillRecord> indexCacheList;
	private int totalIndexCacheMemory;
	private static final int INDEX_CACHE_MEMORY_LIMIT = 1024 * 1024;

	@SuppressWarnings("unchecked")
	public ReduceOutputBuffer(TaskUmbilicalProtocol umbilical, JobConf job,
			TaskReporter reporter) throws IOException,
			ClassNotFoundException {
		this.job = job;
		this.reporter = reporter;
		localFs = FileSystem.getLocal(job);
		dfs=FileSystem.get(job);
		partitions = job.getNumReduceTasks();

		rfs = ((LocalFileSystem) localFs).getRaw();

		indexCacheList = new ArrayList<SpillRecord>();

		// sanity checks
		final float spillper = job.getFloat("io.sort.spill.percent",
				(float) 0.8);
		final float recper = job.getFloat("io.sort.record.percent",
				(float) 0.05);
		final String childheap =conf.get("mapred.child.java.opts", "-Xmx201m");
		final String childheaps =job.get("mapred.child.java.opts", "-Xmx201m");
		final int sortmb = job.getInt("io.sort.mb", 100);
		if (spillper > (float) 1.0 || spillper < (float) 0.0) {
			throw new IOException("Invalid \"io.sort.spill.percent\": "
					+ spillper);
		}
		if (recper > (float) 1.0 || recper < (float) 0.01) {
			throw new IOException("Invalid \"io.sort.record.percent\": "
					+ recper);
		}
		if ((sortmb & 0x7FF) != sortmb) {
			throw new IOException("Invalid \"io.sort.mb\": " + sortmb);
		}
		sorter = ReflectionUtils.newInstance(job.getClass("map.sort.class",
				QuickSort.class, IndexedSorter.class), job);
		LOG.info("io.sort.mb = " + sortmb);
		LOG.info("mapred.child.java.opts"+childheap+"  heaps"+childheaps);
		// buffers and accounting
		int maxMemUsage = sortmb << 20;
		maxMemUsage = (int)Math.min(Runtime.getRuntime().maxMemory() * 0.7f, maxMemUsage);
		int recordCapacity = (int) (maxMemUsage * recper);
		recordCapacity -= recordCapacity % RECSIZE;
		kvbuffer = new byte[maxMemUsage - recordCapacity];
		bufvoid = kvbuffer.length;
		recordCapacity /= RECSIZE;
		kvoffsets = new int[recordCapacity];
		kvindices = new int[recordCapacity * ACCTSIZE];
		softBufferLimit = (int) (kvbuffer.length * spillper);
		softRecordLimit = (int) (kvoffsets.length * spillper);
		LOG
				.info("data buffer = " + softBufferLimit + "/"
						+ kvbuffer.length);
		LOG.info("record buffer = " + softRecordLimit + "/"
				+ kvoffsets.length);
		// k/v serialization
		comparator = job.getReduceOutputKeyComparator();
		//comparator1=WritableComparator.get(job.getMapOutputKeyClass().asSubclass(WritableComparable.class));
		//WritableComparator.get(new Text().getClass().asSubclass(WritableComparable.class));
		//WritableComparator.get(new Text().getClass().asSubclass(WritableComparable.class));
		 LOG.warn("comparator is !!!!!!"+comparator.getClass().getName());
         LOG.warn("keyClass"+ job.getOutputKeyClass().asSubclass(WritableComparable.class));	 
		keyClass = (Class<K>) job.getOutputKeyClass();
		valClass = (Class<V>) job.getOutputValueClass();
		serializationFactory = new SerializationFactory(job);
		keySerializer = serializationFactory.getSerializer(keyClass);
		keySerializer.open(bb);
		valSerializer = serializationFactory.getSerializer(valClass);
		valSerializer.open(bb);
		LOG.info("serializationFactory 4005 ");
		// counters
		mapOutputByteCounter = reporter.getCounter(MAP_OUTPUT_BYTES);
		mapOutputRecordCounter = reporter.getCounter(MAP_OUTPUT_RECORDS);
		Counters.Counter combineInputCounter = reporter
				.getCounter(COMBINE_INPUT_RECORDS);
		combineOutputCounter = reporter.getCounter(COMBINE_OUTPUT_RECORDS);
		// compression
		/*if (job.getCompressMapOutput()) {
			Class<? extends CompressionCodec> codecClass = job
					.getMapOutputCompressorClass(DefaultCodec.class);
			codec = ReflectionUtils.newInstance(codecClass, job);
		}*/
		// combiner
		combinerRunner = CombinerRunner.create(job, getTaskID(),
				combineInputCounter, reporter, null);
		if (combinerRunner != null) {
			combineCollector = new CombineOutputCollector<K, V>(
					combineOutputCounter);
		} else {
			combineCollector = null;
		}
		minSpillsForCombine = job.getInt("min.num.spills.for.combine", 3);
		spillThread.setDaemon(true);
		spillThread.setName("SpillThread");
		spillLock.lock();
		LOG.info("serializationFactory 4031");
		try {
			spillThread.start();
			while (!spillThreadRunning) {
				spillDone.await();
			}
		} catch (InterruptedException e) {
			throw (IOException) new IOException(
					"Spill thread failed to initialize")
					.initCause(sortSpillException);
		} finally {
			spillLock.unlock();
		}
		if (sortSpillException != null) {
			throw (IOException) new IOException(
					"Spill thread failed to initialize")
					.initCause(sortSpillException);
		}
	}

	public synchronized void collect(K key, V value)
			throws IOException {
		reporter.progress();
		if (key.getClass() != keyClass) {   //�ж�map��key�Ƿ�������õĸ�ʽ
			throw new IOException(
					"Type mismatch in key from reduce: expected "
							+ keyClass.getName() + ", recieved "
							+ key.getClass().getName());
		}
		//LOG.info("@@@@@@@@@@@@@@@@buffer collect    "+key+"   "+value);
		if (value.getClass() != valClass) {
			throw new IOException(
					"Type mismatch in value from reduce: expected "
							+ valClass.getName() + ", recieved "
							+ value.getClass().getName());
		}
		final int kvnext = (kvindex + 1) % kvoffsets.length;
		spillLock.lock();
		try {
			boolean kvfull;
			do {
				if (sortSpillException != null) {
					throw (IOException) new IOException("Spill failed")
							.initCause(sortSpillException);
				}
				// sufficient acct space
				kvfull = kvnext == kvstart;
				final boolean kvsoftlimit = ((kvnext > kvend) ? kvnext//kvsoft �����Ƿ�ﵽ�˿�ʼ�����ʱ����
						- kvend > softRecordLimit
						: kvend - kvnext <= kvoffsets.length
								- softRecordLimit);
				if (kvstart == kvend && kvsoftlimit) {
					LOG.info("Spilling  reduce output: record full = "
							+ kvsoftlimit);
					startSpill();
				}
				if (kvfull) {
					try {
						while (kvstart != kvend) {
							reporter.progress();
							spillDone.await();
						}
					} catch (InterruptedException e) {
						throw (IOException) new IOException(
								"Collector interrupted while waiting for the writer")
								.initCause(e);
					}
				}
			} while (kvfull);
		} finally {
			spillLock.unlock();
		}

		try {
			// serialize key bytes into buffer
			int keystart = bufindex;
			//LOG.info("keySerializer.serialize(key); "+key);
			keySerializer.serialize(key);
			if (bufindex < keystart) {
				// wrapped the key; reset required
				bb.reset();
				keystart = 0;
			}
			// serialize value bytes into buffer
			final int valstart = bufindex;
			valSerializer.serialize(value);
			int valend = bb.markRecord();

			/*if (partition < 0 || partition >= partitions) {
				throw new IOException("Illegal partition for " + key + " ("
						+ partition + ")");
			}*/

			mapOutputRecordCounter.increment(1);
			mapOutputByteCounter.increment(valend >= keystart ? valend
					- keystart : (bufvoid - keystart) + valend);
			// update accounting info
			int ind = kvindex * ACCTSIZE;
			kvoffsets[kvindex] = ind;
			//kvindices[ind + PARTITION] = partition;
			kvindices[ind + KEYSTART] = keystart;
			kvindices[ind + VALSTART] = valstart;
			kvindex = kvnext;
		} catch (ReduceBufferTooSmallException e) {
			LOG.info("Record too large for in-memory buffer: "
					+ e.getMessage());
			spillSingleRecord(key, value);
			mapOutputRecordCounter.increment(1);
			return;
		}

	}

	/**
	 * Compare logical range, st i, j MOD offset capacity. Compare by
	 * partition, then by key.
	 * 
	 * @see IndexedSortable#compare
	 */
	public int compare(int i, int j) {
		final int ii = kvoffsets[i % kvoffsets.length];
		final int ij = kvoffsets[j % kvoffsets.length];
		// sort by partition
		if (kvindices[ii + PARTITION] != kvindices[ij + PARTITION]) {
			return kvindices[ii + PARTITION] - kvindices[ij + PARTITION];
		}
		// sort by key
		/*DataInputBuffer buffer = new DataInputBuffer();
		WritableComparable key3=;
		try {
			buffer.reset(kvbuffer, kvindices[ii + KEYSTART], kvindices[ii + VALSTART] - kvindices[ii + KEYSTART]); 
			key3.readFields(buffer);
			 System.out.println("***DEBUG***** key3"+key3.getClass());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}   */
		return comparator.compare(kvbuffer, kvindices[ii + KEYSTART],
				kvindices[ii + VALSTART] - kvindices[ii + KEYSTART],
				kvbuffer, kvindices[ij + KEYSTART],
				kvindices[ij + VALSTART] - kvindices[ij + KEYSTART]);
	}

	/**
	 * Swap logical indices st i, j MOD offset capacity.
	 * 
	 * @see IndexedSortable#swap
	 */
	public void swap(int i, int j) {
		i %= kvoffsets.length;
		j %= kvoffsets.length;
		int tmp = kvoffsets[i];
		kvoffsets[i] = kvoffsets[j];
		kvoffsets[j] = tmp;
	}

	/**
	 * Inner class managing the spill of serialized records to disk.
	 */
	protected class BlockingBuffer extends DataOutputStream {

		public BlockingBuffer() {
			this(new Buffer());
		}

		private BlockingBuffer(OutputStream out) {
			super(out);
		}

		/**
		 * Mark end of record. Note that this is required if the buffer is
		 * to cut the spill in the proper place.
		 */
		public int markRecord() {
			bufmark = bufindex;
			return bufindex;
		}

		/**
		 * Set position from last mark to end of writable buffer, then
		 * rewrite the data between last mark and kvindex. This handles a
		 * special case where the key wraps around the buffer. If the key is
		 * to be passed to a RawComparator, then it must be contiguous in
		 * the buffer. This recopies the data in the buffer back into
		 * itself, but starting at the beginning of the buffer. Note that
		 * reset() should <b>only</b> be called immediately after detecting
		 * this condition. To call it at any other time is undefined and
		 * would likely result in data loss or corruption.
		 * 
		 * @see #markRecord()
		 */
		protected synchronized void reset() throws IOException {
			// spillLock unnecessary; If spill wraps, then
			// bufindex < bufstart < bufend so contention is impossible
			// a stale value for bufstart does not affect correctness, since
			// we can only get false negatives that force the more
			// conservative path
			int headbytelen = bufvoid - bufmark;
			bufvoid = bufmark;
			if (bufindex + headbytelen < bufstart) {
				System.arraycopy(kvbuffer, 0, kvbuffer, headbytelen,
						bufindex);
				System.arraycopy(kvbuffer, bufvoid, kvbuffer, 0,
						headbytelen);
				bufindex += headbytelen;
			} else {
				byte[] keytmp = new byte[bufindex];
				System.arraycopy(kvbuffer, 0, keytmp, 0, bufindex);
				bufindex = 0;
				out.write(kvbuffer, bufmark, headbytelen);
				out.write(keytmp);
			}
		}
	}

	public class Buffer extends OutputStream {
		private final byte[] scratch = new byte[1];

		@Override
		public synchronized void write(int v) throws IOException {
			scratch[0] = (byte) v;
			write(scratch, 0, 1);
		}

		/**
		 * Attempt to write a sequence of bytes to the collection buffer.
		 * This method will block if the spill thread is running and it
		 * cannot write.
		 * 
		 * @throws MapBufferTooSmallException
		 *             if record is too large to deserialize into the
		 *             collection buffer.
		 */
		@Override
		public synchronized void write(byte b[], int off, int len)
				throws IOException {
			boolean buffull = false;
			boolean wrap = false;
			spillLock.lock();
			try {
				do {
					if (sortSpillException != null) {
						throw (IOException) new IOException("Spill failed")
								.initCause(sortSpillException);
					}

					// sufficient buffer space?
					if (bufstart <= bufend && bufend <= bufindex) {
						buffull = bufindex + len > bufvoid;
						wrap = (bufvoid - bufindex) + bufstart > len;
					} else {
						// bufindex <= bufstart <= bufend
						// bufend <= bufindex <= bufstart
						wrap = false;
						buffull = bufindex + len > bufstart;
					}

					if (kvstart == kvend) {
						// spill thread not running
						if (kvend != kvindex) {
							// we have records we can spill
							final boolean bufsoftlimit = (bufindex > bufend) ? bufindex
									- bufend > softBufferLimit
									: bufend - bufindex < bufvoid
											- softBufferLimit;
							if (bufsoftlimit || (buffull && !wrap)) {
								LOG
										.info("Spilling reduce output: buffer full= "
												+ bufsoftlimit);
								startSpill();
							}
						} else if (buffull && !wrap) {
							// We have no buffered records, and this record
							// is too large
							// to write into kvbuffer. We must spill it
							// directly from
							// collect
							final int size = ((bufend <= bufindex) ? bufindex
									- bufend
									: (bufvoid - bufend) + bufindex)
									+ len;
							bufstart = bufend = bufindex = bufmark = 0;
							kvstart = kvend = kvindex = 0;
							bufvoid = kvbuffer.length;
							throw new ReduceBufferTooSmallException(size
									+ " bytes");
						}
					}

					if (buffull && !wrap) {
						try {
							while (kvstart != kvend) {
								reporter.progress();
								spillDone.await();
							}
						} catch (InterruptedException e) {
							throw (IOException) new IOException(
									"Buffer interrupted while waiting for the writer")
									.initCause(e);
						}
					}
				} while (buffull && !wrap);
			} finally {
				spillLock.unlock();
			}
			// here, we know that we have sufficient space to write
			if (buffull) {
				final int gaplen = bufvoid - bufindex;
				System.arraycopy(b, off, kvbuffer, bufindex, gaplen);
				len -= gaplen;
				off += gaplen;
				bufindex = 0;
			}
			System.arraycopy(b, off, kvbuffer, bufindex, len);
			bufindex += len;
		}
	}

	public synchronized void flush() throws IOException,
			ClassNotFoundException, InterruptedException {
		LOG.info("Starting flush of reduce output");
		spillLock.lock();
		try {
			while (kvstart != kvend) {
				reporter.progress();
				spillDone.await();
			}
			if (sortSpillException != null) {
				throw (IOException) new IOException("Spill failed")
						.initCause(sortSpillException);
			}
			if (kvend != kvindex) {
				kvend = kvindex;
				bufend = bufmark;
				sortAndSpill();
				LOG.info("Ending flush of reduce output");
			}
		} catch (InterruptedException e) {
			throw (IOException) new IOException(
					"Buffer interrupted while waiting for the writer")
					.initCause(e);
		} finally {
			spillLock.unlock();
		}
		assert !spillLock.isHeldByCurrentThread();
		// shut down spill thread and wait for it to exit. Since the
		// preceding
		// ensures that it is finished with its work (and sortAndSpill did
		// not
		// throw), we elect to use an interrupt instead of setting a flag.
		// Spilling simultaneously from this thread while the spill thread
		// finishes its work might be both a useful way to extend this and
		// also
		// sufficient motivation for the latter approach.
		try {
			spillThread.interrupt();
			spillThread.join();
		} catch (InterruptedException e) {
			throw (IOException) new IOException("Spill failed")
					.initCause(e);
		}
		// release sort buffer before the merge
		kvbuffer = null;
		LOG.info("start reduce output merge");
		//mergeParts();
	}

	public void close() {
	}

	protected class SpillThread extends Thread {

		@Override
		public void run() {
			spillLock.lock();
			spillThreadRunning = true;
			LOG.warn("SpillThreas start ##########################");
			try {
				while (true) {
					spillDone.signal();
					while (kvstart == kvend) {
						spillReady.await();
					}
					try {
						spillLock.unlock();
						sortAndSpill();
						LOG.warn("sortAndSpill start ##########################");
					} catch (Exception e) {
						sortSpillException = e;
					} catch (Throwable t) {
						sortSpillException = t;
						String logMsg = "Task " + getTaskID()
								+ " failed : "
								+ StringUtils.stringifyException(t);
						reportFatalError(getTaskID(), t, logMsg);
					} finally {
						spillLock.lock();
						if (bufend < bufindex && bufindex < bufstart) {
							bufvoid = kvbuffer.length;
						}
						kvstart = kvend;
						bufstart = bufend;
					}
				}
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			} finally {
				spillLock.unlock();
				spillThreadRunning = false;
			}
		}
	}

	private synchronized void startSpill() {
		LOG.info("bufstart = " + bufstart + "; bufend = " + bufmark
				+ "; bufvoid = " + bufvoid);
		LOG.info("kvstart = " + kvstart + "; kvend = " + kvindex
				+ "; length = " + kvoffsets.length);
		kvend = kvindex;
		bufend = bufmark;
		spillReady.signal();
	}

	private void sortAndSpill() throws IOException, ClassNotFoundException,
			InterruptedException {
		// approximate the length of the output file to be the length of the
		// buffer + header lengths for the partitions
		long size = (bufend >= bufstart ? bufend - bufstart
				: (bufvoid - bufend) + bufstart)
				+ partitions * APPROX_HEADER_LENGTH;
		FSDataOutputStream out = null;
		try {
			// create spill file
			final SpillRecord spillRec = new SpillRecord(partitions);
			/*final Path filename = mapOutputFile.getSpillFileForWrite(
					getTaskID(), numSpills, size, round);
			final Path filename = reduceOutputFile.getSpillFileForWrite(
					job,"leon");*/
//**********************************************************************			
				///String taskids=getTaskID().toString();
			//Path filename = FileOutputFormat.getTaskOutputPath(job, "parts");
			String finalName = getOutputName(getPartition());
			//String finalName=((Integer)partitions).toString();
			Path filename = FileOutputFormat.getTaskOutputPath(job, finalName);
			//Path filename = FileOutputFormat.getTaskOutputPath(job, "part-00001");
			//Path filename=new Path("/user/root/output/i0/part");
			LOG.info(dfs.exists(filename)+"dfs getname"+dfs.getName()+" dfs getclass+ " +
					" "+dfs.getClass()+"@@@@@@@@@@@@@@@@@"+filename.toString()+
					"@@@@@@@@@@partitionsToString :");
			//if(!dfs.exists(filename)){
			//if(!dfs.exists(filename)){
			out = dfs.create(filename)	;
			//out = rfs.create(filename);
			//}
			
			/*else{out=FSDataOutputStream(new BufferedOutputStream(
			        new LocalFSFileOutputStream(f, false), bufferSize), statistics);;}*/
			LOG.info("out have been created!");
			//LOG.info(rfs.exists(filename)+"#####"+filename.toString()+"@@@@@@@@@@out.toString():"+out.toString()+" size:"+size+" getPartition : "+ getPartition());
//*********************************************************************************leon
			final int endPosition = (kvend > kvstart) ? kvend
					: kvoffsets.length + kvend;
			sorter.sort(ReduceOutputBuffer.this, kvstart, endPosition,
					reporter);
			LOG.warn("#####"+"sorter.sort"+"@@@@@@@@@@");		
			int spindex = kvstart;
			IndexRecord rec = new IndexRecord();
			InMemValBytes value = new InMemValBytes();
			for (int i = 0; i < partitions; ++i) {
				IFile.Writer<K, V> writer = null;
				try {
					long segmentStart = out.getPos();
					writer = new Writer<K, V>(job, out, keyClass, valClass,
							codec, spilledRecordsCounter);
					if (combinerRunner == null) {
						// spill directly
						DataInputBuffer key = new DataInputBuffer();
						while (spindex < endPosition
								&& kvindices[kvoffsets[spindex
										% kvoffsets.length]
										+ PARTITION] == i) {
							final int kvoff = kvoffsets[spindex
									% kvoffsets.length];
							getVBytesForOffset(kvoff, value);
							key
									.reset(kvbuffer, kvindices[kvoff
											+ KEYSTART], (kvindices[kvoff
											+ VALSTART] - kvindices[kvoff
											+ KEYSTART]));
							//LOG.warn("#####writer.append"+key.getLength()+value.length+"@@@@@@@@@@");
							/*LOG.info("writer.append : start");
							byte[] w=key.getData();
							int n=w.length;
							LOG.info("writer.append : length :" +n);
							for(int t=0 ;t<n;i++){
								LOG.info("writer.append :"+w[i]);
							}
							LOG.info("writer.append : end");*/			
							writer.append(key, value,"\t");
							++spindex;//дһ����¼
						}
					} else {
						int spstart = spindex;
						while (spindex < endPosition
								&& kvindices[kvoffsets[spindex
										% kvoffsets.length]
										+ PARTITION] == i) {
							++spindex;
						}
						// Note: we would like to avoid the combiner if
						// we've fewer
						// than some threshold of records for a partition
						if (spstart != spindex) {
							combineCollector.setWriter(writer);
							RawKeyValueIterator kvIter = new MRResultIterator(
									spstart, spindex);
							combinerRunner
									.combine(kvIter, combineCollector);
						}
					}

					// close the writer
					writer.close();

					// record offsets
					rec.startOffset = segmentStart;
					rec.rawLength = writer.getRawLength();
					rec.partLength = writer.getCompressedLength();
					spillRec.putIndex(rec, i);

					writer = null;
				} finally {
					if (null != writer)
						writer.close();
				}
			}
            /*IFile.Reader <K,V> reader= new IFile.Reader<K, V>(job, rfs, filename, codec, null);
			LOG.warn("@@@#"+reader.getLength());*/
			if (totalIndexCacheMemory >= INDEX_CACHE_MEMORY_LIMIT) {
				// create spill index file
				Path indexFilename = mapOutputFile
				.getSpillIndexFileForWrite(
						getTaskID(),
						numSpills,
						partitions * Reduce_OUTPUT_INDEX_RECORD_LENGTH,
						round);
				spillRec.writeToFile(indexFilename, job);
			} else {
				indexCacheList.add(spillRec);
				totalIndexCacheMemory += spillRec.size()
						* Reduce_OUTPUT_INDEX_RECORD_LENGTH;
			}
			LOG.info("Finished spill " + numSpills);
			++numSpills;
		} finally {
			if (out != null)
				out.close();
		}
	}

	/**
	 * Handles the degenerate case where serialization fails to fit in the
	 * in-memory buffer, so we must spill the record from collect directly
	 * to a spill file. Consider this "losing".
	 */
	private void spillSingleRecord(final K key, final V value)
			throws IOException {
		long size = kvbuffer.length + partitions * APPROX_HEADER_LENGTH;
		FSDataOutputStream out = null;
		try {
			// create spill file
			final SpillRecord spillRec = new SpillRecord(partitions);
			String taskids=getTaskID().toString();
			Path filename = FileOutputFormat.getTaskOutputPath(job, taskids);
			System.out.println("@@@@@@Single@@@@@@@@@@@"+filename.toString()+"@@@@@@@@@@");
			out = rfs.create(filename);

			// we don't run the combiner for a single record
			IndexRecord rec = new IndexRecord();
			for (int i = 0; i < partitions; ++i) {
				IFile.Writer<K, V> writer = null;
				try {
					long segmentStart = out.getPos();
					// Create a new codec, don't care!
					writer = new IFile.Writer<K, V>(job, out, keyClass,
							valClass, codec, spilledRecordsCounter);

					
						final long recordStart = out.getPos();
						writer.append(key, value);
						// Note that our map byte count will not be accurate
						// with
						// compression
						mapOutputByteCounter.increment(out.getPos()
								- recordStart);
					
					writer.close();

					// record offsets
					rec.startOffset = segmentStart;
					rec.rawLength = writer.getRawLength();
					rec.partLength = writer.getCompressedLength();
					spillRec.putIndex(rec, i);

					writer = null;
				} catch (IOException e) {
					if (null != writer)
						writer.close();
					throw e;
				}
			}
			/*if (totalIndexCacheMemory >= INDEX_CACHE_MEMORY_LIMIT) {
				// create spill index file
				Path indexFilename = reduceOutputFile
						.getSpillIndexFileForWrite(
								job,"index");
				spillRec.writeToFile(indexFilename, job);
			} else {
				indexCacheList.add(spillRec);
				totalIndexCacheMemory += spillRec.size()
						* Reduce_OUTPUT_INDEX_RECORD_LENGTH;
			}*/
			++numSpills;
		} finally {
			if (out != null)
				out.close();
		}
	}

	/**
	 * Given an offset, populate vbytes with the associated set of
	 * deserialized value bytes. Should only be called during a spill.
	 */
	private void getVBytesForOffset(int kvoff, InMemValBytes vbytes) {
		final int nextindex = (kvoff / ACCTSIZE == (kvend - 1 + kvoffsets.length)
				% kvoffsets.length) ? bufend
				: kvindices[(kvoff + ACCTSIZE + KEYSTART)
						% kvindices.length];
		int vallen = (nextindex >= kvindices[kvoff + VALSTART]) ? nextindex
				- kvindices[kvoff + VALSTART] : (bufvoid - kvindices[kvoff
				+ VALSTART])
				+ nextindex;
		vbytes.reset(kvbuffer, kvindices[kvoff + VALSTART], vallen);
	}

	/**
	 * Inner class wrapping valuebytes, used for appendRaw.
	 */
	protected class InMemValBytes extends DataInputBuffer {
		private byte[] buffer;
		private int start;
		private int length;

		public void reset(byte[] buffer, int start, int length) {
			this.buffer = buffer;
			this.start = start;
			this.length = length;

			if (start + length > bufvoid) {
				this.buffer = new byte[this.length];
				final int taillen = bufvoid - start;
				System.arraycopy(buffer, start, this.buffer, 0, taillen);
				System.arraycopy(buffer, 0, this.buffer, taillen, length
						- taillen);
				this.start = 0;
			}

			super.reset(this.buffer, this.start, this.length);
		}
	}

	protected class MRResultIterator implements RawKeyValueIterator {
		private final DataInputBuffer keybuf = new DataInputBuffer();
		private final InMemValBytes vbytes = new InMemValBytes();
		private final int end;
		private int current;

		public MRResultIterator(int start, int end) {
			this.end = end;
			current = start - 1;
		}

		public boolean next() throws IOException {
			return ++current < end;
		}

		public DataInputBuffer getKey() throws IOException {
			final int kvoff = kvoffsets[current % kvoffsets.length];
			keybuf.reset(kvbuffer, kvindices[kvoff + KEYSTART],
					kvindices[kvoff + VALSTART]
							- kvindices[kvoff + KEYSTART]);
			return keybuf;
		}

		public DataInputBuffer getValue() throws IOException {
			getVBytesForOffset(kvoffsets[current % kvoffsets.length],
					vbytes);
			return vbytes;
		}

		public Progress getProgress() {
			return null;
		}
        
		public void close() {
		}
	}
    
	
//use it	
	/*private void mergeParts() throws IOException, InterruptedException,
	ClassNotFoundException {
// get the approximate size of the final output/index files
       long finalOutFileSize = 0;
       long finalIndexFileSize = 0;
       final Path[] filename = new Path[numSpills];
       final TaskAttemptID reduceId = getTaskID();

for (int i = 0; i < numSpills; i++) {
	filename[i] = mapOutputFile.getSpillFile(reduceId, i, round);
	finalOutFileSize += rfs.getFileStatus(filename[i]).getLen();
}
if (numSpills == 1) { // the spill is the final output
	rfs.rename(filename[0], new Path(filename[0].getParent(),
			"file.out"));
	LOG.info("@@"+reduceId+" "+filename[0]+" ");
	if (indexCacheList.size() == 0) {
		rfs
				.rename(mapOutputFile.getSpillIndexFile(reduceId, 0,
						round), new Path(filename[0].getParent(),
						"file.out.index"));
	} else {
		indexCacheList.get(0)
				.writeToFile(
						new Path(filename[0].getParent(),
								"file.out.index"), job);
	}
	return;
}

// read in paged indices
for (int i = indexCacheList.size(); i < numSpills; ++i) {
	Path indexFileName = mapOutputFile.getSpillIndexFile(reduceId, i,
			round);
	indexCacheList.add(new SpillRecord(indexFileName, job));
}

// make correction in the length to include the sequence file header
// lengths for each partition
finalOutFileSize += partitions * APPROX_HEADER_LENGTH;
finalIndexFileSize = partitions * Reduce_OUTPUT_INDEX_RECORD_LENGTH;
Path finalOutputFile = mapOutputFile.getOutputFileForWrite(reduceId,
		finalOutFileSize, round);
String finalName=String.valueOf(partitions);
Path finalOutputFile= FileOutputFormat.getTaskOutputPath(job, finalName);
Path finalIndexFile = mapOutputFile.getOutputIndexFileForWrite(
		reduceId, finalIndexFileSize, round);

// The output stream for the final single output file
FSDataOutputStream finalOut = rfs.create(finalOutputFile, true,
		4096);

//FSDataOutputStream finalOut = dfs.create(finalOutputFile, true		4096);

if (numSpills == 0) {
	// create dummy files
	IndexRecord rec = new IndexRecord();
	SpillRecord sr = new SpillRecord(partitions);
	try {
		for (int i = 0; i < partitions; i++) {
			long segmentStart = finalOut.getPos();
			Writer<K, V> writer = new Writer<K, V>(job, finalOut,
					keyClass, valClass, codec, null);
			writer.close();
			rec.startOffset = segmentStart;
			rec.rawLength = writer.getRawLength();
			rec.partLength = writer.getCompressedLength();
			sr.putIndex(rec, i);
		}
		//sr.writeToFile(finalIndexFile, job);
	} finally {
		finalOut.close();
	}
	return;
}
{
	IndexRecord rec = new IndexRecord();
	final SpillRecord spillRec = new SpillRecord(partitions);
	for (int parts = 0; parts < partitions; parts++) {
		// create the segments to be merged
		List<Segment<K, V>> segmentList = new ArrayList<Segment<K, V>>(
				numSpills);
		for (int i = 0; i < numSpills; i++) {
			IndexRecord indexRecord = indexCacheList.get(i)
					.getIndex(parts);

			Segment<K, V> s = new Segment<K, V>(job, rfs,
					filename[i], indexRecord.startOffset,
					indexRecord.partLength, codec, true);
			segmentList.add(i, s);

			if (LOG.isDebugEnabled()) {
				LOG.debug("MapId=" + reduceId + " Reducer=" + parts
						+ "Spill =" + i + "("
						+ indexRecord.startOffset + ","
						+ indexRecord.rawLength + ", "
						+ indexRecord.partLength + ")");
			}
		}

		// merge
		@SuppressWarnings("unchecked")
		RawKeyValueIterator kvIter = Merger.merge(job, rfs,
				keyClass, valClass, codec, segmentList, job.getInt(
						"io.sort.factor", 100), new Path(reduceId
						.toString()), job.getOutputKeyComparator(),
				reporter, null, spilledRecordsCounter);

		// write merged output to disk
		long segmentStart = finalOut.getPos();
		Writer<K, V> writer = new Writer<K, V>(job, finalOut,
				keyClass, valClass, codec, spilledRecordsCounter);
		if (combinerRunner == null
				|| numSpills < minSpillsForCombine) {
			Merger.writeFile(kvIter, writer, reporter, job);
		} else {
			combineCollector.setWriter(writer);
			combinerRunner.combine(kvIter, combineCollector);
		}

		// close
		writer.close();

		// record offsets
		rec.startOffset = segmentStart;
		rec.rawLength = writer.getRawLength();
		rec.partLength = writer.getCompressedLength();
		spillRec.putIndex(rec, parts);
	}
	//spillRec.writeToFile(finalIndexFile, job);
	finalOut.close();
	for (int i = 0; i < numSpills; i++) {
		rfs.delete(filename[i], true);
	}
}
}*/
	
	/*private void mergeParts() throws IOException, InterruptedException,
			ClassNotFoundException {
		// get the approximate size of the final output/index files
		long finalOutFileSize = 0;
		long finalIndexFileSize = 0;
		final Path[] filename = new Path[numSpills];
		final TaskAttemptID reduceId = getTaskID();

		for (int i = 0; i < numSpills; i++) {
			filename[i] = mapOutputFile.getSpillFile(reduceId, i, round);
			finalOutFileSize += rfs.getFileStatus(filename[i]).getLen();
		}
		if (numSpills == 1) { // the spill is the final output
			rfs.rename(filename[0], new Path(filename[0].getParent(),
					"file.out"));
			if (indexCacheList.size() == 0) {
				rfs
						.rename(mapOutputFile.getSpillIndexFile(reduceId, 0,
								round), new Path(filename[0].getParent(),
								"file.out.index"));
			} else {
				indexCacheList.get(0)
						.writeToFile(
								new Path(filename[0].getParent(),
										"file.out.index"), job);
			}
			return;
		}

		// read in paged indices
		for (int i = indexCacheList.size(); i < numSpills; ++i) {
			Path indexFileName = reduceOutputFile.getSpillIndexFile(reduceId, i,
					round);
			indexCacheList.add(new SpillRecord(indexFileName, job));
		}

		// make correction in the length to include the sequence file header
		// lengths for each partition
		finalOutFileSize += partitions * APPROX_HEADER_LENGTH;
		finalIndexFileSize = partitions * Reduce_OUTPUT_INDEX_RECORD_LENGTH;
		Path finalOutputFile = mapOutputFile.getOutputFileForWrite(job,"fileleon");
		Path finalIndexFile = reduceOutputFile.getOutputIndexFileForWrite(
				reduceId, finalIndexFileSize, round);

		// The output stream for the final single output file
		FSDataOutputStream finalOut = rfs.create(finalOutputFile, true,
				4096);

		if (numSpills == 0) {
			// create dummy files
			IndexRecord rec = new IndexRecord();
			SpillRecord sr = new SpillRecord(partitions);
			try {
				for (int i = 0; i < partitions; i++) {
					long segmentStart = finalOut.getPos();
					Writer<K, V> writer = new Writer<K, V>(job, finalOut,
							keyClass, valClass, codec, null);
					writer.close();
					rec.startOffset = segmentStart;
					rec.rawLength = writer.getRawLength();
					rec.partLength = writer.getCompressedLength();
					sr.putIndex(rec, i);
				}
				sr.writeToFile(finalIndexFile, job);
			} finally {
				finalOut.close();
			}
			return;
		}
		{
			IndexRecord rec = new IndexRecord();
			final SpillRecord spillRec = new SpillRecord(partitions);
			for (int parts = 0; parts < partitions; parts++) {
				// create the segments to be merged
				List<Segment<K, V>> segmentList = new ArrayList<Segment<K, V>>(
						numSpills);
				for (int i = 0; i < numSpills; i++) {
					IndexRecord indexRecord = indexCacheList.get(i)
							.getIndex(parts);

					Segment<K, V> s = new Segment<K, V>(job, rfs,
							filename[i], indexRecord.startOffset,
							indexRecord.partLength, codec, true);
					segmentList.add(i, s);

					if (LOG.isDebugEnabled()) {
						LOG.debug("ReduceId=" + reduceId + " Reducer=" + parts
								+ "Spill =" + i + "("
								+ indexRecord.startOffset + ","
								+ indexRecord.rawLength + ", "
								+ indexRecord.partLength + ")");
					}
				}

				// merge
				@SuppressWarnings("unchecked")
				RawKeyValueIterator kvIter = Merger.merge(job, rfs,
						keyClass, valClass, codec, segmentList, job.getInt(
								"io.sort.factor", 100), new Path(reduceId
								.toString()), job.getOutputKeyComparator(),
						reporter, null, spilledRecordsCounter);

				// write merged output to disk
				long segmentStart = finalOut.getPos();
				Writer<K, V> writer = new Writer<K, V>(job, finalOut,
						keyClass, valClass, codec, spilledRecordsCounter);
				if (combinerRunner == null
						|| numSpills < minSpillsForCombine) {
					Merger.writeFile(kvIter, writer, reporter, job);
				} else {
					combineCollector.setWriter(writer);
					combinerRunner.combine(kvIter, combineCollector);
				}

				// close
				writer.close();

				// record offsets
				rec.startOffset = segmentStart;
				rec.rawLength = writer.getRawLength();
				rec.partLength = writer.getCompressedLength();
				spillRec.putIndex(rec, parts);
			}
			spillRec.writeToFile(finalIndexFile, job);
			finalOut.close();
			for (int i = 0; i < numSpills; i++) {
				rfs.delete(filename[i], true);
			}
		}
	}*/

 }
 
 private static class ReduceBufferTooSmallException extends IOException {
		public ReduceBufferTooSmallException(String s) {
			super(s);
		}
	}
}
