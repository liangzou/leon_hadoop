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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;

/**
 * Manipulate the working area for the transient store for maps and reduces.
 */
class ReduceOutputFile_leon {

	private JobConf conf;
	private JobID jobId;

	public final static int ADD = 1;
	public final static int DELETE = 2;
	public final static int NORMAL = 0;

	ReduceOutputFile_leon() {
	}

	ReduceOutputFile_leon(JobID jobId) {
		this.jobId = jobId;
	}

	private LocalDirAllocator lDirAlloc = new LocalDirAllocator(
			"mapred.local.dir");

	/**
	 * Return the path to local map output file created earlier
	 * 
	 * @param mapTaskId
	 *            a map task id
	 */
	public Path getOutputFile(JobConf job,String name)
			throws IOException {

		/*return lDirAlloc.getLocalPathToRead(TaskTracker
				.getIntermediateOutputDir(jobId.toString(), reduceTaskId
						.toString())
				+ "/i" + pass + "/file.out", conf);*/
		Path file = FileOutputFormat.getTaskOutputPath(job, name);
		return file;

	}

	/**
	 * Create a local map output file name.
	 * 
	 * @param mapTaskId
	 *            a map task id
	 * @param size
	 *            the size of the file
	 */
	public Path getOutputFileForWrite(JobConf job,String name) throws IOException {

		/*return lDirAlloc.getLocalPathForWrite(TaskTracker
				.getIntermediateOutputDir(jobId.toString(), reduceTaskId
						.toString())
				+ "/i" + pass + "/file.out", size, conf);*/
		Path file = FileOutputFormat.getTaskOutputPath(job, name);
		return file;
	}

	/**
	 * Return the path to a local map output index file created earlier
	 * 
	 * @param mapTaskId
	 *            a map task id
	 */
	public Path getOutputIndexFile(TaskAttemptID reduceTaskId, int currentIteration)
			throws IOException {

		return lDirAlloc.getLocalPathToRead(TaskTracker
				.getIntermediateOutputDir(jobId.toString(), reduceTaskId
						.toString())
				+ "/i" + currentIteration + "/file.out.index", conf);
	}

	/**
	 * Create a local map output index file name.
	 * 
	 * @param mapTaskId
	 *            a map task id
	 * @param size
	 *            the size of the file
	 */
	public Path getOutputIndexFileForWrite(TaskAttemptID reduceTaskId, long size,
			int pass) throws IOException {

		return lDirAlloc.getLocalPathForWrite(TaskTracker
				.getIntermediateOutputDir(jobId.toString(), reduceTaskId
						.toString())
				+ "/i" + pass + "/file.out.index", size, conf);
	}

	/**
	 * Return a local map spill file created earlier.
	 * 
	 * @param mapTaskId
	 *            a map task id
	 * @param spillNumber
	 *            the number
	 */
	public Path getSpillFile( TaskAttemptID reduceTaskId, int pass,int spillNumber)
			throws IOException {

		return lDirAlloc.getLocalPathToRead(TaskTracker
				.getIntermediateOutputDir(jobId.toString(), reduceTaskId
						.toString())
				+ "/i" + pass + "/spill" + spillNumber + ".out", conf);
		/*Path file = FileOutputFormat.getTaskOutputPath(job, name);
		return file;*/
	}

	/**
	 * Create a local map spill file name.
	 * 
	 * @param mapTaskId
	 *            a map task id
	 * @param spillNumber
	 *            the number
	 * @param size
	 *            the size of the file
	 */
	public Path getSpillFileForWrite(JobConf job,String name) throws IOException {

		/*return lDirAlloc.getLocalPathForWrite(TaskTracker
				.getIntermediateOutputDir(jobId.toString(), reduceTaskId
						.toString())
				+ "/i" + pass + "/spill" + spillNumber + ".out", size, conf);*/
		Path file = FileOutputFormat.getTaskOutputPath(job, name);
		return file;

	}

	/**
	 * Return a local map spill index file created earlier
	 * 
	 * @param mapTaskId
	 *            a map task id
	 * @param spillNumber
	 *            the number
	 */
	public Path getSpillIndexFile(TaskAttemptID reduceTaskId, int spillNumber, int pass) throws IOException {
		
		return lDirAlloc.getLocalPathToRead(TaskTracker
				.getIntermediateOutputDir(jobId.toString(), reduceTaskId
						.toString())
				+ "/i" + pass + "/index" + reduceTaskId + ".out", conf);
		/*Path file = FileOutputFormat.getTaskOutputPath(job, name);
		String files=file + "/i" + pass + "/spill" + spillNumber + ".out.index";
		return files;*/
		/*Path file = FileOutputFormat.getTaskOutputPath(job, name);
		return file;*/
	}

	/**
	 * Create a local map spill index file name.
	 * 
	 * @param mapTaskId
	 *            a map task id
	 * @param spillNumber
	 *            the number
	 * @param size
	 *            the size of the file
	 */
	public Path getSpillIndexFileForWrite(JobConf job,String name) throws IOException {

		/*return lDirAlloc.getLocalPathForWrite(TaskTracker
				.getIntermediateOutputDir(jobId.toString(), reduceTaskId
						.toString())
				+ "/i" + pass + "/spill" + spillNumber + ".out.index", size,
				conf);*/
		Path file = FileOutputFormat.getTaskOutputPath(job, name);
		return file;
	}

	/**
	 * Return a local reduce input file created earlier
	 * 
	 * @param mapTaskId
	 *            a map task id
	 * @param reduceTaskId
	 *            a reduce task id
	 */
	public Path getInputFile(int reduceId, TaskAttemptID reduceTaskId, int pass)
			throws IOException {
		return lDirAlloc.getLocalPathToRead(TaskTracker
				.getIntermediateOutputDir(jobId.toString(), reduceTaskId
						.toString())
				+ "/i" + pass + "/map_" + reduceId + ".out", conf);
	}

	/**
	 * Create a local reduce input file name.
	 * 
	 * @param mapTaskId
	 *            a map task id
	 * @param reduceTaskId
	 *            a reduce task id
	 * @param size
	 *            the size of the file
	 */
	/*public Path getInputFileForWrite(JobConf job,String name) throws IOException {
		return lDirAlloc.getLocalPathForWrite(TaskTracker
				.getIntermediateOutputDir(jobId.toString(), reduceTaskId
						.toString())
				+ "/i" + pass + "/reduce_" + reduceId.getId() + ".out", size, conf);
		Path file = FileOutputFormat.getTaskOutputPath(job, name);
		return file;
	}*/

	/**
	 * Create a local reduce cache file name
	 */
	public Path getReduceCacheFileForWrite(TaskAttemptID reduceTaskId,
			long size, int pass) throws IOException {
		return lDirAlloc.getLocalPathForWrite(TaskTracker
				.getIntermediateOutputDir(jobId.toString(), reduceTaskId
						.toString())
				+ "c/i" + pass + "/cacheFile.out", size, conf);
	}

	/**
	 * Create a local reduce output cache file name
	 */
	public Path getReduceOutputCacheFileForWrite(TaskAttemptID reduceTaskId,
			long size, int pass) throws IOException {
		return lDirAlloc.getLocalPathForWrite(TaskTracker
				.getIntermediateOutputDir(jobId.toString(), reduceTaskId
						.toString())
				+ "co/i" + pass + "/cacheFile.out", size, conf);
	}

	/**
	 * reduce a local reduce index cache file
	 * 
	 * @param reduceTaskId
	 * @param size
	 * @param pass
	 * @return
	 * @throws IOException
	 */
	public Path getOutputCacheIndexFileForWrite(TaskAttemptID reduceTaskId,
			long size, int pass) throws IOException {
		return lDirAlloc.getLocalPathForWrite(TaskTracker
				.getIntermediateOutputDir(jobId.toString(), reduceTaskId
						.toString())
				+ "co/i" + pass + "/cacheFile.index.out", size, conf);
	}

	/**
	 * Create a local map cache file name
	 */
	public Path getCacheFileForWrite(TaskAttemptID reduceTaskId, long size,
			int pass) throws IOException {
		return lDirAlloc.getLocalPathForWrite(TaskTracker
				.getIntermediateOutputDir(jobId.toString(), reduceTaskId
						.toString())
				+ "mc/i" + pass + "/cacheFile.out", size, conf);
	}

	/**
	 * reduce a local reduce index cache file
	 * 
	 * @param reduceTaskId
	 * @param size
	 * @param pass
	 * @return
	 * @throws IOException
	 */
	public Path getCacheIndexFileForWrite(TaskAttemptID reduceTaskId,
			long size, int pass) throws IOException {
		return lDirAlloc.getLocalPathForWrite(TaskTracker
				.getIntermediateOutputDir(jobId.toString(), reduceTaskId
						.toString())
				+ "c/i" + pass + "/cacheFile.index.out", size, conf);
	}

	/** Removes all of the files related to a task. */
	public void removeAll(TaskAttemptID taskId, int currentIteration)
			throws IOException {
		conf.deleteLocalFiles(TaskTracker.getIntermediateOutputDir(jobId
				.toString(), taskId.toString())
				+ "/i" + currentIteration);
	}

	/** Removes all of the files related to a task. */
	public void removeAll(TaskAttemptID taskId) throws IOException {
		conf.deleteLocalFiles(TaskTracker.getIntermediateOutputDir(jobId
				.toString(), taskId.toString()));
	}

	public void setConf(Configuration conf) {
		if (conf instanceof JobConf) {
			this.conf = (JobConf) conf;
		} else {
			this.conf = new JobConf(conf);
		}
	}

	public void setJobId(JobID jobId) {
		this.jobId = jobId;
	}

}
