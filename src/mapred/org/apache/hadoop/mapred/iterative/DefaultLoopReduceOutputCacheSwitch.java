package org.apache.hadoop.mapred.iterative;

import org.apache.hadoop.mapred.JobConf;

public class DefaultLoopReduceOutputCacheSwitch implements
		LoopReduceOutputCacheSwitch {

	@Override
	public boolean isCacheRead(JobConf conf, int iteration, int step) {
		return false;
	}

	@Override
	public boolean isCacheWritten(JobConf conf, int iteration, int step) {
		return false;
	}

}
