package org.apache.hadoop.mapred;

import java.io.IOException;

/**
 * @author leon(liangzou0318@gmail.com)
 * @date 2013-2-27
 * @filaname leonMapper.java
 */
public interface LeonMapper<K1, V1, K2, V2> extends Mapper<K1, V1, K2, V2> {
	public void map(K1 key, V1 value,K1 key1,V1 value1, OutputCollector<K2, V2> output, Reporter reporter)
	  throws IOException;	

}
