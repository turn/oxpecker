/**
 * Copyright (C) 2015 Turn Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.turn.oxpecker.instrumentation;

import java.util.HashMap;

/**
 * Basic POJO that encapsulates all the information of a Hadoop Job that Samburu needs
 *
 * Encapsulate basic fields, counters and configValues
 * Fields include by default by HadoopJobHistoryFileParser:
 * 		LAUNCH_TIME, 	SUBMIT_TIME,
 * 		START_TIME, 	FINISH_TIME,
 * 		FINISHED_MAPS, 	FAILED_REDUCES,
 * 		TOTAL_MAPS, 	TOTAL_REDUCES,
 * 		FAILED_MAPS, 	FAILED_REDUCES,
 * 		JOB_QUEUE, 		JOB_PRIORITY,
 * 		JOBID, 			JOBNAME
 * 
 * @author jzhang, jshum
 *
 */
public class HadoopJob {
	
	private HashMap<String, Object> fields;
	private HashMap<String, Long> counters;
	private HashMap<String, String> configValues;
	
	public HadoopJob() {
		this.fields = new HashMap<String, Object>();
		this.counters = new HashMap<String, Long>();
		this.configValues = new HashMap<String, String>();
	}
	
	public void setField(HashMap<String, Object> fields) {
		this.fields = fields;
	}
	
	public void setCounters(HashMap<String, Long> counters) {
		this.counters = counters;
	}
	
	public HashMap<String, Object> getFields() {
		return this.fields;
	}
	
	public HashMap<String, Long> getCounters() {
		return this.counters;
	}
	
	public HashMap<String, String> getConfigValues() {
		return configValues;
	}

	public void setConfigValues(HashMap<String, String> props) {
		this.configValues = props;
	}
	
	public void addField(String name, Object value) {
		this.fields.put(name, value);
	}
	
	public void addCounter(String name, long value) {
		this.counters.put(name, value);
	}
	
	public void addConfigValue(String name, String value) {
		this.configValues.put(name, value);
	}

	public HashMap<String, Object> getAllJobLevelPropertiesHashmap() {
		HashMap<String, Object> hm = new HashMap();
		hm.putAll(this.counters);
		hm.putAll(this.fields);
		hm.putAll(this.configValues);
		return hm;
	}

}
