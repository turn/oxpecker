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
package com.turn.oxpecker.reader;

import java.io.File;
import java.util.Collection;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.apache.commons.io.filefilter.TrueFileFilter;

/**
 * @author jshum
 */
public class JobHistoryFileSystem {

	/**
	 * Returns all the files that start with the input jobId string
	 * @param jobHistoryDir
	 * @param jobId
	 * @return
	 */
	public Collection<File> getFilesForGivenJobId(File jobHistoryDir, String jobId) {
		RegexFileFilter jobIdFilter = new RegexFileFilter(String.format("%s.*",jobId));
		Collection<File> jobIdFiles = FileUtils.listFiles(jobHistoryDir, jobIdFilter, TrueFileFilter.INSTANCE);
		return jobIdFiles;
	}

	/**
	 * Returns all the files that end with _conf.xml in its filename
	 * @param dir
	 * @return
	 */
	public Collection<File> getConfFileInDirectory(File dir) {
		RegexFileFilter confFileFilter = new RegexFileFilter("^.*_conf.xml");
		Collection<File> confFiles = FileUtils.listFiles(dir, confFileFilter, TrueFileFilter.INSTANCE);
		return confFiles;
	}

	/**
	 * Returns all the files that do not contain _conf.xml in its filename, which we presume to be statistics file
	 * @param dir
	 * @return
	 */
	public Collection<File> getStatsFileInDirectory(File dir) {
		RegexFileFilter notConfFileFilter = new RegexFileFilter("^.*[_conf.xml]");
		Collection<File> statsFiles = FileUtils.listFiles(dir, notConfFileFilter, TrueFileFilter.INSTANCE);
		return statsFiles;
	}

	/**
	 * Returns all subdirectories of a given base directory
	 * @param baseDir
	 * @return
	 */
	public Collection<File> getAllSubDirectories(File baseDir) {
		Collection<File> dirs =  FileUtils.listFilesAndDirs(baseDir, DirectoryFileFilter.INSTANCE, TrueFileFilter.INSTANCE);
		return dirs;
	}



}
