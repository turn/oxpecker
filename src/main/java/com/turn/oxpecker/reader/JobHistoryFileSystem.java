/**
 * Copyright (C) 2015 Turn, Inc.  All Rights Reserved.
 * Proprietary and confidential.
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

	public Collection<File> getFilesForGivenJobId(File jobHistoryDir, String jobId) {
		RegexFileFilter jobIdFilter = new RegexFileFilter(String.format("%s.*",jobId));
		Collection<File> jobIdFiles = FileUtils.listFiles(jobHistoryDir, jobIdFilter, TrueFileFilter.INSTANCE);
		return jobIdFiles;
	}

	public Collection<File> getConfFileInDirectory(File dir) {
		RegexFileFilter confFileFilter = new RegexFileFilter("^.*_conf.xml");
		Collection<File> confFiles = FileUtils.listFiles(dir, confFileFilter, TrueFileFilter.INSTANCE);
		return confFiles;
	}

	public Collection<File> getStatsFileInDirectory(File dir) {
		RegexFileFilter notConfFileFilter = new RegexFileFilter("^.*[_conf.xml]");
		Collection<File> statsFiles = FileUtils.listFiles(dir, notConfFileFilter, TrueFileFilter.INSTANCE);
		return statsFiles;
	}

	public Collection<File> getAllSubDirectories(File baseDir) {
		Collection<File> dirs =  FileUtils.listFilesAndDirs(baseDir, DirectoryFileFilter.INSTANCE, TrueFileFilter.INSTANCE);
		return dirs;
	}



}
