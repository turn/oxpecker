package com.turn.oxpecker.reader;

import com.turn.oxpecker.instrumentation.HadoopJob;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobHistory;
import org.apache.log4j.Logger;
import org.dom4j.DocumentException;
import org.threeten.bp.Duration;
import org.threeten.bp.LocalDate;
import org.threeten.bp.LocalTime;
import org.threeten.bp.ZoneId;
import org.threeten.bp.ZonedDateTime;
import org.threeten.bp.format.DateTimeFormatter;
import org.threeten.bp.temporal.ChronoUnit;

/**
 * Main class that parses job history files and returns a list of hadoop jobs
 * @author jshum
 */
public class HadoopJobHistoryFileParser {

	static Logger LOGGER = Logger.getLogger(HadoopJobHistoryFileParser.class);

	int count = 0;

	/**
	 * Given a start datetime and an end datetime, generate the list of dates inclusive of start and end datetime
	 * @param start
	 * @param end
	 * @return
	 */
	public static List<ZonedDateTime> getListOfDates(ZonedDateTime start, ZonedDateTime end) {
		ZonedDateTime startDay = start.truncatedTo(ChronoUnit.DAYS);
		ZonedDateTime endDay = end.truncatedTo(ChronoUnit.DAYS);

		Duration diff = Duration.between(startDay, endDay);
		long diffDays = diff.toDays();

		ArrayList<ZonedDateTime> listDates = new ArrayList<ZonedDateTime>((int) diffDays);
		for (int day = 0; day <= diffDays; day++){
			listDates.add(startDay.plusDays(day));
		}
		return listDates;
	}

	/**
	 * Given a list of dates and the base directory, return a list of String paths
	 * with those date directories under the base directory
	 * @param baseDir
	 * @param listDates
	 * @return
	 */
	public static List<String> getListOfDirectories(String baseDir, List<ZonedDateTime> listDates) {
		ArrayList<String> listOfDir = new ArrayList<String>(listDates.size());
		for (ZonedDateTime zdt : listDates) {
			listOfDir.add(String.format("%s/%s",baseDir, zdt.format(DateTimeFormatter.ofPattern("yyyy/MM/dd/"))));
		}
		return listOfDir;
	}

	/**
	 * Given a directory, return the list of all jobIds under this directory
	 * @param dir
	 * @return
	 */
	public static Collection<String> getJobIdsFromDirectory(File dir) {

		RegexFileFilter notConfFileFilter = new RegexFileFilter("^.*[_conf.xml]");
		Collection<File> statsFiles = FileUtils.listFiles(dir, notConfFileFilter, TrueFileFilter.INSTANCE);

		RegexFileFilter confFileFilter = new RegexFileFilter("^.*_conf.xml");
		Collection<File> confFiles = FileUtils.listFiles(dir, confFileFilter, TrueFileFilter.INSTANCE);

		LOGGER.info(String.format("statsFiles %s, confFiles %s, sameLength=%s", statsFiles.size(), confFiles.size(),
				statsFiles.size() == confFiles.size()));

		HashSet<String> confFileStrings = new HashSet<String>();
		Pattern p = Pattern.compile("^(.*)_conf.xml");

		for (File f : confFiles) {
			Matcher m = p.matcher(f.getName());
			m.find();
			String jobIdString = m.group(1);
			confFileStrings.add(jobIdString);
		}

		LOGGER.info(String.format("confFileDedup %s, confFiles %s, sameLength=%s", confFileStrings.size(), confFiles.size(),
				confFileStrings.size() == confFiles.size()));

		return confFileStrings;
	}

	/**
	 * Given a directory and a list of JobIds, return a list of HadoopJob objects corresponding to that list
	 * @param fs
	 * @param jobHistoryDir
	 * @param jobTrackerName
	 * @param jobIds
	 * @return
	 */
	public static Collection<HadoopJob> getHadoopJobFromJobIDList(FileSystem fs, File jobHistoryDir, String jobTrackerName, Collection<String> jobIds) {
		ArrayList<HadoopJob> lOfHadoopJobs = new ArrayList<HadoopJob>(jobIds.size());
		int count = 1;
		for (String jobId : jobIds) {

			if (count % 100 == 0) {
				LOGGER.info(String.format("Read %s jobs", count));
			}

			RegexFileFilter jobIdFilter = new RegexFileFilter(String.format("%s.*",jobId));
			Collection<File> jobIdFiles = FileUtils.listFiles(jobHistoryDir, jobIdFilter, TrueFileFilter.INSTANCE);

			if (jobIdFiles.size() != 2) {
				LOGGER.warn(String.format("One of conf file or statistics file is missing, skipping file : %s", jobId));
				continue;
			}

			boolean hasConfFile = false;
			File confFile = null;
			boolean hasStatsFile = false;
			File statsFile = null;
			for (File f : jobIdFiles) {
				if (f.getName().endsWith("_conf.xml")) {
					hasConfFile= true;
					confFile = f;
				} else {
					hasStatsFile = true;
					statsFile = f;
				}
			}
			if (!hasConfFile && !hasStatsFile) {
				LOGGER.warn(String.format("One of conf file or statistics file is missing, skipping file : %s", jobId));
				continue;
			}
			TaskAttemptFilter nf = new TaskAttemptFilter();
			try {
				getHadoopJobFromStatsFile(fs, statsFile.getAbsolutePath(), nf);
			} catch (IOException e) {
				LOGGER.error("Skipping file",e);
				continue;
			}
			HadoopJob hj = new HadoopJob();

			try {
				populateFieldsFromJobInfo(nf,hj);
				hj.addField(Constant.JOB_TRACKER, jobTrackerName);
				populateCountersFromJobInfo(nf, hj);
				populateConfigsFromFile(hj, confFile);
			} catch (FileNotFoundException e) {
				LOGGER.error("Skipping file", e);
				continue;
			} catch (DocumentException e) {
				LOGGER.error("Skipping file", e);
				continue;
			} catch (HadoopJobParseException e) {
				LOGGER.error("Skipping file", e);
				continue;
			} catch (ParseException e) {
				LOGGER.error("Skipping file", e);
				continue;
			}
			lOfHadoopJobs.add(hj);
			count++;
		}
		LOGGER.info(String.format("Read in total %s jobs", count));
		return lOfHadoopJobs;
	}

	/**
	 * Parse config files and add all config values to HadoopJob objects
	 * @param hj
	 * @param confFile
	 * @throws FileNotFoundException
	 * @throws DocumentException
	 * @throws HadoopJobParseException
	 */
	public static void populateConfigsFromFile(HadoopJob hj, File confFile) throws FileNotFoundException, DocumentException, HadoopJobParseException {
		HadoopJobConfigFileParser configParser = new HadoopJobConfigFileParser(confFile.getAbsolutePath());
		configParser.addJobConfToHadoopJob(hj);
	}

	/**
	 * Given a hadoopjob and Hadoop object, add all the whitelisted fields into the HadoopJob object
	 * @param nf
	 * @param hadoopJob
	 */
	public static void populateFieldsFromJobInfo(TaskAttemptFilter nf, HadoopJob hadoopJob) {
		Map<JobHistory.Keys, String> maps = nf.getValues();
		JobHistory.Keys[] keys = {
			JobHistory.Keys.LAUNCH_TIME, JobHistory.Keys.SUBMIT_TIME,
			JobHistory.Keys.START_TIME, JobHistory.Keys.FINISH_TIME,
			JobHistory.Keys.FINISHED_MAPS, JobHistory.Keys.FAILED_REDUCES,
			JobHistory.Keys.TOTAL_MAPS, JobHistory.Keys.TOTAL_REDUCES,
			JobHistory.Keys.FAILED_MAPS, JobHistory.Keys.FAILED_REDUCES,

			JobHistory.Keys.JOB_QUEUE, JobHistory.Keys.JOB_PRIORITY,
			JobHistory.Keys.JOBID, JobHistory.Keys.JOBNAME
		};
		for (JobHistory.Keys key : keys) {
			hadoopJob.addField(key.name(), maps.get(key));
		}
	}

	/**
	 * Given a Hadoop object and hadoopJob object, add all the counters to the hadoopJob object
	 * @param jobInfo
	 * @param hadoopJob
	 * @throws ParseException
	 */
	public static void populateCountersFromJobInfo(TaskAttemptFilter jobInfo, HadoopJob hadoopJob) throws ParseException {
		Map<JobHistory.Keys, String> maps = jobInfo.getValues();

		Counters totalCounters =
				Counters.fromEscapedCompactString(maps.get(JobHistory.Keys.COUNTERS));
		for ( Counters.Group group : totalCounters) {
			Iterator<Counters.Counter> ci = group.iterator();
			while(ci.hasNext()) {
				Counters.Counter counter = ci.next();
				hadoopJob.addCounter(counter.getName(), counter.getValue());
			}
		}
	}

	/**
	 * Given a jobtrackerName and a dateDirectory, return a Collection of hadoopJobs
	 * @param dateDirectory
	 * @param jobtrackerName
	 * @return
	 * @throws URISyntaxException
	 * @throws IOException
	 */
	public static Collection<HadoopJob> getHadoopJobsFromDirectory(String dateDirectory, String jobtrackerName) throws URISyntaxException, IOException {
		LOGGER.info(String.format("Getting hadoop jobs from %s", dateDirectory));
		File jobHistDirFile = new File(dateDirectory);
		Collection<String> jobIds = getJobIdsFromDirectory(jobHistDirFile);

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get( new URI(dateDirectory), conf);
		LocalFileSystem localFileSystem = fs.getLocal(conf);

		Collection<HadoopJob> hadoopJobs = getHadoopJobFromJobIDList(localFileSystem, jobHistDirFile, jobtrackerName, jobIds);
		LOGGER.info(String.format("Size from %s : %s", dateDirectory, hadoopJobs.size()));
		return hadoopJobs;
	}

	/**
	 * Given a start and end date, and a jobtracker name and the base directory
	 * where all the jobtracker files will be put under, return a list of list of hadoop jobs
	 * @param start
	 * @param end
	 * @param baseJobHistDir
	 * @param jobTrackerName
	 * @return
	 * @throws IOException
	 * @throws URISyntaxException
	 */
	public static List<Collection<HadoopJob>> getHadoopJobsForDates(ZonedDateTime start, ZonedDateTime end, String baseJobHistDir, String jobTrackerName) throws IOException, URISyntaxException {
		List<ZonedDateTime> listOfDates = getListOfDates(start, end);
		List<String> dateDirectories = getListOfDirectories(baseJobHistDir, listOfDates);
		List<Collection<HadoopJob>> allHadoopJobs = new LinkedList<Collection<HadoopJob>>();
		for (String dateDirectory : dateDirectories) {
			if (new File(dateDirectory).exists()) {
				LOGGER.info(String.format("Reading date directory %s", dateDirectory));
				allHadoopJobs.add(getHadoopJobsFromDirectory(dateDirectory, jobTrackerName));
			}
		}
		LOGGER.info(String.format("Read %s dir(s) in total", allHadoopJobs.size()));
		return allHadoopJobs;
	}

	/**
	 * Using Hadoop API to parse a job stats file and populating the TaskAttemptFilter object
	 * @param localFileSystem
	 * @param statsFile
	 * @param l
	 * @throws IOException
	 */
	public static void getHadoopJobFromStatsFile(FileSystem localFileSystem,  String statsFile, TaskAttemptFilter l) throws IOException {
		JobHistory.parseHistoryFromFS(statsFile, l, localFileSystem);
	}

	/**
	 * A static class that ignores the mapAttempt and reduceAttempt since those fields are unnecessary
	 */
	static class TaskAttemptFilter implements JobHistory.Listener {
		private Map<JobHistory.Keys, String> maps =
				new HashMap<JobHistory.Keys, String>();

		Map<JobHistory.Keys, String> getValues(){
			return maps;
		}

		public void handle(JobHistory.RecordTypes recType, Map<JobHistory.Keys, String> values)
				throws IOException {
			if (!recType.equals(JobHistory.RecordTypes.MapAttempt) &&
					!recType.equals(JobHistory.RecordTypes.ReduceAttempt)) {
				for (Map.Entry<JobHistory.Keys, String> e : values.entrySet()) {
					maps.put(e.getKey(), e.getValue());
				}
			}
		}
	}

	public static void main(String[] args) throws IOException, URISyntaxException {
		if (args.length != 4) {
			System.out.println("usage : HadoopJobHistoryFileParser " +
					"<start date YYYY-MM-DD> <end date YYYY-MM-DD> " +
					"<jobtrackerName> <path to top level directory containing files>");
			return;
		}

		DateTimeFormatter df = DateTimeFormatter.ISO_DATE;
		LocalTime midnight = LocalTime.of(0, 0); //midnight

		ZonedDateTime start = ZonedDateTime.of(LocalDate.parse(args[0], df), midnight, ZoneId.systemDefault());
		ZonedDateTime end = ZonedDateTime.of(LocalDate.parse(args[1], df), midnight, ZoneId.systemDefault());

		String jobTrackerName = args[2];
		String jobHistDir = args[3];

		LOGGER.info(String.format("Parsed %s %s %s %s", start, end, jobHistDir, jobTrackerName));

//		String jobHistDir = "/Users/jshum/turn/jtk_job_history_files";
//		String jobTrackerName = "ATL2";

		List<Collection<HadoopJob>> l = getHadoopJobsForDates(start, end, jobHistDir, jobTrackerName);
	}
}
