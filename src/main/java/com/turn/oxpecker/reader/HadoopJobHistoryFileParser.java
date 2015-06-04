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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobHistory;
import org.apache.log4j.Logger;
import org.dom4j.DocumentException;
import org.threeten.bp.Duration;
import org.threeten.bp.LocalDate;
import org.threeten.bp.LocalDateTime;
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

	static String dateDirectoryRegex = "/yyyy/MM/dd$";
	static Logger LOGGER = Logger.getLogger(HadoopJobHistoryFileParser.class);
	public static JobHistoryFileSystem jobHistoryFileSystem = new JobHistoryFileSystem();

	public static ArrayList<HadoopJob> getListOfHadoopJobsForDates(ZonedDateTime start, ZonedDateTime end, String jobHistDir, String jobTrackerName) throws IOException, URISyntaxException {
		int count = 0;
		List<Collection<HadoopJob>> hadoopJobs = HadoopJobHistoryFileParser.getHadoopJobsForDates(start, end, jobHistDir, jobTrackerName);
		ArrayList<HadoopJob> allJobs = new ArrayList<HadoopJob>();

		//merge all the lists together
		for (Collection<HadoopJob> col : hadoopJobs) {
			count+= col.size();
			LOGGER.info(String.format("size %s", count));
		}
		for (Collection<HadoopJob> col : hadoopJobs) {
			allJobs.addAll(col);
		}
		return allJobs;
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
		List<File> dateDirectories = getListOfDirectories(baseJobHistDir, listOfDates);
		List<Collection<HadoopJob>> allHadoopJobs = new LinkedList<Collection<HadoopJob>>();
		for (File dateDirectory : dateDirectories) {
			LOGGER.info(String.format("Reading date directory %s", dateDirectory));
			allHadoopJobs.add(getHadoopJobsFromDirectory(dateDirectory, jobTrackerName));
		}
		LOGGER.info(String.format("Read %s dir(s) in total", allHadoopJobs.size()));
		return allHadoopJobs;
	}

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
	public static List<File> getListOfDirectories(String baseDir, List<ZonedDateTime> listDates) {
		ArrayList<File> listOfDir = new ArrayList<File>(listDates.size());

		Collection<File> dirs = jobHistoryFileSystem.getAllSubDirectories(new File(baseDir));

		for (File dir : dirs) {
			LOGGER.debug(dir.getAbsolutePath());
			for (ZonedDateTime zdt : listDates) {
				Pattern p = Pattern.compile(zdt.format(DateTimeFormatter.ofPattern(dateDirectoryRegex)));
				Matcher m = p.matcher(dir.getAbsolutePath());
				boolean found = m.find();
				if (found) {
					listOfDir.add(dir);
				}
			}
		}
		return listOfDir;
	}

	/**
	 * Given a jobtrackerName and a dateDirectory, return a Collection of hadoopJobs
	 * @param dateDirectory
	 * @param jobtrackerName
	 * @return
	 * @throws URISyntaxException
	 * @throws IOException
	 */
	public static Collection<HadoopJob> getHadoopJobsFromDirectory(File dateDirectory, String jobtrackerName) throws URISyntaxException, IOException {
		LOGGER.info(String.format("Getting hadoop jobs from %s", dateDirectory));
		Collection<String> jobIds = getJobIdsFromDirectory(dateDirectory);

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get( new URI(dateDirectory.getAbsolutePath()), conf);
		LocalFileSystem localFileSystem = fs.getLocal(conf);

		Collection<HadoopJob> hadoopJobs = getHadoopJobsGivenJobIDList(localFileSystem, dateDirectory, jobtrackerName, jobIds);
		LOGGER.info(String.format("Size from %s : %s", dateDirectory, hadoopJobs.size()));
		return hadoopJobs;
	}

	/**
	 * Given a directory, return the list of all jobIds under this directory
	 * @param dir
	 * @return
	 */
	public static HashSet<String> getJobIdsFromDirectory(File dir) {

		Collection<File> statsFiles = jobHistoryFileSystem.getStatsFileInDirectory(dir);

		Collection<File> confFiles =  jobHistoryFileSystem.getConfFileInDirectory(dir);

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

	public static String getJobIdFromConfFileName(String confFileString) {
		Pattern p = Pattern.compile("^(.*)_conf.xml");
		Matcher m = p.matcher(confFileString);
		m.find();
		String jobIdString = m.group(1);
		return jobIdString;
	}

	public static String getJobIdFromStatsFileName(String stasFileName) {
		int index = StringUtils.ordinalIndexOf(stasFileName, "_", 3);
		return stasFileName.substring(0,index);
	}

	/**
	 * Given a directory and a list of JobIds, return a list of HadoopJob objects corresponding to that list
	 * @param fs
	 * @param jobHistoryDir
	 * @param jobTrackerName
	 * @param jobIds
	 * @return
	 */
	public static Collection<HadoopJob> getHadoopJobsGivenJobIDList(FileSystem fs, File jobHistoryDir, String jobTrackerName, Collection<String> jobIds) {
		ArrayList<HadoopJob> lOfHadoopJobs = new ArrayList<HadoopJob>(jobIds.size());
		int count = 1;
		for (String jobId : jobIds) {

			if (count % 100 == 0) {
				LOGGER.info(String.format("Read %s jobs", count));
			}
			HadoopJob hj = getHadoopJobGivenJobID(fs, jobHistoryDir, jobTrackerName, jobId);
			if (hj == null) {
				LOGGER.error(String.format("Skipping job %s",jobId));
				continue;
			}
			lOfHadoopJobs.add(hj);
			count++;
		}
		LOGGER.info(String.format("Read in total %s jobs", count));
		return lOfHadoopJobs;
	}

	/**
	 * Given a directory and a JobId, return a HadoopJob object corresponding to that jobId
	 * with counters, config, fields populated
	 * @param fs
	 * @param jobHistoryDir
	 * @param jobTrackerName
	 * @param jobId
	 * @return
	 */
	public static HadoopJob getHadoopJobGivenJobID(FileSystem fs, File jobHistoryDir, String jobTrackerName, String jobId) {
		Collection<File> jobIdFiles = jobHistoryFileSystem.getFilesForGivenJobId(jobHistoryDir, jobId);

		if (jobIdFiles.size() != 2) {
			LOGGER.warn(String.format("One of conf file or statistics file is missing, skipping file : %s", jobId));
			return null;
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
			return null;
		}
		TaskAttemptFilter nf = new TaskAttemptFilter();
		try {
			getHadoopJobFromStatsFile(fs, statsFile.getAbsolutePath(), nf);
		} catch (IOException e) {
			LOGGER.error("Error",e);
			return null;
		}
		HadoopJob hj = new HadoopJob();

		try {
			populateFieldsFromJobInfo(nf,hj);
			hj.addField(Constant.JOB_TRACKER, jobTrackerName);
			populateCountersFromJobInfo(nf, hj);
			populateConfigsFromFile(hj, confFile);
		} catch (FileNotFoundException e) {
			LOGGER.error("Error", e);
			return null;
		} catch (DocumentException e) {
			LOGGER.error("Error", e);
			return null;
		} catch (HadoopJobParseException e) {
			LOGGER.error("Error", e);
			return null;
		} catch (ParseException e) {
			LOGGER.error("Error", e);
			return null;
		}
		return hj;
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
			JobHistory.Keys.JOBID, JobHistory.Keys.JOBNAME,
			JobHistory.Keys.LAUNCH_TIME, JobHistory.Keys.SUBMIT_TIME,
			JobHistory.Keys.START_TIME, JobHistory.Keys.FINISH_TIME,
			JobHistory.Keys.FINISHED_MAPS, JobHistory.Keys.FINISHED_REDUCES,
			JobHistory.Keys.TOTAL_MAPS, JobHistory.Keys.TOTAL_REDUCES,
			JobHistory.Keys.FAILED_MAPS, JobHistory.Keys.FAILED_REDUCES,

			JobHistory.Keys.JOB_QUEUE, JobHistory.Keys.JOB_PRIORITY,
			JobHistory.Keys.JOB_STATUS,

		};
		for (JobHistory.Keys key : keys) {
			if (maps.containsKey(key)){
				hadoopJob.addField(key.name(), maps.get(key));
			}
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

	public static List<HashSet<String>> getHadoopJobIdsForDates(ZonedDateTime start, ZonedDateTime end, String baseJobHistDir) {
		List<ZonedDateTime> listOfDates = getListOfDates(start, end);
		List<File> dateDirectories = getListOfDirectories(baseJobHistDir, listOfDates);
		List<HashSet<String>> allJobIds = new LinkedList<HashSet<String>>();
		for (File dateDirectory : dateDirectories) {
			LOGGER.info(String.format("Reading date directory %s", dateDirectory));
			allJobIds.add(getJobIdsFromDirectory(dateDirectory));
		}
		LOGGER.info(String.format("Read %s dir(s) in total", allJobIds.size()));
		return allJobIds;
	}

	/**
	 * Given a jobtrackerName and a dateDirectory, return a Collection of hadoopJobs
	 * @param dateDirectory
	 * @param jobtrackerName
	 * @return
	 * @throws URISyntaxException
	 * @throws IOException
	 */
	public static HadoopJob getHadoopJobFromDirectoryGivenJobID(File dateDirectory, String jobtrackerName, String jobId) throws URISyntaxException, IOException {

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get( new URI(dateDirectory.getAbsolutePath()), conf);
		LocalFileSystem localFileSystem = fs.getLocal(conf);

		HadoopJob hadoopJob = getHadoopJobGivenJobID(localFileSystem, dateDirectory, jobtrackerName, jobId);
		return hadoopJob;
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
