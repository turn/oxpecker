package com.turn.oxpecker.reader;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.threeten.bp.ZoneId;
import org.threeten.bp.ZonedDateTime;
import org.threeten.bp.temporal.ChronoUnit;

/**
 * @author jshum
 */
public class HadoopJobHistoryFileParserTest {

	@Test
	public static void getListOfDatesTest() {
		ZonedDateTime start = ZonedDateTime.now();
		ZonedDateTime end = ZonedDateTime.from(start);
		List<ZonedDateTime> lOfDates =  HadoopJobHistoryFileParser.getListOfDates(start, end);
		Assert.assertEquals(lOfDates.size(), 1);
		Assert.assertEquals(lOfDates.get(0), start.truncatedTo(ChronoUnit.DAYS));

		start = ZonedDateTime.now();
		end = start.plusDays(1);
		lOfDates =  HadoopJobHistoryFileParser.getListOfDates(start, end);
		Assert.assertEquals(lOfDates.size(), 2);
		Assert.assertEquals(lOfDates.get(0), start.truncatedTo(ChronoUnit.DAYS));
		Assert.assertEquals(lOfDates.get(1), start.truncatedTo(ChronoUnit.DAYS).plusDays(1));

	}

	@Test
	public static void testGetListOfDirectories() {

		JobHistoryFileSystem jhfs = Mockito.mock(JobHistoryFileSystem.class);
		HadoopJobHistoryFileParser.jobHistoryFileSystem = jhfs;

		Collection<File> dirs = new LinkedList<File>();
		String[] dirPaths = {
				"/Users/jshum/turn/jtk_job_history_files",
				"/Users/jshum/turn/jtk_job_history_files/jt001.sjc2.turn.com_1430284147038_",
				"/Users/jshum/turn/jtk_job_history_files/jt001.sjc2.turn.com_1430303690462_",
				"/Users/jshum/turn/jtk_job_history_files/jt001.sjc2.turn.com_1430307426356_",
				"/Users/jshum/turn/jtk_job_history_files/jt001.sjc2.turn.com_1430419668476_",
				"/Users/jshum/turn/jtk_job_history_files/jt001.sjc2.turn.com_1430963659046_",
				"/Users/jshum/turn/jtk_job_history_files/jt001.sjc2.turn.com_1431571050810_",
				"/Users/jshum/turn/jtk_job_history_files/jt001.sjc2.turn.com_1431571050810_/2015",
				"/Users/jshum/turn/jtk_job_history_files/jt001.sjc2.turn.com_1431571050810_/2015/06",
				"/Users/jshum/turn/jtk_job_history_files/jt001.sjc2.turn.com_1431571050810_/2015/06/01",
				"/Users/jshum/turn/jtk_job_history_files/jt001.sjc2.turn.com_1431571050810_/2015/06/01/00040",
				"/Users/jshum/turn/jtk_job_history_files/jt001.sjc2.turn.com_1432171645801_",
				"/Users/jshum/turn/jtk_job_history_files/jt001.sjc2.turn.com_1432171645801_/2015",
				"/Users/jshum/turn/jtk_job_history_files/jt001.sjc2.turn.com_1432171645801_/2015/06",
				"/Users/jshum/turn/jtk_job_history_files/jt001.sjc2.turn.com_1432171645801_/2015/06/01",
				"/Users/jshum/turn/jtk_job_history_files/jt001.sjc2.turn.com_1431571050810_/2015/06/01/00039",
		};
		for (String s : dirPaths) {
			dirs.add(new File(s));
		}
		Mockito.doReturn(dirs).when(jhfs).getAllSubDirectories(new File("/Users/jshum/turn/jtk_job_history_files/"));

		ZonedDateTime june1 = ZonedDateTime.of(2015,6,1,0,0,0,0, ZoneId.of("America/Los_Angeles"));
		ZonedDateTime[] dates = {june1};
		List<File> subDirsOfDate = HadoopJobHistoryFileParser.getListOfDirectories("/Users/jshum/turn/jtk_job_history_files/", Arrays.asList(dates));
		Assert.assertEquals(subDirsOfDate.size(),2);
		Collections.sort(subDirsOfDate);
		Assert.assertEquals(subDirsOfDate.get(0).getAbsolutePath(),"/Users/jshum/turn/jtk_job_history_files/jt001.sjc2.turn.com_1431571050810_/2015/06/01");
		Assert.assertEquals(subDirsOfDate.get(1).getAbsolutePath(),"/Users/jshum/turn/jtk_job_history_files/jt001.sjc2.turn.com_1432171645801_/2015/06/01");
	}


}
