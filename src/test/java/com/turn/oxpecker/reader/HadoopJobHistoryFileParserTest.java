package com.turn.oxpecker.reader;

import java.io.File;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;
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
	public static void getJobIdsFromDirectoryTest() {

		File f = new File("/Users/jshum/turn/jtk_job_history_files");
		HadoopJobHistoryFileParser.getJobIdsFromDirectory(f);

	}

}
