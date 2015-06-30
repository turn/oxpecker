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

import com.turn.oxpecker.instrumentation.HadoopJob;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.threeten.bp.LocalDate;
import org.threeten.bp.LocalTime;
import org.threeten.bp.ZoneId;
import org.threeten.bp.ZonedDateTime;
import org.threeten.bp.format.DateTimeFormatter;

/**
 * @author jshum
 */
public class Starter {

	static Logger LOGGER = Logger.getLogger(Starter.class);
	public static void main(String[] args) throws IOException, URISyntaxException {

		if (args.length < 0) {
			System.out.println("usage : HadoopJobHistoryFileParser " +
					"<mode: jobid/date>");
			return;
		}

		if (args[0].equals("jobid")) {
			if (args.length != 3) {
				System.out.println("usage : HadoopJobHistoryFileParser " +
						"<mode: jobid> " +
						"<jobid> " +
						"<path to top level directory containing files>");
				return;
			}

			HadoopJob hj = HadoopJobHistoryFileParser.getHadoopJobFromDirectoryGivenJobID(
					new File(args[2]), "local", args[1]);
			HashMap<String, Object> fields = hj.getFields();
			for (Map.Entry<String, Object> s : fields.entrySet()) {
				System.out.println(String.format("%s : %s",s.getKey(),s.getValue()));
			}
		}

		if (args[0].equals("date")) {
			if (args.length != 5) {
				System.out.println("usage : HadoopJobHistoryFileParser " +
						"<mode: date>" +
						"<start date YYYY-MM-DD> <end date YYYY-MM-DD> "+
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

			List<Collection<HadoopJob>> l = HadoopJobHistoryFileParser.getHadoopJobsForDates(start, end, jobHistDir, jobTrackerName);

			return;
		}



	}
}
