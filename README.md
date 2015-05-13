Oxpecker is a library that can be used to read Hadoop job stats files and Hadoop job config files.

Entry points for this method

```
getHadoopJobsForDates(start, end, jobHistDir, jobTrackerName);
```

Example usage :

```
DateTimeFormatter df = DateTimeFormatter.ISO_DATE; //YYYY-MM-DD
LocalTime midnight = LocalTime.of(0, 0); //midnight
ZonedDateTime start = ZonedDateTime.of(LocalDate.parse("2015-01-01", df), midnight, ZoneId.systemDefault());
ZonedDateTime end = ZonedDateTime.of(LocalDate.parse("2015-01-02", df), midnight, ZoneId.systemDefault());
String jobTrackerName = "HADOOPCLUSTER";
String jobHistDir = "/path/to/job/history/files";
List<Collection<HadoopJob>> l = getHadoopJobsForDates(start, end, jobHistDir, jobTrackerName);
```



