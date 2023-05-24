# Problem statement

To create 2 APIs

1. /trigger_report endpoint that will trigger report generation from the data provided (stored in DB)
   1. No input
   2. Output - report_id (random string)
   3. report_id will be used for polling the status of report completion
2. /get_report endpoint that will return the status of the report or the csv
   1. Input - report_id
   2. Output
      - if report generation is not complete, return “Running” as the output
      - if report generation is complete, return “Complete” along with the CSV file with the schema described above.

## Data output requirement

The output should be a report to the user that has the following schema

`store_id, uptime_last_hour(in minutes), uptime_last_day(in hours), update_last_week(in hours), downtime_last_hour(in minutes), downtime_last_day(in hours), downtime_last_week(in hours)`

1. Uptime and downtime should only include observations within business hours.
2. Extrapolate the uptime and downtime based on the periodic polls, to the entire time interval.

## Data sources

There are 3 sources of data

1. The poll is conducted for every store roughly every hour and have data about whether the store was active or not in a CSV. The CSV has 3 columns (`store_id, timestamp_utc, status`) where status is active or inactive. All timestamps are in **UTC**

2. The business hours of all the stores - schema of this data is `store_id, dayOfWeek(0=Monday, 6=Sunday), start_time_local, end_time_local`

   1. These times are in the **local time zone**
   2. If data is missing for a store, assume it is open 24\*7

3. Timezone for the stores - schema is `store_id, timezone_str`
   1. If data is missing for a store, assume it is America/Chicago
   2. This is used so that data sources 1 and 2 can be compared against each other.
