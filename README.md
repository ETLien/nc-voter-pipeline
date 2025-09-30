----------------------

NC VOTER DATA PIPELINE

----------------------



A Python pipeline for downloading, normalizing, and archiving North Carolina State Board of Elections voter registration and voter history data.

The project maintains a PostgreSQL database that tracks changes to voter records over time, ensuring historical accuracy for analysis.



--------

Features

--------



* Download and archives weekly NC voter registration and voter history files.
* Normalizes whitespace and enforces consistent column order.
* Tracks updates per voter (ncid) using a data\_date field.
* Deduplicates records to preserve only meaningful changes.
* Appends new data into historical tables for longitudinal analysis.



----------------------

Database Prerequisites

----------------------



This pipeline requires a running PostgreSQL database with two target tables. You can name these however you wish however I use the following schema locally:



* public.nc\_vreg\_history (voter registration history)
* public.nc\_vhis\_history (voter history)



Both tables should match the NC State Board of Elections public file structures, with the addition of a data\_date column (used to record when a row was published by the state).



If the tables do not exist, create them before running the pipeline.

Example (simplified schema):



CREATE TABLE public.nc\_vreg\_history (

&nbsp;   data\_date TEXT,

&nbsp;   ncid TEXT,

&nbsp;   county\_id TEXT,

&nbsp;   last\_name TEXT,

&nbsp;   first\_name TEXT,

&nbsp;   middle\_name TEXT,

&nbsp;   status TEXT,

&nbsp;   party\_affiliation TEXT

&nbsp;   -- other fields from the NC voter file...

);



CREATE TABLE public.nc\_vhis\_history (

&nbsp;   data\_date TEXT,

&nbsp;   ncid TEXT,

&nbsp;   election\_label TEXT,

&nbsp;   voting\_method TEXT,

&nbsp;   voted\_party TEXT

&nbsp;   -- other fields from the NC history file...

);



-------------

Configuration

-------------



Before running, edit the script to match your environment:



* Database connection details (DB\_USER, DB\_PASSWORD, DB\_HOST, DB\_PORT, DB\_NAME).
* Paths for downloads and archives (DOWNLOAD\_PATH, ARCHIVE\_PATH).



------------

How It Works

------------



* Archive tables; copies existing SQL tables to safeguard against errors.
* Download files; retrieves voter registration \& history zips from NCSBE.
* Archive \& unzip; copies downloads into archive and extracts text files.
* Process files; normalizes whitespace, adds data\_date, deduplicates records.
* Append to DB; inserts only new/changed records into the history tables.



-----------

Data Source

-----------



NCSBE's Voter Registration Data: https://www.ncsbe.gov/results-data/voter-registration-data



-------

License

-------


This project is offered under a **dual license**:

- **MIT License** – You are free to use, modify, and distribute this software, provided that attribution is given.  
- **Commons Clause** – Commercial use of the software requires prior permission.  

See the [LICENSE](LICENSE) file for full details.


