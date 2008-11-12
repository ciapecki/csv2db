Rel. 0.9.1

0.9.1 - supports characterset 
       e.g. characterset: WE8ISO8859P1

0.9
    - supported delimeters in unicode files as well
      currently supported:
      - <TAB>
      - ,
      - ;
      - |

0.7
    - uploading unicode files tab seperated as .txt

0.6
    -skipping empty lines when csv with empty lines provided
    -directUpload -> insert data directly into database without SQLloader
			(oracle client still needed)
    -replace	  -> trim white spaces inserting data (true  -> trim
					  	       false -> do not trim
		     default = true)

    -nice ProgressBar uploading directly :)

Usage:
1. Prepared csv file put into Data folder (e.g. file.csv) 
   - the name before "." will be the name of the new created table in db
   - csv file should have in its first row column names
   - column names must not include not allowed for db characters
2. Run in the main location csv2db.exe <csvFileName>.csv
   - e.g. csv2db file.csv
   - this should create file.bat in main location
   - run file.bat to run sqlloader that inserts rows from csv to db
