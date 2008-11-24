Rel. 0.9.7pre

0.9.7 - filename to process can be in form: Data\<filename.[txt|csv]>
         or Data/<filename.[txt|csv]>
         which is useful when using bash completion

0.9.6 - now even files with .csv can be treated as Unicode files
         in these cases you MUST specify encoding in your dbconf.yaml file
         e.g. 
            test_GCM_download.csv (encoded in ucs-2le ~ excel Unicode encoding)
            in dbconf.yaml put:
            encoding: ucs-2le
            (it is not needed when you change the extension to .txt -> default all .txt files are treated as encoded in ucs-2le)

0.9.5 - added standardized column names (as default):
         e.g. "Party Name" => PARTY_NAME
         if you want to disable that feature put in dbconf.yaml
         standardize: false 

0.9.4 - removed optionally enclosed by case when uploading tab separated fields

0.9.3 - works under Linux (tested on Debian)
      - a very big performance gain (rows=1000) by direct loads
      - supported Unicode (as from excel)
      - supported utf8
      e.g. characterset: utf8
           characterset: ucs-2le (default) not need to be set
           both multibyte charactersets need to have a datafile with .txt extension

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
