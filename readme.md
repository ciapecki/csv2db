Rel. 1.4

1.4     - working in ruby 2.x
        - supporting UTF8 only -> you need to save in Excel as csv UTF8

1.3     - removed annoying BOM added by some text editors and lists generated from GCM

1.2     - added deduplication of duplicated column names
        e.g. 
        if input file contained 3 same column names 
        column_name,column_name,column_name
        it will be converted to
        column_name,column_name_2,columns_name_3

1.1     - changed behaviour when converting files with new line characters in fields, to process line by line and not at once as in 1.0

1.0     - added handling of embedded new line characters.
          the issue is with sqlldr that requires a workaround for handling embedded new line characters:
          
          based on: http://asktom.oracle.com/pls/asktom/f?p=100:11:0::::P11_QUESTION_ID:1286201753718
          The options for loading data with embedded newlines are now as follows:

          o Load the data with some other character in the data that represents a newline (e.g. put 
          the string \n in the text where a newline should appear) and use a SQL function to 
          replace that text with a chr(10) during load time.  This works for string data of 4000 
          bytes or less only!  This works in all releases (although in 7.x, the limit is 2000 
          bytes)

          o Use the FIX attribute on the infile directive and load a fixed length flat filed.  This 
          works in all releases.

          o Use the VAR attribute on the infile directive and load a varying width file that uses a 
          format such that the first few bytes of each line are the LENGTH of the line to follow.  
          This also works in all releases.

          o Use the STR attribute on the infile directive to load a varying width file with some 
          sequence of characters that represent the end of line  as opposed to just the newline 
          character representing that.  This is new in Oracle8i release 8.1.6 (it appears to work 
          in 8.1.5 but it is not documented nor supported and you get funny results in the log file 
          about the number of records actually loaded and processed.  In 8.1.6 -- it works as 
          expected and is documented/supported)


          The option used in csv2db is the 4th option with STR.

          As of asktom.oracle.com example:
          
          Using the STR attribute:

          This is perhaps the most flexible method of loading data with embedded newlines.  Using 
          the STR attribute  I can specify a new end of line character (or sequence of 
          characters).  This allows you to create an input datafile that has some special character 
          at the end of each line  the newline is no longer special.

          I prefer to use a sequence of characters  typically some special marker and then a 
          newline.  This makes it easy to see the end of line character when viewing the input data 
          in a text editor or some utility as each record still has a newline at the end of it.  To 
          use this, we might have a control file like:

          load data
          infile str.dat "str X'7c0a'"
          into table T
          TRUNCATE
          fields terminated by ',' optionally enclosed by '"'
          (
          TEXT
          )

          The above usings the STR X7c0a to specify that each input record will end with a | 
          (pipe character) and a newline.  To construct that string  made of the hex characters 
          X7c0a  I used SQLPlus.  For example to see that a PIPE is 7c, and a newline 0a, I 
          simply:

          ops$tkyte@DEV816> select to_char( ascii( '|' ), 'xx' ) from dual
            2  /

          TO_
          ---
           7c

          ops$tkyte@DEV816> select to_char( ascii( '
            2  ' ), 'xx' ) from dual;

          TO_
          ---
            a

          So, if your input data looks like:

          $ cat str.dat
          123456789012345678901234|
          how now
          brown cow|
          this
          is
          another
          line|

          The above control file will load it correctly.  


0.9.9.3 - added shortening column names upto 30chars, when longer names are provided

0.9.9.2 - added chown executable properties for execution under linux

0.9.9 - files with - in name will be properly processed, creating table names enclosed in " chars
         -> maybe it would be a good thought to standardize it in similar way as column names

0.9.8 - changed delimeter to delimiter in dbconf.yaml (actually both spellings are acceptable in this file)

0.9.7 - filename to process can be in form: Data\<filename.[txt|csv]>
         or Data/<filename.[txt|csv]>
         which is useful when using bash completion
         - standardized columns replaces #_OF with NO_OF, 
            as well #,(,) with _
            and consecutive _+ with single _

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
