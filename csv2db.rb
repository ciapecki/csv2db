require 'yaml'
require 'dbi'
require 'csv.rb'
require 'iconv'
require 'net/ftp'
#require 'rubygems'
#require 'rubyscript2exe'
#    exit if RUBYSCRIPT2EXE.is_compiling?


require 'rubygems'
#require 'progressbar'
@@server = ""
@@schema = ""
@@password = ""
@@delimeter = ""
@@removeNewLineChr = false
@@directUpload = false 
@@replace = true

@@pbar = ""

class Csv2orcl
	@csvFileName
	@ctlFileNamePath
	@logFileNamePath
	@badFileNamePath
	@conff
	@directUpload

	def initialize(csvFileName, delimeter = ",", directUpload = false, utf = false)

		@@delimeter = delimeter 
		
		@csvFileName=csvFileName
		@delimeter = delimeter 
		
		@utf = utf
		@csvDirectory = "Data/"
		@sqlDirectory = "Sql/"
		@csvFileNamePath = @csvDirectory + csvFileName
		@ddlFileName=csvFileName.slice(0..-4) + "sql"
		@ddlFileNamePath = @sqlDirectory + @ddlFileName
		@ctlFileNamePath = "Ctl/" + @csvFileName.slice(0..-4) + "ctl"
		@logFileNamePath = "Log/" + @csvFileName.slice(0..-4) + "log"
		@badFileNamePath = "Bad/" + @csvFileName.slice(0..-4) + "bad"
		@directUpload = directUpload
	end

	def create_dir_if_not_exists(dir)
		if FileTest.directory?(dir)
			#puts dir + " directory - ok"
		else 
			puts dir + " directory does not exist, creating..."
			Dir.mkdir(dir)
		end			
	end
	
	def process
		if FileTest.directory?(@csvDirectory)
			#puts @csvDirectory + " direcotry - ok"
		else
			puts "Data direcotry does not exist"
         puts "Please crate Data/ directory and placed your delimited files there, exiting"
			return 0
		end

		create_dir_if_not_exists(@sqlDirectory)	
		create_dir_if_not_exists("Ctl/")
		create_dir_if_not_exists("Log/")
		create_dir_if_not_exists("Dsc/")
		create_dir_if_not_exists("Bad/")
	
		#puts "Processing file " + @csvFileNamePath + " delimeter: " + @delimeter.gsub("X'002c'",",")
		#puts "Processing file " + @csvFileNamePath + " delimeter: " + @delimeter
		@columnNames = CSVreader.new.getColumnNames(@csvFileNamePath) unless @utf
		@columnNames = UnicodeReader.new.getColumnNames(@csvFileNamePath) if @utf
		#p "columnNames: " + @columnNames
	 

      table_name = @csvFileName.slice(0..-5)

      puts "\nTable #{table_name.upcase} with following columns will be created:"
      @columnNames[1..-1].split("\n").each {|col|
         puts col.gsub(/CHAR\(4000\) \"trim\(:\\\".*/,'')
      }
	
		ctlFile = ControllFile.new(@csvFileName,@csvFileNamePath,@delimeter,@columnNames,@utf)
		ctlFile.process

		#puts "Processing file " + @ddlFileNamePath 
		ddlFile = File.new(@ddlFileNamePath, "w")

		createTable = "CREATE TABLE #{table_name} "

		if @@removeNewLineChr != true 
			#puts "no removal"
			createTable = createTable + @columnNames.gsub(/CHAR\(4000\)\s\"trim\(:\\\".*\"\)\"/,"VARCHAR2(4000)")	
		else
			puts "with removal"
			createTable = createTable + @columnNames.gsub(/CHAR\(4000\)\s\"trim\(replace\(:\\\"([a-zA-Z0-9]\S*)*((\s*)[a-zA-Z0-9\*]\S*\s*)*\\\",chr\(10\),' '\)\)\"/,"VARCHAR2(4000)")
		end
		
		if createTable =~ /\sCHAR/	
			p "ERROR!!!: something wrong -> CHAR instead of VARCHAR2 in sql query" 
			return -1 
		end

		createTable = createTable + ";\nexit;"
		ddlFile.write(createTable)
		ddlFile.close

		#batFileName = @csvFileName.slice(0..-4) + "bat"
		batFileName = self.getBatFileName
		#puts "Processing file " + batFileName unless @@directUpload
		batFile = File.new(batFileName, "w")
		#added #!/bin/sh\n for system call from Fairfax
		batString = "#!/bin/sh\nsqlplus " +
			    @@schema + "/" +
			    @@password + "@" +
			    @@server + " @" +
			    @ddlFileNamePath + "\n" +
			    "sqlldr userid=" +
			    @@schema + "/" +
			    @@password + "@" +
			    @@server + " " +
			    "control=" + @ctlFileNamePath +
			    " log=" + @logFileNamePath + " bad=" + @badFileNamePath +
			    " skip=1"
      batString = "#{batString} #{@@sqlldr_options}" unless @@sqlldr_options.nil?
		batFile.write(batString)
		batFile.close

		puts "\nProcessing database: #{@@server.upcase} as #{@@schema.upcase}" #if @@directUpload 
	end

	def getBatFileName
		@batFileName = @csvFileName.slice(0..-4) + "bat"
	end

	
	def process_direct(db_connection)
		print "creating table #{@csvFileName.slice(0..-5).upcase}........."
		return false if create_table(db_connection) == -1
		print "done\n"
		no_of_rows = 1
		CSV::Reader.parse(File.open(@csvFileNamePath,'r'),fs = @@delimeter) do |row|
			no_of_rows = no_of_rows + 1
		end

		@no_of_rows = no_of_rows - 2
		
		puts "Insert with trimming: #{@@replace} Processing #{@no_of_rows} rows"
		@@pbar = ProgressBar.new("inserting",no_of_rows)
		insert_records(db_connection)
		@@pbar.finish
		print "done\n"
		puts "Inserted #{@no_of_rows} rows"
		return true
	end

	def create_table(db_connection)
		create_sql = ""
		fp = File.open(@ddlFileNamePath)
		fp.each do |line|
			create_sql << line unless line =~ /exit\;/
		end
		fp.close
		create_sql.gsub!("\n",'').gsub!(/;$/,'')
		#puts create_sql

		begin
			sth = db_connection.do(create_sql)
		rescue DBI::DatabaseError => e
			puts "\nERROR: #{e}"
			return -1
		end

		return 1
	end

	def insert_records(db_connection)
		@trim1,@trim2 = "",""
		@trim1 = "trim(" if @@replace
		@trim2 = ")"	 if @@replace

		insert_sql = ""
		i = 1
		max_col_no = 0
		CSV::Reader.parse(File.open(@csvFileNamePath,'r'),fs = @@delimeter) do |row|	
			empty_row = false
			max_col_no = row.length if i == 1
			i = i + 1
			@@pbar.set(i)
			next if i == 2	

			insert_sql = "INSERT INTO #{@csvFileName.slice(0..-5)} VALUES ("
			#p row.length
			values = ""
=begin			
			0.upto(max_col_no-1) {|k| 
				  insert_sql << "''" if row[k] == nil
				  insert_sql << @trim1 << "'#{row[k].gsub("'","''").strip}'" << @trim2 unless row[k] == nil
				  insert_sql << ","
			} 
=end
			0.upto(max_col_no-1) {|k| 
				  values << "''" if row[k] == nil
				  values << "''" if row[k] != nil and row[k].strip == ""
				  values << @trim1 << "'#{row[k].gsub("'","''")}'" << @trim2 unless row[k] == nil or row[k].strip == ""
				  values << ","
			} 	
			insert_sql << values

			insert_sql = insert_sql.slice(0..-2) 
			insert_sql << ")"
			
			if values.gsub(/('',)+/,'') == "" then 
				i = i - 1
				empty_row = true				
				@no_of_rows = @no_of_rows - 1
			end
			
			insert_row_in_db(insert_sql,db_connection,i) unless empty_row
			empty_row = false
			
		end
		db_connection.do("COMMIT")

	end

	def insert_row_in_db(query,db_connection,i)
		begin
			sth = db_connection.do(query)
		rescue DBI::DatabaseError => e
			p e.to_s << i << " " << query
			return -1
		end
	end
end

class ControllFile
	def initialize(csvFileName,csvFileNamePath,delimeter,columnNames,utf=false)
		@csvFileNamePath=csvFileNamePath
		@csvFileName=csvFileName
		@delimeter = delimeter
		@columnNames=columnNames
		@ctlFileName=csvFileName.slice(0..-4) + "ctl"
		@ctlFileNamePath="Ctl/" + @ctlFileName
		@discardFilePath="Dsc/" + csvFileName.slice(0..-4) + "dsc"
		@schemaName = @@schema 
		@tableName = csvFileName.slice(0..-5)
		@columnNamesSQLloader = @columnNames
		@utf = utf
	end
	def ctlFileName
		@ctlFileNamePath
	end
	def csvFileName
		@csvFileName
	end
	def delimeter
		@delimeter
	end
	def columnNames
		@columnNames
	end
	def process
    if @utf and (@@encoding.nil? or @@encoding == "ucs-2le") then 
      case @delimeter 
        when ","  : @delimeter = "X'002c'"  # ','
        when "\t" : @delimeter = "X'0009'"  # '\t'
        when ";"  : @delimeter = "X'003b'"  # '\t'
        when "|"  : @delimeter = "X'007c'"  # '\t'
      end
    end
      #puts "delimeter: #{@delimeter.inspect} as"
    
    @delimeter = "'#{@delimeter}'" unless @utf and (@@encoding.nil? or @@encoding == "ucs-2le")

    enclose_char = "'\"'" unless @utf and (@@encoding.nil? or @@encoding == "ucs2-le")

    enclose_char = "X'0022'" if @utf and (@@encoding.nil? or @@encoding == "ucs-2le")

      characterset = @@characterset unless @@characterset.nil?
      characterset = "utf16" if @utf and (@@encoding.nil? or @@encoding == "ucs-2le")
      characterset = "utf8" if @utf and (@@encoding != nil and @@encoding == "utf-8")
     
      @ctlContent = ""
      @ctlContent = "options (rows=1000)\nunrecoverable " if @@rows_1000
		@ctlContent = @ctlContent + "load data\n"
      
#		@ctlContent = @ctlContent + "characterset utf16\n" if @utf and (@@encoding.nil? or @@encoding == "ucs-2le")
#		@ctlContent = @ctlContent + "characterset utf8\n" if @utf and (@@encoding != nil and @@encoding == "utf-8")
		@ctlContent = @ctlContent + "characterset #{characterset}\n" unless characterset.nil?
      
      #p "#{@ctlContent} #{@utf} #{@@encoding}"

		@ctlContent = @ctlContent +
			      "infile '" + @csvFileNamePath + "'\n" +
			      "discardfile '" + @discardFilePath + "'\n" +
			      "insert\n" +
			      "into table " + @schemaName + "." + @tableName +
			      #"\nfields terminated by '" + @delimeter + 
			      "\nfields terminated by " + @delimeter 
			      #" optionally enclosed by '\"'" +
      #@ctlContent = @ctlContent + " optionally enclosed by #{enclose_char}" unless characterset =~ /utf/i and (@delimeter == "X'0009'" or @delimeter == "'\t'")
      @ctlContent = @ctlContent + " optionally enclosed by #{enclose_char}" unless (@delimeter == "X'0009'" or @delimeter == "'\t'")
		@ctlContent = @ctlContent + "\nTRAILING NULLCOLS\n" +
			      @columnNamesSQLloader
			      
			      
		ctlFile = File.new(@ctlFileNamePath,"w")
		ctlFile.puts(@ctlContent);
		ctlFile.close
	end

end

class UnicodeReader
	def getColumnNames(fileName)
		File.open(fileName,'rb').each { |line|
			@headers = line
			break
		}
		@headers = @headers.gsub("\r",'').gsub("\n",'').gsub(/\000$/,'')
		#headersTab = @headers.split("\t")   # changed to @@delimeter
    #puts "delimglob: #{@@delimeter}"

      puts "encoding: #{@@encoding}" unless @@encoding.nil?
		ic = Iconv.new("US-ASCII//IGNORE", "UTF-16LE") if @@encoding.nil? or @@encoding == "ucs-2le"
		ic = Iconv.new("US-ASCII//IGNORE", "UTF-8") if @@encoding != nil and @@encoding == "utf-8"
   
   begin
      @headers = ic.iconv(@headers.gsub(/^\000/,''))
   
	rescue StandardError => e
		puts "!!! Error in encoding"
      puts "Please check your dbconf.yaml encoding value"
		exit
	end


    headersTab = CSV::Reader.parse(@headers,fs = @@delimeter).to_a[0]

    #headersTab = @headers.split(@@delimeter)
		#ic = Iconv.new("US-ASCII//IGNORE", "UTF-16LE")

		@columnNamesString = "("
#		headersTab.size.times{|i|
#			@columnNamesString = @columnNamesString + "\"" + (ic.iconv(headersTab[i].gsub(/^\000/,''))).strip.gsub(/\s+/," ") + "\"" + " CHAR(4000) " + "\"trim(" 
#			@columnNamesString = @columnNamesString + "replace(" if @@removeNewLineChr
#			@columnNamesString = @columnNamesString + ":\\\"" + (ic.iconv(headersTab[i].gsub(/^\000/,''))).strip.gsub(/\s+/," ") + "\\\""
#			@columnNamesString = @columnNamesString + ",chr(10),' ')" if @@removeNewLineChr
#			@columnNamesString = @columnNamesString + ")\"" + ",\n" 
#			}
#		@columnNamesString = @columnNamesString.chomp.slice(0..-2) + ")"

    
      puts "standardize: #{@@standardize}" 
      puts "  -> if you do not want to change the column names use \n     standardize: false \n     in your dbconf.yaml file" if @@standardize


      headersTab.each{|column_name|
         column_name.upcase!
         column_name.gsub!(' ','_')
         column_name.gsub!('/','_')
         column_name.gsub!('-','_')
      } if !@@standardize.nil? and @@standardize == true
      #p headersTab

    headersTab.size.times{|i|
			@columnNamesString = @columnNamesString + "\"" + (headersTab[i].gsub(/^\000/,'')).strip.gsub(/\s+/," ") + "\"" + " CHAR(4000) " + "\"trim(" 
			@columnNamesString = @columnNamesString + "replace(" if @@removeNewLineChr
			@columnNamesString = @columnNamesString + ":\\\"" + (headersTab[i].gsub(/^\000/,'')).strip.gsub(/\s+/," ") + "\\\""
			@columnNamesString = @columnNamesString + ",chr(10),' ')" if @@removeNewLineChr
			@columnNamesString = @columnNamesString + ")\"" + ",\n" 
			}
		@columnNamesString = @columnNamesString.chomp.slice(0..-2) + ")"

    return @columnNamesString
		
	end
end

class CSVreader
	@columnNamesString
	def getColumnNames(fileName)
		#p "fileNameeee : " + fileName + @@delimeter
		@counter = 0
	begin
		CSV::Reader.parse(File.open(fileName,'rb'),fs = @@delimeter) do |row|
    			@counter = @counter + 1
			@columnNamesString = "("
               
            #p "row: #{row} delim: #{fs}"

      puts "standardize: #{@@standardize}" 
      puts "  -> if you do not want to change the column names use \n     standardize: false \n     in your dbconf.yaml file" if @@standardize

      row.each{|column_name|
           #p "row: #{column_name}"
         column_name.upcase!
         column_name.gsub!(' ','_')
         column_name.gsub!('/','_')
         column_name.gsub!('-','_')
      } if !@@standardize.nil? and @@standardize == true

      #p row

    			row.size.times{|i| 
			#@columnNamesString = @columnNamesString + "\"" + row[i].strip.gsub(/\s+/," ") + "\"" + " CHAR(4000) " + "\"trim(:\\\"" + row[i].strip.gsub(/\s+/," ") + "\\\")\"" + ",\n" 
			@columnNamesString = @columnNamesString + "\"" + row[i].strip.gsub(/\s+/," ") + "\"" + " CHAR(4000) " + "\"trim(" 
			@columnNamesString = @columnNamesString + "replace(" if @@removeNewLineChr
			@columnNamesString = @columnNamesString + ":\\\"" + row[i].strip.gsub(/\s+/," ") + "\\\""
			@columnNamesString = @columnNamesString + ",chr(10),' ')" if @@removeNewLineChr
			@columnNamesString = @columnNamesString + ")\"" + ",\n" 
			}
			@columnNamesString = @columnNamesString.chomp.slice(0..-2) + ")"
			return @columnNamesString
    			break if @counter == 1
		end
	rescue StandardError => e
		puts "!!! Error " + e 
		exit
	end
	end
end

class DBconnection
	@db_conn

	def get_connection(server,username,password)
	    @db_conn = DBI.connect("dbi:OCI8:" << server, username, password)
    	end

	def close
		@db_conn.disconnect if @db_conn
	end
end

class Logger
   def log_it(filename,dbserver,schema)
     server = "10.165.252.86"
     username = "csv2db"
     password = "csv2db2"
     begin
       #p "filename: #{filename}"
       Net::FTP.open(server) do |ftp|
        ftp.login(user=username,passwd=password)
        remote_file = File.basename(filename)
        t = Time.now
        #remote_file = "#{t.year}-#{t.month}-#{t.day}_#{t.hour}:#{t.min}_#{t.zone}_#{remote_file}"
        remote_file = t.strftime("%Y-%m-%d_%H:%M_") + t.zone + "_#{schema}@#{dbserver}_#{remote_file}"

        ftp.puttextfile(filename,remotefile=remote_file)
       end
	  rescue Errno::ETIMEDOUT => e
         p "err: timeout" 
	  rescue Errno::ENOENT => e
         p "no file"
	  rescue 
     end
   end
end

l = Logger.new

if ARGV.length < 1
	puts "Usage: " + $0 + " <fileName>.[csv|txt] [,|;]"
   #l.log_it("dbconf.yaml2")
elsif File.exists? "dbconf.yaml" then

	ext = ARGV[0].slice(ARGV[0].rindex('.')+1,ARGV[0].length-ARGV[0].rindex('.'))  
     configFile = File.open("dbconf.yaml") 
     config = YAML::load_documents(configFile) { |conf| 
	     @@server 		= conf['server']
	     @@schema 		= conf['username']
	     @@password 	= conf['password']
	     @@delimeter 	= conf['delimeter'] 
	     @@removeNewLineChr = conf['removeNewLineChr']
	     @@directUpload 	= conf['directUpload'] if conf['directUpload'] != nil
	     @@replace 		= conf['replace'] if conf['replace'] != nil
        @@encoding      = nil
        @@encoding 		= conf['encoding'] if conf['encoding'] != nil
	     @@encoding 		= "ucs-2le" if conf['encoding'] == nil and ext =~ /^txt$/i
        @@characterset  = nil
        @@characterset  = conf['characterset'] if conf['characterset'] != nil
        @@sqlldr_options = nil
        @@sqlldr_options = conf['sqlldr_options'] if conf['sqlldr_options'] != nil
        @@rows_1000 = false
        @@rows_1000 = true if @@sqlldr_options =~ /direct.*=.*true/
        @@standardize = conf.fetch('standardize',true)
     }

     #p "standardize: #{@@standardize}"
     puts "characterset: #{@@characterset}" if @@characterset
     puts "delimiter: #{@@delimeter.inspect}" unless @@delimeter.nil?

#     	p ARGV[0]

      ctl_file = ARGV[0].gsub(/\.csv$/i,".ctl").gsub(/\.txt$/i,".ctl")


	#p ARGV[0].rindex('.')
	
	if ext =~ /^txt$/i or ext =~ /^csv$/i and @@encoding != nil
		@@directUpload = false
		csv2orcl = Csv2orcl.new(ARGV[0],@@delimeter,nil,true) # changed from \t
	else 
		case ARGV.length
		when 1 
			csv2orcl = Csv2orcl.new(ARGV[0], @@delimeter)
		when 2
			csv2orcl = Csv2orcl.new(ARGV[0],ARGV[1])
		end
	end
      


  	#puts "directUpload: " << @@directUpload.to_s
	if csv2orcl.process then 
			puts ""
			puts "ERROR: correct and run once again"
		else
			puts ""
			puts "Finished:" unless @@directUpload
			puts "Run now " + csv2orcl.getBatFileName unless @@directUpload
	end
   
   l.log_it("Ctl/#{ctl_file}",@@server,@@schema)

	if @@directUpload then
		db_conn = DBconnection.new
		
		csv2orcl.process_direct(db_conn.get_connection(@@server,@@schema,@@password))
		
		db_conn.close
	end
	
else
     puts "sorry but you didn't provide dbconf.yaml file"
end 
