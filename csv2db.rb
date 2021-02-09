#!/usr/local/bin/ruby

$VERBOSE = nil

require 'rubygems'
require 'yaml'
require 'dbi'
require 'csv.rb'
require 'iconv'
require 'net/ftp'
require 'rubygems'
# gem 'fastercsv', '=1.5.3'
#require 'fastercsv'
# require 'system_timer' if RUBY_PLATFORM !~ /mswin|mingw/i
#require 'rubyscript2exe'
#    exit if RUBYSCRIPT2EXE.is_compiling?


require 'rubygems'
#require 'progressbar'



class Csv2orcl


  @@server = ""
  @@schema = ""
  @@password = ""
  @@delimeter = ""
  @@removeNewLineChr = false
  @@directUpload = false 
  @@replace = true

  @@pbar = ""

  @csvFileName
  @ctlFileNamePath
  @logFileNamePath
  @badFileNamePath
  @conff
  @directUpload

  attr_accessor :csvFileName, :encoding, :delimeter, :csvFileNamePath

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

    #p "Processing..... with handling newline characters?: #{@@handle_new_lines} with encoding #{@@encoding} #{@utf}"

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
    #puts "Processing file " + @csvFileNamePath + " delimeter: " + @delimeter.inspect
    @columnNames = CSVreader.new.getColumnNames(@csvFileNamePath) unless @utf
    @columnNames = UnicodeReader.new.getColumnNames(@csvFileNamePath) if @utf
    #p "columnNames: " + @columnNames
   

      table_name = @csvFileName.slice(0..-5)
      table_name = @csvFileName.slice(0..-5).gsub!(/^u8nl_/,'') if @@handle_new_lines

      puts "\nTable #{table_name.upcase} with following columns (see if on separate lines) will be created:"
      @columnNames[1..-1].split("\n").each {|col|
         puts col.gsub(/CHAR\(4000\) \"trim\(:\\\".*/,'')
      }
  
    ctlFile = ControllFile.new(@csvFileName,@csvFileNamePath,@delimeter,@columnNames,@utf)
    #ctlFile = ControllFile.new(@csvFileName,@csvFileNamePath,@delimeter,@columnNames,@utf) if @@handle_new_lines
    ctlFile.process

    #puts "Processing file " + @ddlFileNamePath 
    ddlFile = File.new(@ddlFileNamePath, "w")

    createTable = "CREATE TABLE #{table_name} "
    createTable = "CREATE TABLE \"#{table_name}\" " if table_name.include?("-")

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
      batString = ""
      batString = "#!/bin/sh\n" if RUBY_PLATFORM !~ /mswin|mingw/i
      batString = batString + "sqlplus " +
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
      batFile.chmod(0700) if RUBY_PLATFORM !~ /mswin|mingw/i
    batFile.close

    puts "\nProcessing database: #{@@server.upcase} as #{@@schema.upcase}" #if @@directUpload 
  end

  def getBatFileName
    @batFileName = @csvFileName.slice(0..-4) + "bat" 
    @batFileName = @csvFileName.slice(0..-4).gsub!(/^u8nl_/,'') + "bat" if @@handle_new_lines
    @batFileName
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
      puts "\nERRORooo: #{e}"
      return -1
    end

    return 1
  end

  def insert_records(db_connection)
    @trim1,@trim2 = "",""
    @trim1 = "trim(" if @@replace
    @trim2 = ")"   if @@replace

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
    @ctlFileName=csvFileName.slice(0..-4).gsub!(/^u8nl_/,'') + "ctl" if @@handle_new_lines
    #p "in ctl creation with handle_new_lines set to #{@@handle_new_lines} ctl: #{@ctlFileName}"
      # table_name = @csvFileName.slice(0..-5).gsub!(/^u8nl_/,'')
    @ctlFileNamePath="Ctl/" + @ctlFileName
    @discardFilePath="Dsc/" + csvFileName.slice(0..-4) + "dsc"
    @discardFilePath="Dsc/" + csvFileName.slice(0..-4).gsub!(/^u8nl_/,'') + "dsc" if @@handle_new_lines
    #p "in ctl creation with handle_new_lines set to #{@@handle_new_lines} dsc: #{@discardFilePath}"
    @schemaName = @@schema 
    @tableName = csvFileName.slice(0..-5)
    @tableName = csvFileName.slice(0..-5).gsub!(/^u8nl_/,'') if @@handle_new_lines
    #p "in ctl creation with handle_new_lines set to #{@@handle_new_lines} tableName: #{@tableName}"
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
        when ","  then @delimeter = "X'002c'"  # ','
        when "\t" then @delimeter = "X'0009'"  # '\t'
        when ";"  then @delimeter = "X'003b'"  # '\t'
        when "|"  then @delimeter = "X'007c'"  # '\t'
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
      
#    @ctlContent = @ctlContent + "characterset utf16\n" if @utf and (@@encoding.nil? or @@encoding == "ucs-2le")
#    @ctlContent = @ctlContent + "characterset utf8\n" if @utf and (@@encoding != nil and @@encoding == "utf-8")
    @ctlContent = @ctlContent + "characterset #{characterset}\n" unless characterset.nil?
      
      #p "#{@ctlContent} #{@utf} #{@@encoding}"

      ctl_tablename = @tableName
      ctl_tablename = "\"#{@tableName}\"" if @tableName =~ /-/

    @ctlContent = @ctlContent +
            "infile '#{@csvFileNamePath}'" 
    @ctlContent = @ctlContent + " \"str X'7c7e7c0a'\"" if @@handle_new_lines
    @ctlContent = @ctlContent + "\n" +
            "discardfile '" + @discardFilePath + "'\n" +
            "insert\n" +
            "into table " + @schemaName + "." + ctl_tablename +
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

  def deduplicate_columns(columns_tab)
    
    h = Hash.new{0}
    
    columns_tab.each{|el|
      #h[el.gsub(/\|~\|$/,'')] += 1
      h[el] += 1
    }

  # p h

    h.each do |k, v|
      while v > 1
        new_value = k.chop.chop + "_#{v}" if k.length > 27
        new_value = k + "_#{v}" if k.length <= 27
        puts "duplicated column #{k} changed to #{new_value}"
        columns_tab[columns_tab.rindex(k)] = new_value
        v -= 1
      end
    end

    columns_tab
  end

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
      #p "headers #{@headers.inspect}"
      #p "headers #{@headers.class}"
      @headers = ic.iconv(@headers.gsub(/^\000/,'')) if @@encoding == "utf-8"
      # @headers = @headers[1..10] if @@encoding == "ucs-2le"
   #p "after headers #{@headers.inspect}"
  rescue StandardError => e
    puts "!!! Error in encoding #{e.inspect}"
      puts "Please check your dbconf.yaml encoding value"
    exit
  end


   #p "we're here"
    #headersTab = CSV.parse(@headers,fs = @@delimeter).to_a[0]
  
    #p @headers
    #p @@delimeter
   
    headersTab = CSV.parse(@headers,{:col_sep => @@delimeter}).to_a[0]



    #headersTab = @headers.split(@@delimeter)
    #ic = Iconv.new("US-ASCII//IGNORE", "UTF-16LE")

    @columnNamesString = "("
#    headersTab.size.times{|i|
#      @columnNamesString = @columnNamesString + "\"" + (ic.iconv(headersTab[i].gsub(/^\000/,''))).strip.gsub(/\s+/," ") + "\"" + " CHAR(4000) " + "\"trim(" 
#      @columnNamesString = @columnNamesString + "replace(" if @@removeNewLineChr
#      @columnNamesString = @columnNamesString + ":\\\"" + (ic.iconv(headersTab[i].gsub(/^\000/,''))).strip.gsub(/\s+/," ") + "\\\""
#      @columnNamesString = @columnNamesString + ",chr(10),' ')" if @@removeNewLineChr
#      @columnNamesString = @columnNamesString + ")\"" + ",\n" 
#      }
#    @columnNamesString = @columnNamesString.chomp.slice(0..-2) + ")"

    
      puts "standardize: #{@@standardize}" 
      puts "  -> if you do not want to change the column names use \n     standardize: false \n     in your dbconf.yaml file" if @@standardize

      puts "\n"

        # p headersTab

      headersTab.each{|column_name|
         column_name.upcase!
         column_name.gsub!(' ','_')
         column_name.gsub!('/','_')
         column_name.gsub!('-','_')
         column_name.gsub!('&','_')
         column_name.gsub!(')','_')
         column_name.gsub!('(','_')
         column_name.gsub!('#_OF','NO_OF')
         column_name.gsub!('#','_')
         column_name.gsub!(/_+/,'_')
         column_name.slice!(30..-1) 
      } if !@@standardize.nil? and @@standardize == true

      no_of_cols = headersTab.length

      # p headersTab
      headersTab[no_of_cols-1].gsub!(/\|~\|$/,'')
      # p headersTab

      headersTab = deduplicate_columns(headersTab) if !@@standardize.nil? and @@standardize == true


      # do not understand it now :(
      # p headersTab
      #headersTab[no_of_cols-1] = headersTab[no_of_cols-1][0,(headersTab[no_of_cols-1].length)-3] if @@handle_new_lines


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

# class ColumnNamesFormatter
#   def format(column_names_array)
# 
#   end
# end

class CSVreader
  @columnNamesString
  
  def deduplicate_columns(columns_tab)
    h = Hash.new{0}
    
    columns_tab.each{|el|
      h[el.gsub(/\|~\|$/,'')] += 1
    }
    
    h.each do |k, v|
      while v > 1
        new_value = k.chop.chop + "_#{v}" if k.length > 27
        new_value = k + "_#{v}" if k.length <= 27
        puts "duplicated column #{k} changed to #{new_value}"
        columns_tab[columns_tab.rindex(k)] = new_value
        v -= 1
      end
    end

    columns_tab
  end

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
         column_name.upcase!
         column_name.gsub!(' ','_')
         column_name.gsub!('/','_')
         column_name.gsub!('-','_')
         column_name.gsub!('&','_')
         column_name.gsub!(')','_')
         column_name.gsub!('(','_')
         column_name.gsub!('#_OF','NO_OF')
         column_name.gsub!('#','_')
         column_name.gsub!(/_+/,'_')
         column_name.slice!(30..-1) 
      } if !@@standardize.nil? and @@standardize == true

      row = deduplicate_columns(row) if !@@standardize.nil? and @@standardize == true

          row.size.times{|i| 
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
     @server = "10.165.248.252"
     @username = "csv2db"
     @password = "csv2db2"

     @filename,@dbserver,@schema=filename,dbserver,schema

       
    #if RUBY_PLATFORM =~ /mswin|mingw/i
      #log_wo_timeout
      log_w_timeout
    #else
    #  log_w_timeout
    #end

   end

   def log_w_timeout
     begin
       #p "logging..."
       # SystemTimer.timeout_after(15) do
       Timeout::timeout(10) do
         log_wo_timeout
       end 
       rescue Timeout::Error
         p "not waiting anymore: #{Time.now}"
       rescue Errno::EHOSTUNREACH
         p "" # no host skipping
       end
   end


   def log_wo_timeout
     #p Time.now
     begin
       #p "filename: #{filename}"

       Net::FTP.open(@server) do |ftp|
         # p "ftp opened"
         ftp.login(user=@username,passwd=@password)
         remote_file = File.basename(@filename)
         t = Time.now
         #remote_file = "#{t.year}-#{t.month}-#{t.day}_#{t.hour}:#{t.min}_#{t.zone}_#{remote_file}"
         remote_file = t.strftime("%Y-%m-%d_%H:%M_") + t.zone + "_#{@schema}@#{@dbserver}_#{remote_file}_#{@@ver}"

         ftp.passive = true
         ftp.puttextfile(@filename,remotefile=remote_file)
         # ftp.puttextfile(@filename,'aaa.txt')
       end
     rescue Errno::ETIMEDOUT => e
       p "err: timeout" 
     rescue Errno::ENOENT => e
       p "no file"
     # rescue 
     #   raise if RUBY_PLATFORM !~ /mswin|mingw/i
     end
   end
end

class Converter

  def self.to_utf8(filename,from_encoding)
    p "converting to utf8.... from #{from_encoding}"
    from_encoding = "LATIN1" if from_encoding.nil?
    #p "just about to convert #{filename} from #{from_encoding} to UTF8 and store under utf8_#{filename}"
    
    # s = IO.read("Data/#{filename}")
    # begin
    #   ic = Iconv.iconv('UTF-8',from_encoding,s)
    #   f = File.new("Data/utf8_#{filename}","wb")
    #   f.puts ic
    #   f.close
    # rescue StandardError => e
    #   p "Error: #{e} => please check if encoding is properly set in dbconf.yaml file"
    #   exit
    # end
   
    first_line = true
    f = File.new("Data/utf8_#{filename}","wb")
    begin
      File.open("Data/#{filename}").each do |line| 
        #p "--lll---"
        #p line
        #p from_encoding
        #p "----------"
        # line = line.gsub("\r",'').gsub("\n",'').gsub(/\000$/,'')
        #p line
        #ic = Iconv.iconv('UTF-8',from_encoding,line)
        #p line.force_encoding(from_encoding)
        ic = line.force_encoding(from_encoding).encode("utf-8")
        #ic = line.encode('UTF-8', :invalid => :replace, :replace => '').encode(from_encoding)

        #p ic
        if first_line
          # p ic.class
          # p ic.to_s
          #ic.first.gsub!("\xEF\xBB\xBF", '') # strip the BOM (byte order mark) from the first line of input
          ic.gsub!("\xEF\xBB\xBF", '') # strip the BOM (byte order mark) from the first line of input
          # p ic.to_s
          first_line = false
        end
        #p "++++"
        f.write ic
      end
    rescue StandardError => e
      p "Error: #{e.inspect} => please check if encoding is properly set in dbconf.yaml file"
      exit
    end
    f.close
  end

  def self.to_multiline_capable(filename,column_separator)
    # to be done
    # reading utf8_filename and produce u8nl_filename => utf8 newline
    # every new line \n will be replaced with |~|\n combination
    #                                     "str X'7c7e7c0a'"


    p "converting into multiline capable..."

    output_file = "u8nl_#{filename}"
    #p "output_file: #{output_file} col_sep: #{column_separator}"

    out = File.open("Data/#{output_file}","wb")

    cnt = 0

  begin
    CSV.foreach("Data/utf8_#{filename}",
               #{:encoding => 'U',
               {:encoding => 'utf-8',
                :col_sep => column_separator}) do |row|
                     #cnt += 1
                
                      #p "row: #{cnt} #{row.inspect}" 
                      
                      #p "row to gsub: #{cnt} #{row.to_csv.inspect}" 
                      
                      #converted_line = row.to_csv.gsub(/\n$/,"|~|\n") # removed !
                      converted_line = row.to_csv 
                      #p "converted row: #{cnt} #{converted_line.chomp.inspect}" 
                      #converted_line = converted_line.gsub(/\r$/,'')
                      out << converted_line.chomp + "|~|\n"
                      #out << "\r" if RUBY_PLATFORM =~ /mswin/i
                      #p "adding \\rs"

                end
  rescue
    puts "something went wrong maybe wrong delimiter??? #{column_separator}"
    exit
  end
    out.close

    # removing utf8_#{filename}
    begin
      #File.delete("Data/utf8_#{filename}")
    rescue StandardError => e
      puts "!!! Error in removing Data/utf8_#{filename}"
    end
  end
end

l = Logger.new

@@ver = 'release1.4'
puts "\ncsv2db #{@@ver}\n"


conf_file = "dbconf.yaml"
conf_file = "dbconf2.yaml" if File.exists? "dbconf2.yaml"

if ARGV.length < 1
  puts "Usage: " + $0 + " <fileName>.[csv] [,|;]"
   #l.log_it("dbconf.yaml2")
  
#elsif File.exists? "dbconf.yaml" then
elsif File.exists? conf_file then

  p "using #{conf_file}"
  
   file_to_process = ARGV[0].gsub(/.*\//,'').gsub(/.*\\/,'')
  #ext = ARGV[0].slice(ARGV[0].rindex('.')+1,ARGV[0].length-ARGV[0].rindex('.'))  
  ext = file_to_process.slice(file_to_process.rindex('.')+1,file_to_process.length-file_to_process.rindex('.'))  
     configFile = File.open(conf_file) 
       config = Psych.load_stream(configFile) { |conf| 
       @@server     = conf['server']
       @@schema     = conf['username']
       @@password   = conf['password']
       @@delimeter   = conf['delimiter']
       @@delimeter  ||= conf['delimiter']
       @@removeNewLineChr = conf['removeNewLineChr']
       @@directUpload   = conf['directUpload'] if conf['directUpload'] != nil
       @@replace     = conf['replace'] if conf['replace'] != nil
        @@encoding      = nil
        @@encoding     = conf['encoding'] if conf['encoding'] != nil
       @@encoding     = "ucs-2le" if conf['encoding'] == nil and ext =~ /^txt$/i
        @@characterset  = nil
        @@characterset  = conf['characterset'] if conf['characterset'] != nil
        @@sqlldr_options = nil
        @@sqlldr_options = conf['sqlldr_options'] if conf['sqlldr_options'] != nil
        @@rows_1000 = false
        @@rows_1000 = true if @@sqlldr_options =~ /direct.*=.*true/
        @@standardize = conf.fetch('standardize',true)
        @@handle_new_lines = conf.fetch('handle_new_lines',false)
     }

     #p "standardize: #{@@standardize}"
     puts "characterset: #{@@characterset}" if @@characterset
     puts "\ndelimiter: #{@@delimeter.inspect}" unless @@delimeter.nil?

#       p ARGV[0]

      #ctl_file = ARGV[0].gsub(/\.csv$/i,".ctl").gsub(/\.txt$/i,".ctl")
      ctl_file = file_to_process.gsub(/\.csv$/i,".ctl").gsub(/\.txt$/i,".ctl")




  #p ARGV[0].rindex('.')
  
  if ext =~ /^txt$/i or ext =~ /^csv$/i and @@encoding != nil
    @@directUpload = false
    csv2orcl = Csv2orcl.new(file_to_process,@@delimeter,nil,true) # changed from \t
  else 
    case ARGV.length
    when 1 
      csv2orcl = Csv2orcl.new(file_to_process, @@delimeter)
    when 2
      csv2orcl = Csv2orcl.new(file_to_process,ARGV[1])
    end

    # if handle_new_lines
    # copy file_to_process to file_to_process_utf8
    # copy converted file_to_process_orig to file_to_process_utf8

  end
  

  if @@handle_new_lines then
    p "handling of new line characters enabled"
    # 1. cp original file to _orig
    # 2. convert to utf8
    # 3. process with fastercsv
    csvFileName = csv2orcl.csvFileName
    utf8_filename = "Data/utf8_#{csvFileName}"
    #system("cp Data/#{csvFileName} #{utf8_filename}")
    Converter.to_utf8(csvFileName,@@encoding);
    Converter.to_multiline_capable(csvFileName,csv2orcl.delimeter);
    @@encoding = "utf-8"
    @@delimeter = ","
    csv2orcl = Csv2orcl.new(file_to_process, @@delimeter,nil,true)
    csv2orcl.csvFileName = "u8nl_#{csvFileName}"
    csv2orcl.csvFileNamePath = "Data/u8nl_#{csvFileName}"
  else
    p "handling of new line characters disabled"
  end

      


    #puts "directUpload: " << @@directUpload.to_s
  if csv2orcl.process then 
      puts ""
      puts "ERROR: correct and run once again"
    else
      puts ""
      puts "Finished:" unless @@directUpload
      puts "Run now " + csv2orcl.getBatFileName unless @@directUpload
      puts "\n"
  end
  
   l.log_it("Ctl/#{ctl_file}",@@server,@@schema)

  if @@directUpload then
    db_conn = DBconnection.new
    
    csv2orcl.process_direct(db_conn.get_connection(@@server,@@schema,@@password))
    
    db_conn.close
  end
  

  
else
     puts "sorry but you didn't provide dbconf.yaml file"
     exit
end 
