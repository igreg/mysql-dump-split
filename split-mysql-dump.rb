#!/usr/bin/env ruby
 
require 'optparse'
require 'zlib'
 
tables = []
ignore = []
dumpfile = ""
@compress = false

def new_outfile(filename)
  if @compress
    file = Zlib::GzipWriter.open("#{filename}.gz")
  else
    file = File.new(filename, "w")
  end
  # It is always a good idea to update these variables before we start
  file.puts("/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;")
  file.puts("/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;")
  file.puts("/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;")
  file.puts("/*!40101 SET NAMES utf8 */;")
  file.puts("/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;")
  file.puts("/*!40103 SET TIME_ZONE='+00:00' */;")
  file.puts("/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;")
  file.puts("/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;")
  file.puts("/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;")
  file.puts("/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;")
  file
end

def close_outfile(file)
  if file and !file.closed?
    # It is always a good idea to restore these variables once we're done
    file.puts("/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;")
    file.puts("/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;")
    file.puts("/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;")
    file.puts("/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;")
    file.puts("/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;")
    file.puts("/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;")
    file.puts("/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;")
    file.close
  end
end

cmds = OptionParser.new do |opts|
  opts.banner = "Usage: split-mysql-dump.rb [options] [FILE]"

  opts.on("-s", "Read from stdin") do
  dumpfile = $stdin
  end
  
  opts.on("-t", '--tables TABLES', Array, "Extract only these tables") do |t|
    tables = t
  end
  
  opts.on("-i", '--ignore-tables TABLES', Array, "Ignore these tables") do |i|
    ignore = i
  end

  opts.on("-c", '--compress', Array, "Compress split files with Gzip") do
    @compress = true
  end
  
  opts.on_tail("-h", "--help") do
  puts opts
  end

end.parse!

if dumpfile == ""
  dumpfile = ARGV.shift
  if not dumpfile
    puts "Nothing to do"
    exit 
  end
end

STDOUT.sync = true

if File.exist?(dumpfile)
  if dumpfile == $stdin
    d = $stdin
  else
    d = File.new(dumpfile, "r")
  end
 
  outfile = nil
  table = nil
  db = nil
  linecount = tablecount = starttime = 0
 
  while (line = d.gets)
    # Detect table changes
    if line =~ /^-- Table structure for table .(.+)./ or line =~ /^-- Dumping data for table .(.+)./
      is_new_table = table != $1
      table = $1

      # previous file should be closed
      if is_new_table
        close_outfile(outfile)

        puts("\n\nFound a new table: #{table}")

        if (tables != [] and not tables.include?(table))
          puts"`#{table}` not in list, ignoring"
          table = nil
        elsif (ignore != [] and ignore.include?(table))
          puts"`#{table}` will be ignored"
          table = nil
        else
          starttime = Time.now
          linecount = 0
          tablecount += 1
          outfile = new_outfile("#{db}_#{table}.sql")
          outfile.write("USE `#{db}`;\n\n")
        end
      end
    elsif line =~ /^-- Current Database: .(.+)./
      db = $1
      table = nil
      close_outfile(outfile)
      outfile = new_outfile("#{db}_1create.sql")
      puts("\n\nFound a new db: #{db}")
    elsif line =~ /^-- Position to start replication or point-in-time recovery from/
      db = nil
      table = nil
      close_outfile(outfile)
      outfile = new_outfile("1replication.sql")
      puts("\n\nFound replication data")
    end
 
    # Write line to outfile
    if outfile and !outfile.closed?
      outfile.write(line)
      linecount += 1
      elapsed = Time.now.to_i - starttime.to_i + 1
      print("    writing line: #{linecount} in #{elapsed} seconds                 \r")
    end
  end
end

# Let's not forget to close the file
close_outfile(outfile)
 
puts
