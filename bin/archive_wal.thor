# module: postgresql

require 'rubygems'
require 'bzip2'
require 'fileutils'
require 'right_aws'
require 'thor'

class ArchiveWal < Thor  
  desc "archive PATH FILENAME", "archive the WAL file to S3"
  method_options :aws_access_key => :string, :aws_secret_access_key => :string, :s3_bucket => :string, :s3_path => :string
  def archive(path, filename)
    puts "running"
    parse_options
    
    compressed_filename = "#{filename}.bz2"
    tmpfile = "/tmp/#{compressed_filename}"
    
    compress_file(path, tmpfile)
    upload_file(tmpfile, compressed_filename)
    File.delete(tmpfile)
  end
  
  desc "restore FILENAME PATH", "restore the WAL file from s3"
  def restore(filename, path)
    parse_options
    compressed_filename = "#{filename}.bz2"
    tmpfile = "/tmp/#{compressed_filename}"

    download_file(compressed_filename, tmpfile)
    uncompress_file(tmpfile, path)
    File.delete(tmpfile)
  end


  private
  def compress_file(src, dest)
    ins = File.open(src, "r")
    outs = Bzip2::Writer.open(dest, "w")
   
    FileUtils.copy_stream ins, outs
    
    outs.close 
    ins.close
  end
  
  def uncompress_file(src, dest)
    ins = Bzip2::Reader.open(src, "r")
    outs = File.open(dest, "w")
    
    FileUtils.copy_stream ins, outs 
    
    outs.close
    ins.close
  end
  
  def upload_file(src, filename)
    ins = File.open(src, "r")
    
    @s3.put(@s3_bucket, s3_key(filename), ins)
    
    ins.close
  end
  
  def download_file(filename, dest)
    outs = File.open(dest, "w")
    
    @s3.get(@s3_bucket, s3_key(filename)) do |chunk|
      outs.write(chunk)
    end
    
    outs.close
  end
  
  def s3_key(filename)
    File.join(@s3_prefix, filename)
  end
  
  def parse_options
    config_file = File.expand_path("~/.archive_wal.yml")
    puts "Config File: #{config_file}"
    puts "Exists #{File.exists?(config_file)}"
    config = File.exists?(config_file) ? YAML.load_file(config_file) : {}
    
    aws_access_key = options[:aws_access_key] || ENV['AWS_ACCESS_KEY'] || config['AWS_ACCESS_KEY']
    aws_secret_access_key = options[:aws_secret_access_key] || ENV['AWS_SECRET_ACCESS_KEY'] || config['AWS_SECRET_ACCESS_KEY']
    
    puts "AK: #{aws_access_key}"
    puts "AK: #{config['AWS_ACCESS_KEY']}"
    
    @s3 = RightAws::S3Interface.new(aws_access_key, aws_secret_access_key, {:multi_thread => true}) 
    @s3_bucket = options[:s3_bucket] || config['S3_BUCKET']
    @s3_prefix = options[:s3_path] || config['S3_KEY_PREFIX']
  end
  
end



