#!/usr/bin/env ruby

# Take a folder of files and prune it down to the information used in analysis
# Takes as input
# - a file that maps search queries to candidates
# - a folder that contains twitter search results
# - a folder to write the result to

require 'optparse'
require 'json'
require 'fileutils'
require 'rubygems'
require 'backports/1.9.1/kernel/require_relative' if RUBY_VERSION =~ /1\.8/
require_relative 'config_parser'

def run_cmd(cmd)
  puts "#{cmd}"
  `#{cmd}`
end

def filter_path
  File.join(File.dirname(__FILE__), "..", "jq", "twitter_prune.jq")
end

def prune_compress_path
  File.join(File.dirname(__FILE__), "..", "jq", "prune_compress.jq")
end

def prune(candidates, rundir, outdir)
  jq = "jq"
  files = File.join(rundir, "*")
  base_prune = "#{jq} -c -f #{filter_path} #{files}"
  uniquify = "#{jq} -c -s --argjson namemap '#{candidates}' -f #{prune_compress_path}"
  FileUtils.mkdir_p(outdir)
  outpath = File.join(outdir, File.basename(rundir) + ".json")

  cmd = "#{base_prune} | #{uniquify} > #{outpath}"
  run_cmd(cmd)
end

options = {}
parser = OptionParser.new do |opts|
  opts.banner = "Usage: prune.rb config"
end

parser.parse!
if ARGV.empty?
  puts parser
  exit(-1)
end

if ARGV.length < 1
  puts parser
  exit(-1)
end

config = SmetConfigParser.parse(ARGV[0])
config.tasks.each do | task |
  prune(config.candidates_map, task.inpath, task.outfolder)
end
