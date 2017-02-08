#!/usr/bin/env ruby

# Take a pruned run file and summarize the metadata contained therein.
# Takes as input
# - a file with search results from a run
# - a folder to write the result to

require 'optparse'
require 'json'
require 'rubygems'
require 'backports/1.9.1/kernel/require_relative' if RUBY_VERSION =~ /1\.8/
require_relative 'config_parser'

def run_cmd(cmd)
  puts "#{cmd}"
  `#{cmd}`
end

def filter_path
  File.join(File.dirname(__FILE__), "..", "jq", "twitter_summary.jq")
end

def summarize(run_path, outdir)
  jq = "jq"
  run_name = File.basename(run_path)
  # drop the '.json' from the run name before passing to jq filter
  base_summary = "#{jq} -c --arg runname #{run_name[0..-6]} -f #{filter_path} #{run_path}"
  outpath = File.join(outdir, run_name)

  cmd = "#{base_summary} > #{outpath}"
  run_cmd(cmd)
end

options = {}
parser = OptionParser.new do |opts|
  opts.banner = "Usage: mdsummary.rb config"
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
  summarize(task.inpath + ".json", task.outfolder)
end
