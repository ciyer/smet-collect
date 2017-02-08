#!/usr/bin/env ruby

# Take a pruned run file and summarize the hashtags contained therein.
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
  File.join(File.dirname(__FILE__), "..", "jq", "twitter_hashtags.jq")
end

def accumulate(tag, candidate, entry, key)
  candidate[tag][key] = candidate[tag][key] + entry[key] unless entry[key].nil?
end

def hashtag_counts(summary, run_name)
  candidates = Hash.new do |h, k|
    h[k] = Hash.new { |h1, k1| h1[k1] = {'tag' => k1, 'rtc' => 0, 'rt_rtc' => 0, 'fav' => 0, 'rt_fav' => 0} }
  end
  summary.each do | s |
    c = candidates[s['candidate']]
    s['hashtags'].each do | t |
      tag = t.downcase
      accumulate(tag, c, s, 'rtc')
      accumulate(tag, c, s, 'rt_rtc')
      accumulate(tag, c, s, 'fav')
      accumulate(tag, c, s, 'rt_fav')
    end
  end
  result = []
  candidates.each do | candidate, counts |
    summary = {'name' => candidate, 'runname' => run_name}
    sorted = counts.sort_by {|k, v| -v['rt_fav']}
    tag_counts = []
    sorted.each do | k, c |
      tag_counts.push(c) if c['rtc'] > 0 && c['rt_rtc'] > 0 && c['fav'] > 0 && c['rt_fav'] > 0
    end
    summary['counts'] = tag_counts
    result.push(summary)
  end
  result
end

def summarize(run_path, outdir)
  return unless File.file? run_path

  jq = "jq"
  run_name = File.basename(run_path)
  # drop the '.json' from the run name before passing to jq filter
  base_summary = "#{jq} -c --arg runname #{run_name[0..-6]} -f #{filter_path} #{run_path}"
  outpath = File.join(outdir, run_name)

  cmd = "#{base_summary}"
  result_json = run_cmd(cmd)
  result = JSON.parse(result_json)
  result = hashtag_counts(result, run_name[0..-6])
  File.open(outpath, "w") do |f|
    f.write(result.to_json)
  end
end

options = {}
parser = OptionParser.new do |opts|
  opts.banner = "Usage: hashtags.rb config"
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
