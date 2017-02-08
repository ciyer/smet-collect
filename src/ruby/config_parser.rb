#!/usr/bin/env ruby
require 'json'

# Helper class for configuration files that describe processing tasks.

class SmetConfigParser

  def self.parse(path)
    json_str = File.read path
    config_json = JSON.parse(json_str)
    SmetConfigParser.new(config_json).run
  end

  def initialize(config_json)
    @json = config_json
    @candidates_map = {}
    @tasks = []
  end

  def run
    parse_candidates
    parse_tasks
    SmetConfig.new(JSON.dump(@candidates_map), @tasks)
  end

  def encoded_search_term(t)
    t.gsub("'", "%27").gsub("@", "%40").gsub(" ", "+").gsub('#', "%23")
  end

  def encoded_name(n)
    n.gsub("'", "\\'")
  end

  def parse_candidates
    @json["races"].each do | race |
      race['candidates'].each do | c |
        c['terms'].each { | t | @candidates_map[encoded_search_term(t)] = encoded_name(c['name'])}
      end
    end
  end

  def parse_tasks
    @json["tasks"].each do | task |
      @tasks.push(SmetTask.new(task))
    end
  end

end

class SmetConfig
  attr_accessor :candidates_map, :tasks
  def initialize(candidates_map, tasks)
    @candidates_map = candidates_map
    @tasks = tasks
  end
end

class SmetTask
  attr_accessor :task_json
  def initialize(task_json)
    @task_json = task_json
  end

  def inpath
    @task_json['inpath']
  end

  def outfolder
    @task_json['outfolder']
  end
end
