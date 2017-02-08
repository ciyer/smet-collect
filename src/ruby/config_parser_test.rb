require_relative 'config_parser'
require 'test/unit'
require 'test/unit/ui/console/testrunner'
require 'json'


class SmetConfigParserTest < Test::Unit::TestCase
	def setup

	end

	def teardown

	end

	def config_file_path()
	  File.join(File.dirname(__FILE__), "..", "scala", "src", "test", "resources", "spark_driver_config.json")
	end

	def test_parser
		config = SmetConfigParser.parse(config_file_path())
		candidates_map = JSON.parse(config.candidates_map)
		assert candidates_map.size > 0, "There should be some candidates"
		assert candidates_map["Hillary"] == "Hillary Clinton", "Search term 'Hillary' should map to candidate 'Hillary Clinton'"

		assert config.tasks.size > 0, "There should be some tasks"
		assert config.tasks[0].task_json["inpath"] == "path/inputr"
	end

end
