require "helper"
require "fluent/plugin/parser_openlineage.rb"

class OpenlineageParserTest < Test::Unit::TestCase
  setup do
    Fluent::Test.setup
    @parser = Fluent::Test::Driver::Parser.new(Fluent::Plugin::OpenlineageParser)
    @parser.configure(
      'json_parser' => 'yajl',
      'spec_directory' => '../../spec/'
    )
  end

 test "test event with no runId" do
   ol_event = File.read("events/event_no_run_id.json")

   err = assert_raise Fluent::ParserError do
     @parser.instance.parse(ol_event)
   end
   assert_match "\"runId\" is a required property", err.message
 end

  test "test invalid json" do
    assert_raise Fluent::ParserError do
      @parser.instance.parse('{"run": Not a JSON}')
    end
  end

  test "event full test" do
    @parser.configure(
      'json_parser' => 'yajl',
      'spec_directory' => '../../spec/',
    )
    ol_event = File.read("events/event_full.json")
    @parser.instance.parse(ol_event) { | time, json |
       assert_equal("ea041791-68bc-4ae1-bd89-4c8106a157e4", json['run']['runId'])
    }
  end

  test "event simple test" do
    ol_event = File.read("events/event_simple.json")
    @parser.instance.parse(ol_event) { | time, json |
      assert_equal("41fb5137-f0fd-4ee5-ba5c-56f8571d1bd7", json['run']['runId'])
    }
  end

 test "invalid spec_directory test" do
   assert_raise Fluent::ParserError do
     @parser.configure(
       'json_parser' => 'yajl',
       'spec_directory' => './non-existent-spec/'
     )
   end
 end

  test "valid spec_directory without slash" do
    @parser.configure(
      'json_parser' => 'yajl',
      'spec_directory' => '../../spec'
    )
    ol_event = File.read("events/event_simple.json")
    @parser.instance.parse(ol_event) { | time, json |
      assert_equal("41fb5137-f0fd-4ee5-ba5c-56f8571d1bd7", json['run']['runId'])
    }
  end

  test "run facet validation" do
    ol_event = File.read("events/event_invalid_run_facet.json")
    err = assert_raise Fluent::ParserError do
      @parser.instance.parse(ol_event)
    end
    assert_match "\"runId\" is a required property", err.message
  end

  test "run facet validation turned off" do
    ol_event = File.read("events/event_invalid_run_facet.json")
    @parser.configure(
      'json_parser' => 'yajl',
      'spec_directory' => '../../spec/',
      'validate_run_facets' => false,
    )
    @parser.instance.parse(ol_event) { | time, json |
      assert_equal("41fb5137-f0fd-4ee5-ba5c-56f8571d1bd7", json['run']['runId'])
    }
  end

  test "job facet validation" do
    ol_event = File.read("events/event_invalid_job_facet.json")
    err = assert_raise Fluent::ParserError do
      @parser.instance.parse(ol_event)
    end
    assert_match "\"/job/ownership/owners/0\": \"name\" is a required property", err.message
  end

  test "job facet validation turned off" do
    ol_event = File.read("events/event_invalid_job_facet.json")
    @parser.configure(
      'json_parser' => 'yajl',
      'spec_directory' => '../../spec/',
      'validate_job_facets' => false,
    )
    @parser.instance.parse(ol_event) { | time, json |
      assert_equal("41fb5137-f0fd-4ee5-ba5c-56f8571d1bd7", json['run']['runId'])
    }
  end

  test "dataset facet validation" do
    ol_event = File.read("events/event_invalid_dataset_facet.json")
    err = assert_raise Fluent::ParserError do
      @parser.instance.parse(ol_event)
    end
    assert_match "path \"/outputs/0/ownership/owners/0\": \"name\" is a required property", err.message
  end

  test "dataset facet validation turned off" do
    ol_event = File.read("events/event_invalid_dataset_facet.json")
    @parser.configure(
      'json_parser' => 'yajl',
      'spec_directory' => '../../spec/',
      'validate_dataset_facets' => false,
    )
    @parser.instance.parse(ol_event) { | time, json |
      assert_equal("41fb5137-f0fd-4ee5-ba5c-56f8571d1bd7", json['run']['runId'])
    }
  end

  test "input dataset facet validation" do
    ol_event = File.read("events/event_invalid_input_dataset_facet.json")
     @parser.configure(
      'json_parser' => 'yajl',
      'spec_directory' => '../../spec/',
      'validate_input_dataset_facets' => true,
    )
    err = assert_raise Fluent::ParserError do
      @parser.instance.parse(ol_event)
    end
    assert_match "DataQualityMetricsInputDatasetFacet", err.message
  end

  test "output dataset facet validation" do
    ol_event = File.read("events/event_invalid_output_dataset_facet.json")
    err = assert_raise Fluent::ParserError do
      @parser.instance.parse(ol_event)
    end
    assert_match "OutputStatisticsOutputDatasetFacet", err.message
  end

  test "output dataset facet validation turned off" do
    ol_event = File.read("events/event_invalid_output_dataset_facet.json")
    @parser.configure(
      'json_parser' => 'yajl',
      'spec_directory' => '../../spec/',
      'validate_output_dataset_facets' => false,
    )
    @parser.instance.parse(ol_event) { | time, json |
      assert_equal("41fb5137-f0fd-4ee5-ba5c-56f8571d1bd7", json['run']['runId'])
    }
  end

  private

  def create_driver(conf)
    Fluent::Test::Driver::Parser.new(Fluent::Plugin::OpenlineageParser).configure(conf)
  end
end
