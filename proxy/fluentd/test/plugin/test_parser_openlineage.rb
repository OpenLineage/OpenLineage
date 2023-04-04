require "helper"
require "fluent/plugin/parser_openlineage.rb"

class OpenlineageParserTest < Test::Unit::TestCase
  setup do
    Fluent::Test.setup
    @parser = Fluent::Test::Driver::Parser.new(Fluent::Plugin::OpenlineageParser)
  end

 test "test event with no runId" do
    @parser.configure('json_parser' => 'yajl')
    ol_event = File.read("events/event_no_run_id.json")

    assert_raise Fluent::ParserError do
      @parser.instance.parse(ol_event)
    end
  end

  test "test invalid json" do
      @parser.configure('json_parser' => 'yajl')
      assert_raise Fluent::ParserError do
        @parser.instance.parse('{"run": Not a JSON}')
      end
    end

  test "event full test" do
      @parser.configure('json_parser' => 'yajl')
      ol_event = File.read("events/event_full.json")
      parsed = @parser.instance.parse(ol_event)
      assert_equal("ea041791-68bc-4ae1-bd89-4c8106a157e4", parsed['run']['runId'])
  end

  test "event simple test" do
      @parser.configure('json_parser' => 'yajl')
      ol_event = File.read("events/event_simple.json")
      parsed = @parser.instance.parse(ol_event)
      assert_equal("41fb5137-f0fd-4ee5-ba5c-56f8571d1bd7", parsed['run']['runId'])
  end

 test "invalid schemaURL test" do
      @parser.configure('json_parser' => 'yajl')
      ol_event = File.read("events/event_with_invalid_schema_url.json")
      assert_raise Fluent::ParserError do
        @parser.instance.parse(ol_event)
      end
  end

  private

  def create_driver(conf)
    Fluent::Test::Driver::Parser.new(Fluent::Plugin::OpenlineageParser).configure(conf)
  end
end
