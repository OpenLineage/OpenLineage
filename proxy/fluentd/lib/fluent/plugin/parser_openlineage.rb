require "fluent/plugin/parser"
require 'json'
require "rusty_json_schema"
require 'uri'
require 'net/http'

module Fluent
  module Plugin
    class OpenlineageParser < Fluent::Plugin::JSONParser
      Fluent::Plugin.register_parser("openlineage", self)

      DEFAULT_SCHEMA="https://openlineage.io/spec/1-0-5/OpenLineage.json"

      @@cached_schemas = {}

      # https://docs.fluentd.org/plugin-development/api-plugin-parser
      def parse(text)
        # parse JSON with default JSONParser
        super(text) { | time, json |
          validate_openlineage(json)
          return json
        }
      end

      def validate_openlineage(json)
        if json == nil
          raise ParserError, "Openlineage validation failed: invalid json provided"
        end
        schema_url = json["schemaURL"] ? json["schemaURL"] : DEFAULT_SCHEMA

        # https://github.com/driv3r/rusty_json_schema
        # Rust json parser ported to ruby that supports Draft 2020-12
        validator = RustyJSONSchema.build(get_schema(schema_url))
        errors = validator.validate(json)

        if !errors.empty?
          raise ParserError, "Openlineage validation failed: " + errors.join(", ")
        end
      end

      def get_schema(schema_url)
        if !@@cached_schemas.key?(schema_url)
          response = Net::HTTP.get_response(URI(schema_url))
          case response
          when Net::HTTPSuccess then
            @@cached_schemas[:schema_url] = response.body
          else
            raise ParserError, "Openlineage validation failed, unable to fetch schema " + schema_url
          end
        end
        return @@cached_schemas[:schema_url]
      end
    end
  end
end
