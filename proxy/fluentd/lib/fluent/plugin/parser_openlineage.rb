require "fluent/plugin/parser"
require 'fluent/plugin/parser_json'
require 'json'
require "rusty_json_schema"

module Fluent
  module Plugin
    class OpenlineageParser < Fluent::Plugin::JSONParser
      Fluent::Plugin.register_parser("openlineage", self)

      DEFAULT_SPEC_DIRECTORY="/etc/spec"

      def configure(conf)
        if conf.has_key?('spec_directory')
          @spec_directory = conf['spec_directory']
        else
          @spec_directory = DEFAULT_SPEC_DIRECTORY
        end
        if (not @spec_directory.end_with?("/"))
          @spec_directory += "/"
        end
        @validate_input_dataset_facets = conf.fetch('validate_input_dataset_facets', false)
        @validate_output_dataset_facets = conf.fetch('validate_output_dataset_facets', true)
        @validate_dataset_facets = conf.fetch('validate_dataset_facets', true)
        @validate_run_facets = conf.fetch('validate_run_facets', true)
        @validate_job_facets = conf.fetch('validate_job_facets', true)
        @validator = RustyJSONSchema.build(load_schema())
        super
      end

      # https://docs.fluentd.org/plugin-development/api-plugin-parser
      def parse(text)
        # parse JSON with default JSONParser
        super(text) { | time, json |
          validate_openlineage(json)
          yield time, json
        }
      end

      private


      def validate_openlineage(json)
        if json == nil
          raise ParserError, "Openlineage validation failed: invalid json provided"
        end

        # https://github.com/driv3r/rusty_json_schema
        # Rust json parser ported to ruby that supports Draft 2020-12
        errors = @validator.validate(json)

        if !errors.empty?
          raise ParserError, "Openlineage validation failed: " + errors.join(", ")
        end
      end

      def load_schema()
        schemaFile = @spec_directory + "/OpenLineage.json"

        if (not File.exist?(schemaFile))
          raise ParserError, "Couldn't find Openlineage.json file within a defined spec directory: " + schemaFile
        end

        schema = File.read(schemaFile)
        schema = rewrite_schema_to_include_facets(schema)
        return schema
      end

      # Current Openlineage schema contains references to facets' definitions stored in files
      # in facets directory which are not valid schemas for json_schema.
      # In this step we rewrite Openlineage schema to contain facets definitions within it
      def rewrite_schema_to_include_facets(schema)
        # replace all the refs in schema to local refs
        # "facets/ColumnLineageDatasetFacet.json" -> "#/defs/ColumnLineageDatasetFacet"
        schema = schema.gsub(
          /"facets\/([a-zA-Z]+)\.json"/,
          '"#/$defs/\1"'
        )
        schema_json = JSON.parse(schema)
        facets_path = @spec_directory + "facets/"

        # dataset facets
        if @validate_dataset_facets
          schema_json \
            ["$defs"] \
            ["Dataset"] \
            ["properties"] \
            ["facets"] \
            ["anyOf"].select {|el| el.key?("$ref") }.each { | facet_ref |
              facet_name =  facet_ref["$ref"].gsub("#/$defs/", "")
              facet_schema = JSON.parse(File.read(facets_path + facet_name + ".json"))
              property_name = facet_schema["properties"].keys[0]
              schema_json["$defs"]["Dataset"]["properties"][property_name] = {
                "$ref" => "#/$defs/" + facet_name
            }
          }
        end

        # run facets
        if @validate_run_facets
          schema_json \
            ["$defs"] \
            ["Run"] \
            ["properties"] \
            ["facets"] \
            ["anyOf"].select {|el| el.key?("$ref") }.each { | facet_ref |
              facet_name =  facet_ref["$ref"].gsub("#/$defs/", "")
              facet_schema = JSON.parse(File.read(facets_path + facet_name + ".json"))
              property_name = facet_schema["properties"].keys[0]
              schema_json["$defs"]["Run"]["properties"][property_name] = {
                "$ref": "#/$defs/" + facet_name
            }
          }
        end

        # job facets
        if @validate_job_facets
          schema_json \
            ["$defs"] \
            ["Job"] \
            ["properties"] \
            ["facets"] \
            ["anyOf"].select {|el| el.key?("$ref") }.each { | facet_ref |
              facet_name =  facet_ref["$ref"].gsub("#/$defs/", "")
              facet_schema = JSON.parse(File.read(facets_path + facet_name + ".json"))
              property_name = facet_schema["properties"].keys[0]
              schema_json["$defs"]["Job"]["properties"][property_name] = {
                "$ref": "#/$defs/" + facet_name
              }
          }
        end

        # input dataset facets
        if @validate_input_dataset_facets
          schema_json \
            ["$defs"] \
            ["InputDataset"] \
            ["allOf"].select {|el| el.key?("type") } \
            [0] \
            ["properties"] \
            ["inputFacets"] \
            ["anyOf"].select {|el| el.key?("$ref") }.each { | facet_ref |
              facet_name =  facet_ref["$ref"].gsub("#/$defs/", "")
              facet_schema = JSON.parse(File.read(facets_path + facet_name + ".json"))
              property_name = facet_schema["properties"].keys[0]

              input_dataset = schema_json["$defs"]["InputDataset"]["allOf"]
                .select {|el| el.key?("type") }[0]["properties"]["inputFacets"]["anyOf"]
                .select {|el| el.key?("type") }[0]
              if not input_dataset.key?("properties")
                input_dataset["properties"] = {}
              end
              input_dataset["properties"][property_name] = {  "$ref" => "#/$defs/" + facet_name }
          }
        end

        # output dataset facets
        if @validate_output_dataset_facets
          schema_json \
            ["$defs"] \
            ["OutputDataset"] \
            ["allOf"].select {|el| el.key?("type") } \
            [0] \
            ["properties"] \
            ["outputFacets"] \
            ["anyOf"].select {|el| el.key?("$ref") }.each { | facet_ref |
              facet_name =  facet_ref["$ref"].gsub("#/$defs/", "")
              facet_schema = JSON.parse(File.read(facets_path + facet_name + ".json"))
              property_name = facet_schema["properties"].keys[0]

              output_dataset = schema_json["$defs"]["OutputDataset"]["allOf"]
                .select {|el| el.key?("type") }[0]["properties"]["outputFacets"]["anyOf"]
                .select {|el| el.key?("type") }[0]

              if not output_dataset.key?("properties")
                output_dataset["properties"] = {}
              end
              output_dataset["properties"][property_name] = {  "$ref" => "#/$defs/" + facet_name }
          }
        end

        # list all the facets
        Dir.entries(facets_path).each { |facet_file|
          if facet_file.end_with?(".json")
            facet_schema =  JSON.parse(File.read(facets_path + facet_file))
            # include facets' definitions within schema
            schema_json["$defs"] = schema_json["$defs"].merge(facet_schema["$defs"])
          end
        }
        return schema_json
      end
    end
  end
end
