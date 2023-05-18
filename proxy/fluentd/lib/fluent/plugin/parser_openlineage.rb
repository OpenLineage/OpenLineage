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
        @validate_output_dataset_facets = conf.fetch('validate_output_dataset_facets', false)
        @validate_dataset_facets = conf.fetch('validate_dataset_facets', false)
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
        schemaFile = @spec_directory + "OpenLineage.json"

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
        dataset_facets = schema_json["$defs"]["Dataset"]["properties"]["facets"]
        if @validate_dataset_facets
          dataset_facets \
            ["anyOf"].select {|el| el.key?("$ref") }.each { | facet_ref |
              facet_name =  facet_ref["$ref"].gsub("#/$defs/", "")
              facet_schema = JSON.parse(File.read(facets_path + facet_name + ".json"))
              property_name = facet_schema["properties"].keys[0]
              if not dataset_facets.key?("properties")
                dataset_facets["properties"] = {}
                dataset_facets["additionalProperties"] = { "$ref" => "#/$defs/DatasetFacet" }
              end
              dataset_facets["properties"][property_name] = {  "$ref" => "#/$defs/" + facet_name }

          }
        else
          dataset_facets["additionalProperties"] = { "type" => "object" }
        end
        dataset_facets.delete("anyOf")

        # run facets
        run_facets = schema_json["$defs"]["Run"]["properties"]["facets"]
        if @validate_run_facets
          run_facets \
            ["anyOf"].select {|el| el.key?("$ref") }.each { | facet_ref |
              facet_name =  facet_ref["$ref"].gsub("#/$defs/", "")
              facet_schema = JSON.parse(File.read(facets_path + facet_name + ".json"))
              property_name = facet_schema["properties"].keys[0]

              if not run_facets.key?("properties")
                run_facets["properties"] = {}
                run_facets["additionalProperties"] = { "$ref" => "#/$defs/RunFacet" }
              end
              run_facets["properties"][property_name] = {  "$ref" => "#/$defs/" + facet_name }
          }
        else
          run_facets["additionalProperties"] = { "type" => "object" }
        end
        run_facets.delete("anyOf")

        # job facets
        job_facets = schema_json["$defs"]["Job"]["properties"]["facets"]
        if @validate_job_facets
          job_facets["anyOf"].select {|el| el.key?("$ref") }.each { | facet_ref |
              facet_name =  facet_ref["$ref"].gsub("#/$defs/", "")
              facet_schema = JSON.parse(File.read(facets_path + facet_name + ".json"))
              property_name = facet_schema["properties"].keys[0]

              if not job_facets.key?("properties")
                job_facets["properties"] = {}
                job_facets["additionalProperties"] = { "$ref" => "#/$defs/JobFacet" }
              end
              job_facets["properties"][property_name] = {  "$ref" => "#/$defs/" + facet_name }
          }
        else
          job_facets["additionalProperties"] = { "type" => "object" }
        end
        job_facets.delete("anyOf")

        # input dataset facets
        if @validate_input_dataset_facets
          input_facets = schema_json \
            ["$defs"] \
            ["InputDataset"] \
            ["allOf"].select {|el| el.key?("type") } \
            [0] \
            ["properties"] \
            ["inputFacets"]
          input_facets \
            ["anyOf"].select {|el| el.key?("$ref") }.each { | facet_ref |
              facet_name =  facet_ref["$ref"].gsub("#/$defs/", "")
              facet_schema = JSON.parse(File.read(facets_path + facet_name + ".json"))
              property_name = facet_schema["properties"].keys[0]

              if not input_facets.key?("properties")
                input_facets["properties"] = {}
                input_facets["additionalProperties"] = { "$ref" => "#/$defs/OutputDatasetFacet" }
              end
              input_facets["properties"][property_name] = {  "$ref" => "#/$defs/" + facet_name }
          }
          input_facets.delete("anyOf")
        end

        # output dataset facets
        if @validate_output_dataset_facets
          output_facets = schema_json \
            ["$defs"] \
            ["OutputDataset"] \
            ["allOf"].select {|el| el.key?("type") } \
            [0] \
            ["properties"] \
            ["outputFacets"]

          output_facets \
            ["anyOf"].select {|el| el.key?("$ref") }.each { | facet_ref |
              facet_name =  facet_ref["$ref"].gsub("#/$defs/", "")
              facet_schema = JSON.parse(File.read(facets_path + facet_name + ".json"))
              property_name = facet_schema["properties"].keys[0]
              if not output_facets.key?("properties")
                output_facets["properties"] = {}
                output_facets["additionalProperties"] = { "$ref" => "#/$defs/OutputDatasetFacet" }
              end
              output_facets["properties"][property_name] = {  "$ref" => "#/$defs/" + facet_name }
          }
          output_facets.delete("anyOf")
        end

        # list all the facets
        Dir.entries(facets_path).each { |facet_file|
          if facet_file.end_with?(".json")
            facet_schema =  JSON.parse(
              File.read(facets_path + facet_file).gsub(
                /"https:\/\/openlineage\.io\/spec\/\d-\d-\d\/OpenLineage\.json#\/\$defs\/([a-zA-Z]+)"/,
                '"#/$defs/\1"'
              )
            )
            # include facets' definitions within schema
            schema_json["$defs"] = schema_json["$defs"].merge(facet_schema["$defs"])
          end
        }
        return schema_json
      end
    end
  end
end
