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
        @schema = load_schema()
        @validator = RustyJSONSchema.build(@schema)
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

        if errors.join(", ").include? "is not valid under any of the given schemas"
          errors = enrich_oneOf_errors(json)
        end
        if !errors.empty?
          raise ParserError, "Openlineage validation failed: " + errors.join(", ")
        end
      end

      # Validator returns very generic OneOfNotValid error message
      # We try to find better reason for mismatch with each candidate.
      def enrich_oneOf_errors(json)
        errors = []
        @schema["oneOf"].each { |ref|
          changed_schema = @schema
          changed_schema.delete("oneOf")
          changed_schema["$ref"] = ref["$ref"]
          validator = RustyJSONSchema.build(changed_schema)
          error = validator.validate(json)
          if !error.empty?
            errors.append("#{ref}: #{error.join(", ")}")
          end
        }
        return errors
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

        
        # list all the facets
        Dir.glob("#{facets_path}/*.json").each { |facet_file|
            facet_schema =  JSON.parse(
              File.read(facet_file).gsub(
                /"https:\/\/openlineage\.io\/spec\/\d-\d-\d\/OpenLineage\.json#\/\$defs\/([a-zA-Z]+)"/,
                '"#/$defs/\1"'
              )
            )

            facet_schema["properties"].each { |property, ref|
              facet_name =  ref["$ref"]&.gsub("#/$defs/", "")
              parents = []
              facet_schema["$defs"][facet_name]["allOf"]&.each { |definition|
                unless definition["$ref"].nil?
                  parents.append(definition["$ref"].gsub("#/$defs/", ""))
                end
              }
              parents.each {|parent|
              add_ref_as_parent_property(schema_json, parent, facet_name, property)
              }
            }
            # include facets' definitions within schema
            schema_json["$defs"] = schema_json["$defs"].merge(facet_schema["$defs"])
        }
        return schema_json
      end

      def add_ref_as_parent_property(schema, parent, facet_name, property)
        getter = find_parent_object_getter(parent)
        if getter.nil?
          return
        end
        properties = getter.call(schema)["properties"] || {}
        properties[property] = {"$ref" => "#/$defs/" + facet_name}
        getter.call(schema)["properties"] = properties
      end

      # Based on facet name find path to object facets
      def find_parent_object_getter(parent)
        getter = nil
        case parent
        when "JobFacet"
          if @validate_job_facets
            getter = ->(schema) { schema["$defs"]["Job"]["properties"]["facets"] }
          end
        when "RunFacet"
          if @validate_run_facets
            getter = ->(schema) { schema["$defs"]["Run"]["properties"]["facets"] }
          end
        when "DatasetFacet"
          if @validate_dataset_facets
            getter = ->(schema) { schema["$defs"]["Dataset"]["properties"]["facets"] }
          end
        when "OutputDatasetFacet"
          if @validate_output_dataset_facets
            getter = ->(schema) { schema \
            ["$defs"] \
            ["OutputDataset"] \
            ["allOf"].select {|el| el.key?("type") } \
            [0] \
            ["properties"] \
            ["outputFacets"] }
          end
        when "InputDatasetFacet"
          if @validate_input_dataset_facets
            getter = ->(schema) { schema \
            ["$defs"] \
            ["InputDataset"] \
            ["allOf"].select {|el| el.key?("type") } \
            [0] \
            ["properties"] \
            ["inputFacets"] }
          end
        end
        return getter
      end
    end
  end
end
