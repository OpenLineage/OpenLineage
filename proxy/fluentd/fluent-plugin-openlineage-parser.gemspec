lib = File.expand_path("../lib", __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)

Gem::Specification.new do |spec|
  spec.name    = "fluent-plugin-openlineage"
  spec.version = "0.1.0"
  spec.authors = ["Pawel Leszczynski"]
  spec.email   = ["leszczynski.pawel@gmail.com"]

  spec.summary       = %q{Parser to validate Openlineage events.}
  spec.description   = %q{Fluentd parser that validates if JSON is a valid Openlineage event.}
  spec.homepage      = "http://openlineage.io"
  spec.license       = "Apache-2.0"

  test_files, files  = `git ls-files -z`.split("\x0").partition do |f|
    f.match(%r{^(test|spec|features)/})
  end
  spec.files         = files
  spec.executables   = files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = test_files
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 1.17.2"
  spec.add_development_dependency "rake", "~> 12.3.3"
  spec.add_development_dependency "test-unit", "~> 3.2.9"
  spec.add_runtime_dependency "fluentd", [">= 0.14.10", "< 2"]
  spec.add_dependency "rusty_json_schema"
end
