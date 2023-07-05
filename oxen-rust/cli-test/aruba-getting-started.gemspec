# coding: utf-8
lib = File.expand_path("../lib", __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require "aruba/getting/started/version"

Gem::Specification.new do |spec|
  spec.name          = "aruba-getting-started"
  spec.version       = Aruba::Getting::Started::VERSION
  spec.authors       = ["Dennis Günnewig"]
  spec.email         = ["dev@fedux.org"]

  spec.summary       = %q{This is an example app for the aruba project}
  spec.homepage      = "https://github.com/cucumber/aruba-getting-started"

  # Prevent pushing this gem to RubyGems.org. To allow pushes either set the 'allowed_push_host'
  # to allow pushing to a single host or delete this section to allow pushing to any host.
  if spec.respond_to?(:metadata)
    spec.metadata["allowed_push_host"] = "TODO: Set to 'http://mygemserver.com'"
  else
    raise "RubyGems 2.0 or newer is required to protect against " \
      "public gem pushes."
  end

  spec.files         = `git ls-files -z`.split("\x0").reject do |f|
    f.match(%r{^(test|spec|features)/})
  end
  spec.bindir        = "exe"
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]
end
