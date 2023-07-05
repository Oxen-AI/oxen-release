require "bundler/setup"
require "aruba/getting/started"
require "aruba/rspec"
require "fileutils"
require "dotenv"

# Todo: look into how tests can be "grouped" together so that the
# before(:suite) and after(:suite) hooks can be used only where relevant

RSpec.configure do |config|
  config.include Aruba::Api
  config.include Aruba::Matchers
  # Enable flags like --only-failures and --next-failure
  config.example_status_persistence_file_path = ".rspec_status"

  # Disable RSpec exposing methods globally on `Module` and `main`
  config.disable_monkey_patching!

  config.expect_with :rspec do |c|
    c.syntax = :expect
  end

  config.before(:each) do 
    source_path = File.join(File.dirname(__FILE__), 'data/ox-img.png')
    destination_path = expand_path('.')
    Dotenv.load('.env')
    run_command_and_stop("oxen config --name ruby-test --email test@oxen.ai")
    run_command_and_stop("oxen config --auth hub.oxen.ai #{ENV["OXEN_API_KEY"]}")

    # Copy the file
    FileUtils.cp(source_path, destination_path)

  end


end
