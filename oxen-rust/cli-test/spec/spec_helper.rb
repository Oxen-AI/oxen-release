require 'bundler/setup'
require 'aruba/rspec'
require 'fileutils'
require 'dotenv'

# TODO: look into how tests can be "grouped" together so that the
# before(:suite) and after(:suite) hooks can be used only where relevant

RSpec.configure do |config|
  config.include Aruba::Api
  config.include Aruba::Matchers
  # Enable flags like --only-failures and --next-failure
  config.example_status_persistence_file_path = '.rspec_status'

  # Disable RSpec exposing methods globally on `Module` and `main`
  config.disable_monkey_patching!

  config.expect_with :rspec do |c|
    c.syntax = :expect
  end

  config.before(:each) do
    Dotenv.load('.env')
    run_command_and_stop('oxen config --name ruby-test --email test@oxen.ai')
    run_command_and_stop("oxen config --auth dev.hub.oxen.ai #{ENV['OXEN_API_KEY']}")
  end

  config.after(:each) do
    # Ensure the remote repository is deleted after each test

    run_command_and_stop('oxen delete-remote --name EloyMartinez/performance-test --host dev.hub.oxen.ai')
  rescue StandardError => e
    # Log the error or ignore it
    puts "Warning: Failed to delete remote repository - #{e.message}"
  end
end
