require 'spec_helper'

RSpec.describe 'schemas add - test relative paths', type: :aruba do
  before(:each) do
    aruba.config.exit_timeout = 120
  end

  after(:each) do
    run_command_and_stop('rm -rf test-schema-paths')
  end

  it 'tests oxen schemas add with relative paths from subdirectories' do
    directory_path = 'tmp/aruba/test-schema-paths'

    # Setup base repo
    run_command_and_stop('mkdir test-schema-paths')
    cd 'test-schema-paths'
    run_command_and_stop('oxen init')

    # Create nested directory structure
    run_command_and_stop('mkdir -p data/frames')

    csv_path = File.join(directory_path, 'root.csv')
    File.open(csv_path, 'w') do |file|
      file.puts 'id,image,description'
      file.puts '1,/path/to/img1.jpg,test image 1'
      file.puts '2,/path/to/img2.jpg,test image 2'
    end

    run_command_and_stop('oxen add root.csv')
    run_command_and_stop('oxen commit -m "adding root csv"')


    # Create a CSV file in the nested directory
    csv_path = File.join(directory_path, 'data/frames/test.csv')
    File.open(csv_path, 'w') do |file|
      file.puts 'id,image,description'
      file.puts '1,/path/to/img1.jpg,test image 1'
      file.puts '2,/path/to/img2.jpg,test image 2'
    end

    cd 'data/frames'

    # Add and commit the CSV file
    run_command_and_stop('oxen add test.csv')
    run_command_and_stop('oxen commit -m "adding test csv"')

    run_command_and_stop('oxen schemas add test.csv -c image -m \'{"_oxen": {"render": {"func": "image"}}}\'')
    run_command_and_stop('oxen schemas add ../../root.csv -c image -m \'{"_oxen": {"render": {"func": "image"}}}\'')

    # Verify schema changes
    cd '..'
    run_command_and_stop('oxen status')

    # Check for schema changes in status
    expect(last_command_started).to have_output(/new schema: data\/frames\/test\.csv/)
    expect(last_command_started).to have_output(/new schema: root\.csv/)
    expect(last_command_started).to have_output(/Schemas to be committed/)
  end
end