require 'spec_helper'

RSpec.describe 'add - test relative paths', type: :aruba do
  before(:each) do
    aruba.config.exit_timeout = 120
  end

  after(:each) do
    run_command_and_stop('rm -rf test-relative-paths')
  end

  it 'tests oxen add with relative paths from subdirectories' do
    directory_path = 'tmp/aruba/test-relative-paths'

    # Setup base repo
    run_command_and_stop('mkdir test-relative-paths')
    cd 'test-relative-paths'
    run_command_and_stop('oxen init')

    # Create nested directory structure
    run_command_and_stop('mkdir -p images/test')
    file_path = File.join(directory_path, 'hi.txt')
    File.open(file_path, 'a') do |file|
      file.puts 'This is a simple text file.'
    end

    # Create a file in root from nested directory
    cd 'images/test'
    
    # Add file using relative path from nested directory
    run_command_and_stop('oxen add ../../hi.txt')

    # Create another file in nested directory

    file_path = File.join(directory_path, 'images/test/nested.txt')
    File.open(file_path, 'w') do |file|
      file.puts 'nested'
    end
    
    # Add file from current directory
    run_command_and_stop('oxen add nested.txt')
    
    # Go back to root and verify files
    cd '../..'
    run_command_and_stop('oxen status')
    expect(last_command_started).to have_output(/hi\.txt/)
    expect(last_command_started).to have_output(/images\/test\/nested\.txt/)
    # Verify file contents
    expect(File.read(File.join(directory_path, 'hi.txt'))).to eq("This is a simple text file.\n")
    expect(File.read(File.join(directory_path, 'images/test/nested.txt'))).to eq("nested\n")
  end
end
