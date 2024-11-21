require 'spec_helper'

RSpec.describe 'rm - test relative paths', type: :aruba do
  before(:each) do
    aruba.config.exit_timeout = 120
  end

  after(:each) do
    run_command_and_stop('rm -rf test-relative-paths')
  end

  it 'tests oxen rm with relative paths from subdirectories' do
    directory_path = 'tmp/aruba/test-relative-paths'

    # Setup base repo
    run_command_and_stop('mkdir test-relative-paths')
    cd 'test-relative-paths'
    run_command_and_stop('oxen init')

    # Create nested directory structure
    run_command_and_stop('mkdir -p images/test')
    
    # Create and commit first set of files
    file_path = File.join(directory_path, 'root.txt')
    File.open(file_path, 'w') do |file|
      file.puts 'root file'
    end
    run_command_and_stop('oxen add root.txt')
    run_command_and_stop('oxen commit -m "adding root file"')
    
    # Create and commit nested file
    nested_file_path = File.join(directory_path, 'images/test/nested.txt')
    File.open(nested_file_path, 'w') do |file|
      file.puts 'nested file'
    end
    cd 'images/test'
    run_command_and_stop('oxen add nested.txt')
    run_command_and_stop('oxen commit -m "adding nested file"')
    
    # Test removing file from nested directory
    run_command_and_stop('oxen rm ../../root.txt')
    
    # Test removing local file
    run_command_and_stop('oxen rm nested.txt')
    
    # Go back to root and verify files are removed
    cd '../..'
    run_command_and_stop('oxen status')
    
    # Should show files as removed in staging
    expect(last_command_started).to have_output(/removed: root\.txt/)
    expect(last_command_started).to have_output(/removed: images\/test\/nested\.txt/)

    
    # Files should still exist on disk
    expect(File.exist?(File.join(directory_path, 'root.txt'))).to be false
    expect(File.exist?(File.join(directory_path, 'images/test/nested.txt'))).to be false
  end
end