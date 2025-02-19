require 'spec_helper'
require 'pathname'
require 'securerandom'

RSpec.describe 'oxen download', type: :aruba do
  # Helper method to generate unique IDs
  def generate_unique_id
    SecureRandom.hex(4)  # Generates an 8-character hex string
  end

  before(:each) do
    aruba.config.exit_timeout = 120
  end

  after(:each) do
    FileUtils.rm_rf('test-relative-paths')
  end

  it 'tests oxen download works for a single file' do
    # Use the unique ID in your test
    unique_id = generate_unique_id
    directory_path = File.join('tmp', 'aruba', "test-relative-paths-#{unique_id}")
    FileUtils.mkdir_p(directory_path)

    Dir.chdir(directory_path)
    run_system_command('oxen init') 

    # Create nested directory structure
    file_path = File.join('hi.txt')
   
    File.open(file_path, 'a') do |file|
      file.puts 'This is a simple text file.'
    end

    # Add file from current directory
    run_system_command("oxen add hi.txt") 

    # Commit file
    run_system_command('oxen commit -m "add hi.txt"') 

    # Create a remote repo
    unique_id = generate_unique_id
    remote_repo_name = "ox/hi-#{unique_id}"
    run_system_command("oxen create-remote --name #{remote_repo_name} --host localhost:3000 --scheme http")
    run_system_command("oxen config --set-remote origin http://localhost:3000/#{remote_repo_name}")

    # Push to remote
    run_system_command("oxen push origin main")

    # Download the file
    run_system_command("oxen download #{remote_repo_name} hi.txt -o hi2.txt --host localhost:3000 --scheme http")

    # Verify file contents
    expect(File.read(File.join('hi2.txt'))).to eq("This is a simple text file.\n")

    # Return to cli-test
    parent_path = File.join('..', '..')
    Dir.chdir(parent_path)
  end
end
