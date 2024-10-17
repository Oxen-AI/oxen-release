require 'spec_helper'
require 'dotenv'

RSpec.describe 'test', type: :aruba do
  before(:each) do
    aruba.config.exit_timeout = 600 # Set the timeout to 600 seconds
  end

  after(:each) do
    run_command_and_stop('rm -rf test-small-repo')
  end

  it 'tests oxen init, add, commit, and push with a small file' do
    # Setup
    measure_time('mkdir test-small-repo')
    cd 'test-small-repo'

    # Generate image repository
    system('python ../benchmark/generate_image_repo.py --output_dir ~/test-small-repo/Data/10k_images --num_images 1 --num_dirs 1 --image_size 128 128')

    # Initialize the repository
    puts "oxen init command took: #{measure_time('oxen init')} seconds"

    # Add the file
    puts "oxen add command took: #{measure_time('oxen add .')} seconds"

    # Commit the file
    puts "oxen commit command took: #{measure_time('oxen commit -m "Add small file"')} seconds"

    # Create the remote repository
    puts "oxen create-remote command took: #{measure_time('oxen create-remote --name EloyMartinez/performance-test --host dev.hub.oxen.ai --scheme https --is_public')} seconds"

    # Set the remote
    puts "oxen config --set-remote command took: #{measure_time('oxen config --set-remote origin https://dev.hub.oxen.ai/EloyMartinez/performance-test')} seconds"

    # Push the file
    puts "oxen push command took: #{measure_time('oxen push')} seconds"

    cd '..'

    # Clone the repository
    puts "oxen clone command took: #{measure_time('oxen clone https://dev.hub.oxen.ai/EloyMartinez/performance-test')} seconds"

    directory_path = 'tmp/aruba/performance-test'
    file_path = File.join(directory_path, 'simple.txt')

    # Ensure the directory exists
    FileUtils.mkdir_p(directory_path)

    # Append to 'simple.txt'
    File.open(file_path, 'a') do |file|
      file.puts 'This is a simple text file. Edited remotely.'
    end

    cd 'performance-test'

    # Add, commit, and push the changes
    puts "oxen add simple.txt command took: #{measure_time('oxen add simple.txt')} seconds"
    puts "oxen commit command took: #{measure_time('oxen commit -m "Edit simple text file remotely"')} seconds"
    puts "oxen push command took: #{measure_time('oxen push')} seconds"

    # Pull the changes from test-repo
    cd '..'
    cd 'test-small-repo'
    puts "oxen pull command took: #{measure_time('oxen pull')} seconds"

    # Checkout to a new branch
    puts "oxen checkout command took: #{measure_time('oxen checkout -b second_branch')} seconds"

    directory_path = 'tmp/aruba/test-small-repo'
    file_path = File.join(directory_path, 'simple.txt')

    # Ensure the directory exists
    FileUtils.mkdir_p(directory_path)

    # Append to 'simple.txt'
    File.open(file_path, 'a') do |file|
      file.puts 'This is a simple text file. Edited remotely. this is a second change'
    end

    # Add, commit, and push the second change
    puts "oxen add simple.txt command took: #{measure_time('oxen add simple.txt')} seconds"
    puts "oxen commit command took: #{measure_time('oxen commit -m "Edit simple text file remotely second time"')} seconds"
    puts "oxen push command took: #{measure_time('oxen push')} seconds"

    # Checkout main branch and then back to second branch
    puts "oxen checkout main command took: #{measure_time('oxen checkout main')} seconds"
    puts "oxen checkout second_branch command took: #{measure_time('oxen checkout second_branch')} seconds"

    # Restore the file from the main branch
    puts "oxen restore command took: #{measure_time('oxen restore --source main simple.txt')} seconds"

    # Delete the remote repository
    puts "oxen delete-remote command took: #{measure_time('oxen delete-remote --name EloyMartinez/performance-test --host dev.hub.oxen.ai')} seconds"
  end

  def measure_time(command)
    start_time = Time.now
    run_command_and_stop(command)
    end_time = Time.now
    end_time - start_time
  end
end