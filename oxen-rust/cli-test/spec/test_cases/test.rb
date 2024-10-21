require 'spec_helper'
require 'dotenv'

RSpec.describe 'test', type: :aruba do
  before(:each) do
    aruba.config.exit_timeout = 600 # Set the timeout to 60 seconds
  end

  after(:each) do
    run_command_and_stop('rm -rf test-small-repo')
  end

  it 'tests oxen init, add, commit, and push with a small file' do
    # Setup
    run_command_and_stop('mkdir test-small-repo')
    cd 'test-small-repo'

    system('python ../benchmark/generate_image_repo.py --output_dir ~/test-small-repo/Data/10k_images --num_images 10000 --num_dirs 1000 --image_size 128 128')

    # Initialize the repository
    start_time = Time.now
    run_command_and_stop('oxen init')
    end_time = Time.now
    puts "oxen init command took: #{end_time - start_time} seconds"

    # Add the file
    start_time = Time.now
    run_command_and_stop('oxen add .')
    end_time = Time.now
    puts "oxen add command took: #{end_time - start_time} seconds"

    # Commit the file
    start_time = Time.now
    run_command_and_stop('oxen commit -m "Add small file"')
    end_time = Time.now
    puts "oxen commit command took: #{end_time - start_time} seconds"
  end
end
