require 'spec_helper'
require 'dotenv'

RSpec.describe 'test', type: :aruba do
  before(:each) do
    aruba.config.exit_timeout = 6000 # Set the timeout to 6000 seconds
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
    init_time = measure_time('oxen init')
    puts "oxen init command took: #{init_time} seconds"
    expect(init_time).to be < 1.0 # Example threshold

    # Add the file
    add_time = measure_time('oxen add .')
    puts "oxen add command took: #{add_time} seconds"
    expect(add_time).to be < 35.0 # Example threshold

    # Commit the file
    commit_time = measure_time('oxen commit -m "Add small file"')
    puts "oxen commit command took: #{commit_time} seconds"
    expect(commit_time).to be < 70.0 # Example threshold

    # Create the remote repository
    create_remote_time = measure_time('oxen create-remote --name EloyMartinez/performance-test --host dev.hub.oxen.ai --scheme https --is_public')
    puts "oxen create-remote command took: #{create_remote_time} seconds"
    expect(create_remote_time).to be < 1.0 # Example threshold

    # Set the remote
    set_remote_time = measure_time('oxen config --set-remote origin https://dev.hub.oxen.ai/EloyMartinez/performance-test')
    puts "oxen config --set-remote command took: #{set_remote_time} seconds"
    expect(set_remote_time).to be < 0.5 # Example threshold

    # Push the file
    push_time = measure_time('oxen push')
    puts "oxen push command took: #{push_time} seconds"
    expect(push_time).to be < 800.0 # Example threshold

    cd '..'

    # Clone the repository
    clone_time = measure_time('oxen clone https://dev.hub.oxen.ai/EloyMartinez/performance-test')
    puts "oxen clone command took: #{clone_time} seconds"
    expect(clone_time).to be < 650.0 # Example threshold

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
    add_simple_time = measure_time('oxen add simple.txt')
    puts "oxen add simple.txt command took: #{add_simple_time} seconds"
    expect(add_simple_time).to be < 3.0 # Example threshold

    commit_simple_time = measure_time('oxen commit -m "Edit simple text file remotely"')
    puts "oxen commit command took: #{commit_simple_time} seconds"
    expect(commit_simple_time).to be < 3.0 # Example threshold

    push_simple_time = measure_time('oxen push')
    puts "oxen push command took: #{push_simple_time} seconds"
    expect(push_simple_time).to be < 300.0 # Example threshold

    # Pull the changes from test-repo
    cd '..'
    cd 'test-small-repo'
    pull_time = measure_time('oxen pull')
    puts "oxen pull command took: #{pull_time} seconds"
    expect(pull_time).to be < 100.0 # Example threshold

    # Checkout to a new branch
    checkout_time = measure_time('oxen checkout -b second_branch')
    puts "oxen checkout command took: #{checkout_time} seconds"
    expect(checkout_time).to be < 1.0 # Example threshold

    directory_path = 'tmp/aruba/test-small-repo'
    file_path = File.join(directory_path, 'simple.txt')

    # Ensure the directory exists
    FileUtils.mkdir_p(directory_path)

    # Append to 'simple.txt'
    File.open(file_path, 'a') do |file|
      file.puts 'This is a simple text file. Edited remotely. this is a second change'
    end

    # Add, commit, and push the second change
    add_second_change_time = measure_time('oxen add simple.txt')
    puts "oxen add simple.txt command took: #{add_second_change_time} seconds"
    expect(add_second_change_time).to be < 1.0 # Example threshold

    commit_second_change_time = measure_time('oxen commit -m "Edit simple text file remotely second time"')
    puts "oxen commit command took: #{commit_second_change_time} seconds"
    expect(commit_second_change_time).to be < 1.0 # Example threshold

    push_second_change_time = measure_time('oxen push')
    puts "oxen push command took: #{push_second_change_time} seconds"
    expect(push_second_change_time).to be < 20.0 # Example threshold

    # Checkout main branch and then back to second branch
    checkout_main_branch_time = measure_time('oxen checkout main')
    puts "oxen checkout main command took: #{checkout_main_branch_time} seconds"
    expect(checkout_main_branch_time).to be < 100.0 # Example threshold

    checkout_second_branch_time = measure_time('oxen checkout second_branch')
    puts "oxen checkout second_branch command took: #{checkout_second_branch_time} seconds"
    expect(checkout_second_branch_time).to be < 100.0 # Example threshold

    # Restore the file from the main branch
    restore_second_change_time = measure_time('oxen restore --source main simple.txt')
    puts "oxen restore command took: #{restore_second_change_time} seconds"
    expect(restore_second_change_time).to be < 3.0 # Example threshold

    # Delete the remote repository
    delete_remote_time = measure_time('oxen delete-remote --name EloyMartinez/performance-test --host dev.hub.oxen.ai')
    puts "oxen delete-remote command took: #{delete_remote_time} seconds"
    expect(delete_remote_time).to be < 2.0 # Example threshold
  end

  def measure_time(command)
    start_time = Time.now
    run_command_and_stop(command)
    end_time = Time.now
    end_time - start_time
  end
end
