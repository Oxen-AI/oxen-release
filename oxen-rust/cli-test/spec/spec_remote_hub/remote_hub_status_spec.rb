require 'spec_helper'

RSpec.describe 'Remote Status', :type => :aruba do
  before(:each) do
    run_command_and_stop('oxen config --name ruby-test --email test@ox.ai')
    aruba.config.exit_timeout = 5000
  end

  # Full string
  it "tests remote status for empty repo" do 
    # Setup
    run_command_and_stop('oxen clone https://hub.oxen.ai/ba/test-empty-repo --shallow')
    cd 'test-empty-repo'

    start_time = Time.now
    run_command_and_stop('oxen remote status')
    puts "Remote status empty repo: #{Time.now - start_time} seconds"
    puts all_output
  end 

  it "tests remote status for small repo with many commits" do
    run_command_and_stop('oxen clone https://hub.oxen.ai/ba/test-empty-repo-many-commits --shallow')
    cd 'test-empty-repo-many-commits'

    start_time = Time.now
    run_command_and_stop('oxen remote status')
    puts "Remote status small repo with many commits: #{Time.now - start_time} seconds"
    puts all_output
  end
    

  it "tests remote status for large repo" do 
    #Setup 
    run_command_and_stop('oxen clone https://hub.oxen.ai/ba/test-large-repo --shallow')
    cd 'test-large-repo'

    start_time = Time.now
    run_command_and_stop('oxen remote status')
    puts "Remote status large repo: #{Time.now - start_time} seconds"
    puts all_output
  end

end
