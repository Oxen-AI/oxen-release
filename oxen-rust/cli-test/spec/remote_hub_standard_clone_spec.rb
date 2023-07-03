require 'spec_helper'

RSpec.describe 'Standard Clone', :type => :aruba do
  before(:each) do
    run_command_and_stop('oxen config --name ruby-test --email test@ox.ai')
    aruba.config.exit_timeout = 5000
  end

  # Full string
  it "tests standard clone for empty repo" do 
    start_time = Time.now
    run_command_and_stop('oxen clone https://hub.oxen.ai/ba/test-empty-repo')
    puts "Standard clone empty repo: #{Time.now - start_time} seconds"
    cd 'test-empty-repo'
    puts all_output
  end 

  it "tests standard clone for small repo with many commits" do
    start_time = Time.now
    run_command_and_stop('oxen clone https://hub.oxen.ai/ba/test-empty-repo-many-commits')
    puts "Standard clone small repo with many commits: #{Time.now - start_time} seconds"
    cd 'test-empty-repo-many-commits'
    puts all_output
  end
    

  it "tests standard clone for large repo" do 
    start_time = Time.now
    run_command_and_stop('oxen clone https://hub.oxen.ai/ba/test-large-repo')
    cd 'test-large-repo'
    puts "Standard clone large repo: #{Time.now - start_time} seconds"
    puts all_output
  end

end
