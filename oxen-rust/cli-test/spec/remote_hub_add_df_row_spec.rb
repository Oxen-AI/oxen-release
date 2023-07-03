require 'spec_helper'

RSpec.describe 'Remote add dataframe row', :type => :aruba do
  before(:each) do
    run_command_and_stop('oxen config --name ruby-test --email test@ox.ai')
    aruba.config.exit_timeout = 5000
  end

  after(:each) do 
    run_command_and_stop('oxen remote restore --staged annotations/train.csv')
  end

  # Full string
  it "tests remote add df row for empty repo" do 
    run_command_and_stop('oxen clone https://hub.oxen.ai/ba/test-empty-repo --shallow')
    cd 'test-empty-repo'

    start_time = Time.now
    run_command_and_stop('oxen remote df annotations/train.csv --add-row "hi,there,adding,a,row"')
    puts "Remote add df row empty repo: #{Time.now - start_time} seconds"

    run_command_and_stop('oxen remote status')
    puts all_output
  end 

  it "tests remote add df row for small repo with many commits" do
    run_command_and_stop('oxen clone https://hub.oxen.ai/ba/test-empty-repo-many-commits --shallow')
    cd 'test-empty-repo-many-commits'
    
    start_time = Time.now
    run_command_and_stop('oxen remote df annotations/train.csv --add-row "hi,there,adding,a,row"')
    puts all_output
    puts "Remote add df row small repo with many commits: #{Time.now - start_time} seconds"
  end
    

  it "tests remote add df row for large repo" do 
    run_command_and_stop('oxen clone https://hub.oxen.ai/ba/test-large-repo --shallow')
    cd 'test-large-repo'

    start_time = Time.now
    run_command_and_stop('oxen remote add #{IMAGE_PATH}')
    run_command_and_stop('oxen remote df annotations/train.csv --add-row "hi,there"')
    puts all_output
    puts "Remote add df row large repo: #{Time.now - start_time} seconds"
  end

end
