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
    expect(last_command_started).to have_output include "modified: annotations/train.csv"
    puts all_output
  end 
end
