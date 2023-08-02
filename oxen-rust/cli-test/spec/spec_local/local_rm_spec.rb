require 'spec_helper'


RSpec.describe 'Oxen rm local', :type => :aruba do
  before(:each) do
    aruba.config.exit_timeout = 500000
    run_command_and_stop('oxen config --name ruby-test --email test@ox.ai')

    repo_name = 'test-small-repo'
    fixtures_dir = expand_path("%/")
    current_dir = expand_path(".")

    run_command_and_stop("cp -r #{fixtures_dir}/#{repo_name} #{current_dir}")
    cd "./test-small-repo"
    run_command_and_stop("rm -r .oxen")
    run_command_and_stop("oxen init")
    
  end

  after(:each) do 

  end

  it "tests oxen rm -r directory with 1k images" do 
    run_command_and_stop("rm -r .oxen")
    run_command_and_stop("oxen init")
    run_command_and_stop("oxen add ./")
    run_command_and_stop("oxen commit -m 'committing'")

    start_time = Time.now


    
    run_command_and_stop("oxen rm -r images")
    run_command_and_stop("oxen status")
    expect(last_command_started).to have_output match %r{.*?Files to be committed:(?:.|\n)*?removed: images/.*}


    expect(last_command_started).to_not have_output include "images/LOCK"

    end_time = Time.now
    puts "oxen rm -r directory of 1k images: #{end_time - start_time} seconds"
    puts all_output
  end 


it "tests removing 1k --staged images" do 

    run_command_and_stop("oxen add ./")

    start_time = Time.now
    run_command_and_stop("oxen rm -r --staged images")
    run_command_and_stop("oxen status")

    expect(last_command_started).to have_output match /Untracked Directories.*images\//m

    end_time = Time.now
    puts "oxen rm --staged directory of 1k images: #{end_time - start_time} seconds"
    puts all_output
    end 

end
