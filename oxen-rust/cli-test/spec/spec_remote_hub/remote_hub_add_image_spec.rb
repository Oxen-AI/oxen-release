require 'spec_helper'
require 'dotenv'

IMAGE_PATH = 'ox-img.png'

RSpec.describe 'Remote Add Image', :type => :aruba do
  before(:each) do
    Dotenv.load('.env')
    run_command_and_stop("oxen config --name ruby-test --email test@oxen.ai")
    run_command_and_stop("oxen config --auth hub.oxen.ai #{ENV["OXEN_API_KEY"]}")
  end

  after(:each) do 
    run_command_and_stop("oxen remote rm --staged ox-img.png")
  end

  # Full string
  it "tests remote add image for empty repo" do 
    # Setup
    run_command_and_stop('oxen clone https://hub.oxen.ai/ba/test-empty-repo --shallow')
    copy "ox-img.png", "test-empty-repo"
    cd 'test-empty-repo'
    
    start_time = Time.now
    run_command_and_stop("oxen remote add #{IMAGE_PATH}")
    end_time = Time.now
    run_command_and_stop("oxen remote status")
    expect(last_command_started).to have_output include "new file: ox-img.png"
    puts "Remote add image empty repo: #{end_time - start_time} seconds"
  end 

  # it "tests remote add image for small repo with many commits" do
  #   run_command_and_stop('oxen clone https://hub.oxen.ai/ba/test-empty-repo-many-commits --shallow')
  #   cd 'test-empty-repo-many-commits'
    
  #   start_time = Time.now
  #   run_command_and_stop('oxen remote add #{IMAGE_PATH}')
  #   puts "Remote add image small repo with many commits: #{Time.now - start_time} seconds"
  # end
    

  # it "tests remote add image for large repo" do 
  #   run_command_and_stop('oxen clone https://hub.oxen.ai/ba/test-large-repo --shallow')
  #   cd 'test-large-repo'

  #   start_time = Time.now
  #   run_command_and_stop('oxen remote add #{IMAGE_PATH}')
  #   puts "Remote add image large repo: #{Time.now - start_time} seconds"
  # end

end
