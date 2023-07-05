require 'spec_helper'
require 'dotenv'

IMAGE_PATH = 'ox-img.png'

#TODO: Add remove tests for directories when we can remote rm directories 

RSpec.describe 'Remote Remove Image', :type => :aruba do
    # Full string
  it "tests remote remove image for empty repo" do 
    # Setup
    run_command_and_stop('oxen clone https://hub.oxen.ai/ba/test-empty-repo --shallow')
    copy "ox-img.png", "test-empty-repo"
    cd 'test-empty-repo'
    run_command_and_stop("oxen remote add #{IMAGE_PATH}")
    
    run_command_and_stop("oxen remote status")
    expect(last_command_started).to have_output include "new file: ox-img.png"

    # Test remove
    start_time = Time.now
    run_command_and_stop("oxen remote rm --staged ox-img.png")
    end_time = Time.now
    puts all_output
    run_command_and_stop("oxen remote status")
    expect(last_command_started).to have_output include "nothing to commit"
    puts "Remote remove image empty repo: #{end_time - start_time} seconds"    
  end 
end