require 'spec_helper'


RSpec.describe 'Oxen rm local', :type => :aruba do
  before(:each) do
    # aruba.config.allow_absolute_paths = true
    aruba.config.exit_timeout = 5000000
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

  # Full string
  it "tests removing 900 images from staging" do 
    run_command_and_stop("oxen add ./")
    run_command_and_stop("oxen commit -m 'committing'")


    start_time = Time.now


    run_command_and_stop("oxen rm -r images")

    run_command_and_stop("oxen status")

    end_time = Time.now
    puts "Hard remove directory of 900 images: #{end_time - start_time} seconds"
    puts all_output
  end 


# Full string
# it "tests removing 900 --staged images" do 

#     run_command_and_stop("oxen add ./")

#     start_time = Time.now

#     run_command_and_stop("oxen status")

#     run_command_and_stop("oxen rm -r --staged images")

#     run_command_and_stop("oxen status")

#     end_time = Time.now
#     puts "Remove directory of 900 images: #{end_time - start_time} seconds"
#     puts all_output
#     end 

end
