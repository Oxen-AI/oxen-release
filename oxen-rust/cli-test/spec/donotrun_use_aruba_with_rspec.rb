require 'spec_helper'

RSpec.describe 'First Run', :type => :aruba do
  let(:file) { 'file.txt' }
  let(:content) { 'Hello, Aruba!' }

  before { write_file file, content }
  before { run_command('aruba-test-cli file.txt') }

  # Full string
  it { expect(last_command_started).to have_output content }

  # Substring
  it { expect(last_command_started).to have_output(/Hello/) }
end
