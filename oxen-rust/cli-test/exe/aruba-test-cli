#!/usr/bin/env ruby

file = ARGV[0]

if file.nil? || file.empty?
  abort "aruba-test-cli [file]: Filename is missing"
elsif !File.exist? file
  abort "aruba-test-cli [file]: File does not exist"
end

puts File.read(file).chomp
