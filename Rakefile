# -*- ruby -*-

require 'rubygems'
require 'hoe'
require './lib/sparrow.rb'

Hoe.new('sparrow', Sparrow::VERSION) do |p|
  p.rubyforge_name = 'Sparrow'
  p.author = 'Alex MacCAw'
  p.email = 'info@eribium.org'
  p.summary = 'Simple file based messagine queue using the memcache protocol'
  # p.description = p.paragraphs_of('README.txt', 2..5).join("\n\n")
  p.url = 'http://code.google.com/p/sparrow'
  p.changes = p.paragraphs_of('History.txt', 0..1).join("\n\n")
  p.extra_deps << ['eventmachine', '>=0.10.0']
end

# vim: syntax=Ruby
