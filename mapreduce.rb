require 'rubygems'
require 'mongo'

include Mongo

#var map = function() { emit(this.district, 1); }
#var reduce = function(key, values) { var total = 0; for (index in values) { total += 1; } return total;}
#db.crimes.mapReduce(map, reduce, { out: "map_reduce_example" })

def mongoMapReduce()
  @client = MongoClient.new('localhost', 27017)
  @db     = @client['test']
  @crimes   = @db['crimes']
  map = "function() { emit(this.district, 1); }"
  reduce = "function(key, values) { var total = 0; for (index in values) { total += values[index]; } return total;}"
  @results = @crimes.map_reduce(map, reduce, :out => "mr_results")
  puts "ok"
  @results.find().each { |item| puts item }
end

def couchDBMapReduce()
  map = "function() { emit(this.author, {votes: this.votes}); }"
  reduce = "function(key, values) { " +
  "var sum = 0; " +
  "values.forEach(function(doc) { " +
  " sum += doc.votes; " +
  "}); " +
  "return {votes: sum}; " +
  "};"
  #@results = @comments.map_reduce(map, reduce, :out => "mr_results")
  #@results.find().to_a  
end

mongoMapReduce()
couchDBMapReduce()