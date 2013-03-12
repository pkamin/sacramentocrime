def mongoMapReduce()
  @client = MongoClient.new('localhost', 27017)
  @db     = @client['test']
  @coll   = @db['crimes']
  map = "function() { emit(this.cdatetime, {district: this.district}); }"
  reduce = "function(key, values) { " +
  "var sum = 0; " +
  "values.forEach(function(doc) { " +
  " sum += doc.district; " +
  "}); " +
  "return {district: sum}; " +
  "};"
  @results = @client.map_reduce(map, reduce, :out => "mr_results")
  puts "ok"
  @results.find()
end

def couchDBMapReduce()
  map = "function() {" + 
    + "emit(this.author, {votes: this.votes}); }"
  reduce = "function(key, values) { " +
  "var sum = 0; " +
  "values.forEach(function(doc) { " +
  " sum += doc.votes; " +
  "}); " +
  "return {votes: sum}; " +
  "};"
  @results = @comments.map_reduce(map, reduce, :out => "mr_results")
  @results.find().to_a  
end

mongoMapReduce()
couchDBMapReduce()