#require 'postgres'
require 'pg'
require 'json'
require 'net/http'

module Couch

  class Server
    def initialize(host, port, options = nil)
      @host = host
      @port = port
      @options = options
    end

    def delete(uri)
      request(Net::HTTP::Delete.new(uri))
    end

    def get(uri)
      request(Net::HTTP::Get.new(uri))
    end

    def put(uri, json)
      req = Net::HTTP::Put.new(uri)
      req["content-type"] = "application/json"
      req.body = json
      request(req)
    end

    def post(uri, json)
      req = Net::HTTP::Post.new(uri)
      req["content-type"] = "application/json"
      req.body = json
      request(req)
    end

    def request(req)
      res = Net::HTTP.start(@host, @port) { |http|http.request(req) }
      unless res.kind_of?(Net::HTTPSuccess)
        handle_error(req, res)
      end
      res
    end

    private

    def handle_error(req, res)
      e = RuntimeError.new("#{res.code}:#{res.message}\nMETHOD:#{req.method}\nURI:#{req.path}\n#{res.body}")
      raise e
    end
  end
end

def createjson(filename)
  puts filename
  if !File.exist?(filename)
  else
    csv = File.open(filename)
    json = File.new(filename.split(".")[0] + ".json", "w")
    header = csv.readline().split(",")
    csv.each_line do |line|
      jsonEntry = Hash.new()
      line = line.split(",")
      line.each do |item|
        jsonEntry[header[line.index(item)].strip] = item.strip
      end
      json.write(jsonEntry.to_json)
      json.write("\n")
    end
    json.close()
    csv.close()
  end
end

def insertToMongoDB(filename)
  if !File.exist?(filename)
   
  else
    cmd = "C:/Users/kamyk/Desktop/databases/mongo/mongodb-win32-x86_64-2.2.3/bin/mongoimport --host localhost --db test --collection crimes --type json --file " + filename
    system(cmd)
    puts "after mongo"
  end
end

def insertToCouchDB(filename)
  if !File.exist?(filename)
   
  else
    #puts "start"
    server = Couch::Server.new("localhost", "5984")
    #puts "sadasd"
    server.delete("/sample/")
    server.put("/sample/", "")
    system("curl -X PUT -d " + filename + " http://admin:secret@127.0.0.1:5984/sample/document_id")
    #puts "poipoiop"
    #server.put("/sample/document_id", filename)
    #puts "ok"
  end  
end

def insertToElasticSearch(filename)
  if !File.exist?(filename)
   
  else
    cmd = ""
    #system(cmd)
  end  
end

createjson("C:/Users/kamyk/Desktop/program_nosql/SacramentocrimeJanuary2006.csv")
insertToMongoDB("C:/Users/kamyk/Desktop/program_nosql/SacramentocrimeJanuary2006.json")
insertToCouchDB("C:/Users/kamyk/Desktop/program_nosql/SacramentocrimeJanuary2006.json")
insertToElasticSearch("C:/Users/kamyk/Desktop/program_nosql/SacramentocrimeJanuary2006.json")
