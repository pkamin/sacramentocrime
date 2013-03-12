def insertToPostgresql(filename)
  if !File.exist?(filename)
   
  else
    conn = PGconn.connect( :hostaddr=>"127.0.0.1", :port=>3000, :dbname=>"kamyk", :user=>"kamyk", :password=>'kamyk')
    csv = File.open(filename)
    header = csv.readline().split(",")
    cmd = "DROP TABLE IF EXISTS SacramentocrimeJanuary2006"
    conn.exec(cmd)
    cmd = "CREATE TABLE SacramentocrimeJanuary2006 (cdatetime varchar ,address varchar," + 
      "district varchar, beat varchar, grid varchar, crimedescr text, ucr_ncic_code varchar, latitude double precision, longitude double precision)"
    conn.exec(cmd)
    cmd = "COPY SacramentocrimeJanuary2006 FROM '" + filename + "' DELIMITERS ',' CSV"
    #system(cmd)
    csv.close()
  end  
end

insertToPostgresql("C:/Users/kamyk/Desktop/program_nosql/SacramentocrimeJanuary2006.csv")