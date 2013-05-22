#!/usr/bin/ruby
###############################################################################
# Copyright (c) 2011 New York University 
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or 
# without modification, are permitted for nonprofit educational 
# purposes provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above 
#    copyright notice, this list of conditions and the following 
#    disclaimer. 
# 2. Redistributions in binary form must reproduce the above 
#    copyright notice, this list of conditions and the following 
#    disclaimer in the documentation and/or other materials 
#    provided with the distribution. 
# 3. All advertising materials mentioning features or use of 
#    this software must display the following acknowledgement: 
#    This product includes software developed by New York 
#    University and its contributors. 
# 4. Neither the name of the University nor the names of its 
#    contributors may be used to endorse or promote products 
#    derived from this software without specific prior written 
#    permission.
# 5. This software is not used nor made available for use as 
#    part of a for-profit service without specific prior written 
#    permission.
#
# THIS SOFTWARE IS PROVIDED BY THE UNIVERSITY AND CONTRIBUTORS 
# ``AS IS'' AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, 
# BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY 
# AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO 
# EVENT SHALL THE UNIVERSITY OR CONTRIBUTORS BE LIABLE FOR ANY
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR 
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, 
# PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, 
# DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED 
# AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT 
# LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) 
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF 
# ADVISED OF THE POSSIBILITY OF SUCH DAMAGE
#
###############################################################################
# $Id$
###############################################################################

require 'cgi'
require 'net/https'
require 'uri'


#global vars used within file. 
#Stoopid Primo not yet implemented

# list of solr fields and their equivalent mappings 
#in nyu-core and oai_dc
MAP_FILE = 'crosswalk_updated.txt'

# list of standard oai verbs
VERB_LIST = 'acceptable_verbs.txt'

#list of error codes and the accompanying text
ERR_LIST = 'err_list.txt'

#list of supported metadata
MD_LIST = ["oai_dc","nyu-core","primo"]

#solr query url
#QUERY_URL = "http://dev-dl-pa.home.nyu.edu/solr_discovery_dev/core0/select/?"
QUERY_URL = "http://dev-discovery.dlib.nyu.edu:8080/solr3_discovery/unioncatalog/select?"

#earliest date query for solr
EARLIEST_DATE = "fq=created&wt=ruby&sort=ds_created%20asc&fl=ds_created"


#returns all approved verbs
#in an array
def get_verbs(verb_list)
    file=File.new(verb_list,'r')
    verbs = Array.new
    v = 0
    while (line = file.gets)
        verbs[v] = line.chomp
        v += 1
    end
    return verbs
end

#populates err file into a hash
def get_err(err_list)
    file=File.new(err_list,'r')
    err = Hash.new
    line_count = 0
    while (line = file.gets)
          if line_count > 0
             err_code,err_stmt = line.split(',')
             err[err_code] = err_stmt.chomp
          end
          line_count = line_count + 1
    end
    return err
end

#prints out error stmt in results
def err_stmt(code,err_hsh)
    if err_hsh.has_key?(code)
       err_msg = err_hsh[code]
       err = '<error code="'+code+'">'+err_msg+'</error>'
       return err
    end
end

#checks given command for various errors
#needs to be better organized & coded
def chk_cmd(verb,verb_list,err_hsh,metadata,from,to,resumption)
   err = "blah"
   if ! verb_list.include?(verb)
       err =  err_stmt('badVerb',err_hsh) 
   elsif verb == "ListSets"
       err = err_stmt('noSetHierarchy',err_hsh)
   elsif verb == "Identify"
         err = verb     
         #no date arguments or metadata argument should be specified
         if from != "" || to != "" || MD_LIST.include?(metadata)
            err = err_stmt('badArgument',err_hsh)
         end
   #we do not support resumptionTokens
   elsif resumption != ""
         err = err_stmt('badResumptionToken',err_hsh)
   #if metadata argument does not match
   #approved metadata list
   elsif ! MD_LIST.include?(metadata)
       err = err_stmt('badArgument',err_hsh)
   #date checks
   #if from and to date values are sent
   elsif from != "" || to != ""
         #check that from and to dates are sent in 
         #the same format
         #from_date_chk = validate_date_format(from)
         #to_date_chk = validate_date_format(to)
         earliest_date = get_earliest_date_stamp
         #ensure from and until values have similar levels of granularity
         #if ((from != "" && to != "") && (from_date_chk != to_date_chk))
         #   err = err_stmt('badArgument',err_hsh)
         #ensure from and until values have correct date format
         #elsif ((from != ""  && from_date_chk == "") || (to != "" && to_date_chk == ""))
         #   err = err_stmt('badArgument',err_hsh)
         #ensure until values is greater than earliest datestamp
         if (to < earliest_date)
            err = err_stmt('noRecordsMatch',err_hsh)
         end
   end
   return err
end

#adds mapping fields to a hash
#equivalent solr,nyu-core, and dc fields are 
#loaded in one record of the hash
def get_map(file)
    file=File.new(file,'r')
    element_map = Hash.new
    line_count = 0
    while (line = file.gets)
          if line_count > 0 
             solr,nyu_core,dc = line.split(' ')
             element_map[solr] = [nyu_core,dc]
          end 
          line_count = line_count + 1
    end
    return element_map 
end

#generates beginiing and ending tags
#for each metadata
def gen_md_rec(md_prefix,tag_type)
   if md_prefix == 'nyu-core' 
      if tag_type == 'start'
         puts '<nyu:NYUCoreRecord xmlns:dc="http://purl.org/dc/elements/1.1/"
                  xmlns:dcterms="http://purl.org/dc/terms/"
                  xmlns:nyu="http://purl.org/nyu/digicat/"
                  xmlns:xml="http://www.w3.org/XML/1998/namespace"
                  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                  xsi:schemaLocation="http://purl.org/nyu/digicat/ http://harper.bobst.nyu.edu/data/nyucore.xsd">'
      elsif tag_type == 'end'
        puts "</nyu:NYUCoreRecord>"
      end
   elsif md_prefix == 'oai_dc' 
      if tag_type == 'start'
         puts '<oai_dc:dc 
          xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/" 
          xmlns:dc="http://purl.org/dc/elements/1.1/" 
          xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
          xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/oai_dc/ 
          http://www.openarchives.org/OAI/2.0/oai_dc.xsd">'
      elsif tag_type == 'end'
          puts '</oai_dc:dc>'
      end
   elsif md_prefix == 'primo' 
      if tag_type == 'start'
         puts '<oai_dc:dc xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/"
          xmlns:dc="http://purl.org/dc/elements/1.1/"
          xmlns:dcterms="http://purl.org/dc/terms/"
          xmlns:nyu="http://purl.org/nyu/digicat/"
          xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
          xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/oai_dc/ http://www.openarchives.org/OAI/2.0/oai_dc.xsd">'
      elsif tag_type == 'end'
          puts '</oai_dc:dc>'
      end
   end
end

#specifies which index of hash to use
#based on md prefix
#The hash contains fields loaded in
#from mapping file
def map_result(md_prefix)
    index=0
    if md_prefix == 'oai_dc' 
       index = 1
    end  
    return index
end

#generates main record
def gen_oai_rec(element_map,rsp,verb,md_prefix)
       puts "<#{verb}>" 
    rsp['response']['docs'].each { |doc|
       puts "<record>" 
       puts "<header>" 
       puts "<identifier>#{doc['dc_identifier']}</identifier>" 
       puts "<datestamp>#{doc['created']}</datestamp>"
       puts "</header>" 
       puts "<metadata>" 
       #generates beginning xml fragment
       gen_md_rec(md_prefix,'start')
        #maps fields depending on metadata prefix
        #position in hash
       index = map_result(md_prefix)
       element_map.each{|solr,map_array|
          if doc.key?(solr) and not(map_array[index].nil?)
             puts "<#{map_array[index]}>#{doc[solr]}</#{map_array[index]}>"
          end
       }
        #ending xml tag
        gen_md_rec(md_prefix,'end')
        puts "</metadata>" 
        puts "</record>" 
    }
        puts "</#{verb}>" 
       
end

#consistent date formatting
def format_date(value)
sprintf("%02d",value)
end

#returns datestamp
def get_date
time = Time.new
month = format_date(time.month)
day = format_date(time.day) 
hour = format_date(time.hour) 
min = format_date(time.min) 
sec = format_date(time.sec) 

datestamp = "#{time.year}-#{month}-#{day}T#{hour}:#{min}:#{sec}Z"

return datestamp
end

def generate_request_header(verb,addn_params)
puts '<request verb='+'"'+verb+'"' + addn_params + '>http://dev-dl-pa.home.nyu.edu/uc-oai-demo/</request>'

end

#this is for a query which does not
#contain a from date parameter
#The repository is supposed to implement 
#that query as from the first records ingested
def get_earliest_date_stamp
    date_query = QUERY_URL + EARLIEST_DATE
    dates_result = Net::HTTP.get(URI.parse(date_query))
    dates = eval(dates_result)
    earliest_date = dates['response']['docs'][0]['ds_created']
   
    return earliest_date
end

#for the Identify verb
def identify_result
earliest_date = get_earliest_date_stamp
puts "<Identify>
    <repositoryName>NYU Libraries Union Catalog</repositoryName> 
    <baseURL>http://dev-dl-pa.home.nyu.edu/uc-oai-demo/oai/</baseURL>
    <protocolVersion>2.0</protocolVersion>
    <adminEmail>esha@nyu.edu</adminEmail>
    <earliestDatestamp>#{earliest_date}</earliestDatestamp>
    <deletedRecord>no</deletedRecord>
    <granularity>YYYY-MM-DDThh:mm:ssZ</granularity>
    </Identify>"
end

#generates xml fragment for the root of the record
def add_root_element
puts "Content-Type: text/xml"
puts # this is necessary
puts '<?xml version="1.0" encoding="UTF-8"?>'
puts '<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/" 
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/
         http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd">'
datestamp = get_date 
puts "<responseDate>#{datestamp}</responseDate>"
end

#should pass one of the two regexps
def validate_date_format(date)
    chk_date=""
    if (date =~ /\d{4}-\d{2}-\d{2}T\d{2}\:\d{2}\:\d{2}Z/)
        chk_date = date
    elsif (date =~ /\d{4}-\d{2}-\d{2}/)
        d = date.scan(/\d{4}-\d{2}-\d{2}/)
        chk_date = "#{d}T00:00:00Z"
    end

    return chk_date

end

#creates solr queries for various permutations
#of date arguments sent by user
def list_records_header(from_date,to_date,md)
    d_query = ""
    response_param = ""
     date_query = ""
     from_date =  validate_date_format(from_date)
     to_date =  validate_date_format(to_date)
     #if from date is populated, but not to date
     if (from_date !=  "" && to_date == "")
       to_date = get_date
       date_query = " ds_created:[#{from_date} TO #{to_date}] OR ds_changed:[#{from_date} TO #{to_date}]"
       response_param = ' from='+'"'+from_date+'"'
     #from and to are populated
     elsif (from_date !=  "" && to_date != "")
       date_query = " ds_created:[#{from_date} TO #{to_date}] OR ds_changed:[#{from_date} TO #{to_date}]"
       response_param = ' from="'+from_date+'" until='+'"'+to_date+'"'
     #only to is populated
     elsif (from_date == "" && to_date != "")
       from_date = get_earliest_date_stamp
       date_query = " ds_created:[#{from_date} TO #{to_date}] OR ds_changed:[#{from_date} TO #{to_date}]"
       response_param = ' until='+'"'+to_date+'"'
     #neither is populated
     elsif (from_date == "" && to_date == "")
       date_query = ""
     end
     #this is for the xml fragment in the oai response
     if md != ""
        response_param += ' metadataPrefix="'+md+'"'
     end
    d_query = date_query
    str = Array.new
    #sending array back for solr processing
    #and for inclusion into xml fragment  
    str = [from_date,to_date,d_query,response_param]
    return str
end

#main processing
cgi = CGI.new("html4")

#loading hash with mappings from file
element_map = get_map(MAP_FILE)

#getting params
oai_verb = URI.escape(cgi["verb"])
md_prefix = URI.escape(cgi["metadataPrefix"])
resumption = URI.escape(cgi["resumptionToken"])
#grabbing query fragment based on args
query_str = list_records_header(URI.escape(cgi["from"]),URI.escape(cgi["until"]),md_prefix)

#setting from, until, date query for solr and additional params for the oai response xml
from_date = query_str[0] 
to_date = query_str[1] 
date_query = query_str[2] 
addn_params = query_str[3]
solr_verb = ""
#loading approved verbs from file
verb_list=get_verbs(VERB_LIST)
#loading error codes and statements from file
err_list = get_err(ERR_LIST)

#generating oai response
add_root_element
generate_request_header(oai_verb,addn_params)
chk_err = chk_cmd(oai_verb,verb_list,err_list,md_prefix,from_date,to_date,resumption)
if chk_err.include?'Identify'
   identify_result
elsif ! chk_err.include?'error'
     if oai_verb == "ListRecords"
        query = "fq=#{date_query}" 
        solr_verb = URI.escape(query)
      end
   myQuery = QUERY_URL + solr_verb +"&wt=ruby&rows=500"
   res = Net::HTTP.get(URI.parse(myQuery))
   rsp = eval(res)

   gen_oai_rec(element_map,rsp,oai_verb,md_prefix)
else
  puts chk_err 
end
puts "</OAI-PMH>"
