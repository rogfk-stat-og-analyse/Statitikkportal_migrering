#Denne skal til fabric som notatbok


# setup ------------------------------------------------------------------

library(pacman)

p_load(char = c("httr2","rjstat","sparklyr","tidyverse","here","openxlsx","SparkR"))


# data -------------------------------------------------------------------

tabell_oversikt<- read.xlsx(here("Sp_tabeller","bi_tabeller.xlsx"),sheet = 2)


# test -------------------------------------------------------------------

url<-"https://data.ssb.no/api/pxwebapi/v2/tables"

tabellene<- unique(tabell_oversikt$Tabell)

#reconstitute request
tabell_df<- tabell_oversikt %>% filter(Tabell == tabellene[1]) %>% 
  mutate(paramNames = if_else(is.na(Selector),Dimension,paste0(Selector,"[",Dimension,"]")))

api_params = setNames(as.list(tabell_df$value),tabell_df$paramNames)
tmp<- tempfile(fileext = ".parquet")  
api_req<- request(url) %>% 
  req_url_path_append(.,tabellene[1],"data") %>% 
  req_url_query(!!!api_params,outputformat = "parquet") %>% #this can be modified to accommodate other formats. can be parametrized if turned into a function
  req_perform() %>% 
  resp_body_raw() #%>% 
  #writeBin(object = .,con = tmp)# it might be wiser to hold responses in a list first and then itterate over the list to write a tmp.parqeuet

#####################################################################################
#There are several other methods of reading a parquet file as well.                 #
# This just seems slightly more convenient. See the notes below for an alternative  #
#sparkR.session()#if SparkR is not initiallized already                             #
#tabell_delta<- SparkR::read.df(tmp,source = "parquet")                             #
#saveAsTable(sdf,paste0("bronze_",tabellene[i]),source = "delta",mode = "overwrite")#
#####################################################################################


# notes ------------------------------------------------------------------

## alternativ data storage methods

### 1)sparklyr

# resp <- req_perform(req)
# tmp  <- tempfile(fileext = ".parquet")
# writeBin(resp_body_raw(resp), tmp)

# sc <- spark_connect(method = "databricks")  # or appropriate Fabric method
# sdf <- spark_read_parquet(sc, name = "tmp_07459", path = tmp)

# # Write Delta; if sparklyr delta helpers aren’t available, use Spark SQL
# dbExecute(sc, "DROP TABLE IF EXISTS bronze_07459")
# dbWriteTable(sc, "bronze_07459", sdf)  # may create as parquet, so prefer SQL:

# DBI::dbExecute(sc, "
#   CREATE TABLE bronze_07459
#   USING delta
#   AS SELECT * FROM tmp_07459
# ")


## About api querry reconstitution:

# There is no actual need to split the querry
# into components first and then reconstitute
# if the api querries are manually built and 
# registered in the table list. The reason to do it here
# is to 1) validate the parameters, 2) make it slightly easier to update some of the parameters