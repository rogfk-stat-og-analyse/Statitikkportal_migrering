#Denne skal til fabric som notatbok


# setup ------------------------------------------------------------------
##SparkR is archieved on CRAN, installing it from the tarball for testing purposes
##SparkR is natively available on Fabric
# sparkR_url<- "https://cran.r-project.org/src/contrib/Archive/SparkR/SparkR_3.1.2.tar.gz"
# sparkR_tar<- "SparkR_3.1.2.tar.gz"
# download.file(url = sparkR_url,destfile = sparkR_tar)
# install.packages(pkgs = sparkR_tar,type = "source",repos = NULL)
# unlink(sparkR_tar)
# rm(list = ls())
################
library(pacman)

p_load(char = c("httr2","tidyverse","here","openxlsx"))
#util pack: "sparklyr","SparkR","rjstat"

# data -------------------------------------------------------------------

tabell_oversikt<- read.xlsx(here("Sp_tabeller","bi_tabeller.xlsx"),sheet = 2)


#mapping it
excel_dir = ""
table_dir = ""
paramDF<-read.xlsx(here("Sp_tabeller","bi_tabeller.xlsx"),sheet = 2)

tabellene<- unique(tabell_oversikt$Tabell)

urlParams<- map(.x = tabellene, ~{
  paramTable = paramDF %>% 
    filter(Tabell == .x) %>% 
      mutate(
        paramNames = if_else(is.na(Selector),Dimension,paste0(Selector,"[",Dimension,"]")),
        value = as.character(value),# never factors
        value = stringi::stri_trim_both(value),# trim ASCII + NBSP + UNICODE WS
        value = gsub("\\s+", "", value)# no spaces inside comma lists
          
                         ) 

  api_params = setNames(as.list(paramTable$value),paramTable$paramNames)
    
})

apiTabellene <- walk2(.x = urlParams,.y = tabellene,~{
  tmp<- tempfile(fileext = ".parquet")

  Sys.sleep(15)#sleep the script 15 seconds to give API some breathing room

  api_send<- request("https://data.ssb.no/api/pxwebapi/v2/tables") %>% 
  req_url_path_append(.,.y,"data") %>% 
  req_url_query(!!!.x,outputformat = "parquet") %>%#this can be modified to accommodate other formats. can be parametrized if turned into a function,outputformat = "parquet" is easier to pipe into a delta table
  req_retry(max_tries = 5, backoff = ~ runif(1, 2, 6))  
  
  api_reg<-req_perform(api_send) 
  
  if(resp_status(api_req)==200L ){
  
    if (!length(resp_body_raw(api_req))) {
      warning(sprintf("Tabell %s returned empty body", .y))
      return(invisible(NULL))
    }

  apiResp<- api_req%>% 
  resp_body_raw() %>% 
  writeBin(object = .,con = tmp)
  
  tabell_delta<- SparkR::read.df(tmp,source = "parquet")                             #
  
  SparkR::saveAsTable(
    df        = sdf,
    tableName = paste0("bronze_", .y),
    source    = "delta",
    mode      = "overwrite"
  )
  }else{
    
      warning(sprintf("Tabell %s returned %s", .y, resp_status(api_req)))
      return(invisible(NULL))
  }
  
},.progress = T)



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