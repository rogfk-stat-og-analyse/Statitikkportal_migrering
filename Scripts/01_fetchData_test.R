
# test -------------------------------------------------------------------

url<-"https://data.ssb.no/api/pxwebapi/v2/tables"

tabellene<- unique(tabell_oversikt$Tabell)

# Test: reconstitute request

tabell_df<- tabell_oversikt %>%
  dplyr::filter(Tabell == tabellene[1]) %>% 
    mutate(
      paramNames = if_else(is.na(Selector), Dimension, paste0(Selector, "[", Dimension, "]")),
      value = as.character(value),                    # never factors
      value = stringi::stri_trim_both(value),                  # trim ASCII + NBSP
      value = if_else(grepl("^valueCodes\\[", paramNames),
                      gsub("\\s+", "", value), value) # no spaces inside comma lists
    ) %>% 
  dplyr::mutate(paramNames = if_else(is.na(Selector),Dimension,paste0(Selector,"[",Dimension,"]")))

api_params = setNames(as.list(tabell_df$value),tabell_df$paramNames)
tmp<- tempfile(fileext = ".parquet")  

api_req<- request(url) %>% 
  req_url_path_append(.,tabellene[1],"data") %>% 
  req_url_query(!!!api_params) %>% #this can be modified to accommodate other formats. can be parametrized if turned into a function,outputformat = "parquet"
  req_perform() %>% 
  # resp_body_string() %>% #sanity check on the table. The reconstituted url works as intended 
  # rjstat::fromJSONstat()
  resp_body_raw() %>% 
  writeBin(object = .,con = tmp)# it might be wiser to hold responses in a list first and then itterate over the list to write a tmp.parqeuet
# does piping into a tmp file affect the cost of Fabric in a meaningful way?
