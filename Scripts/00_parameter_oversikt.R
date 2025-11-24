# Oversikt av de API spørring parameterne
library(pacman)

p_load(char = c("httr2","rjstat","sparklyr","tidyverse","here","openxlsx"))


# Tables -----------------------------------------------------------------
## dekonstrukt spørringene for enklere oppdatering
## lagre parametrene til valueCode og verdiIndeksene til hver valuecode parameter slik som
## de kan oppdateres før url gjenbygging på en enkelt nåk måte (kansje som list av lists?)
bi_tabeller<- read.xlsx(xlsxFile = here("SP_tabeller","tabeller.xlsx"))

api_tabeller<- bi_tabeller %>%
  filter(fra_api == 1) %>% 
  select(tabell_nummer,api_sporring) %>% 
  distinct(tabell_nummer,.keep_all = T)

##api spørring dekonstruksjon
api_tabeller<- api_tabeller %>% 
  mutate(tabell_base_url = str_split_i(string = api_sporring, pattern = "\\?",i=1))

##test

split_querry<- function(sporring){
    
  querryTabel = str_split_i(sporring,pattern = "\\?",i=1) %>% 
    str_extract(string = .,pattern = "[[:digit:]]{5}")
  
  querryParams = str_split_i(sporring,pattern = "\\?",i=2)
  
  querryParts= strsplit(x = querryParams, split = "&",fixed = T)[[1]]
  
  QuerryPartsTibble = tibble(
    key = sub("=.*$","",querryParts),
    value = utils::URLdecode(sub("^[^=]*=", "", querryParts))
  ) 
    
  keyPairs= str_match(string = QuerryPartsTibble$key,pattern = "^(.*?)\\[(.*?)\\]$")[,2:3]
  
  keyTibble = tibble(Selector = keyPairs[,1],Dimension = keyPairs[,2]) %>% 
    mutate(Dimension = replace_na(Dimension,"lang"))
  
  keyTibble<- bind_cols(keyTibble,QuerryPartsTibble["value"]) %>% 
    mutate(Tabell = querryTabel)
  

}


API_parameterne<- bi_tabeller %>% 
  filter(fra_api==1) %>% 
  select(tabell_nummer,api_sporring) %>% 
  distinct(tabell_nummer,.keep_all = T) %>% 
  pull(api_sporring) %>% 
  map(.x = ., ~split_querry(.x)) %>% 
  purrr::list_rbind()

wb<- createWorkbook()

addWorksheet(wb,"tabell_oversikt")
writeData(wb,"tabell_oversikt",bi_tabeller)
addWorksheet(wb,"parameter_oversikt")
writeData(wb,"parameter_oversikt",API_parameterne)

saveWorkbook(wb,here("SP_tabeller","bi_tabeller.xlsx"),overwrite = T)

