library(rvest)
library(httr)

alfred <- read_html('https://alfred.stlouisfed.org/categories')
alLinks <- alfred %>% html_nodes('a') %>% html_attr('href')
dataLinks <- paste0('https://alfred.stlouisfed.org', alLinks[grepl('category?', alLinks, fixed=T)])
