#library(rvest)
library(httr)
library(jsonlite)
library(data.table)

userID <- Sys.info()[[7]]
fredKey <- beaKey 	<- readLines(paste0('C:/Users/', userID, '/Documents/alfredApiKey.csv'))
alfURI <- 'https://api.stlouisfed.org/fred/'
serURL <- paste0(alfURI, 'release/series?release_id=9&file_type=json&api_key=', fredKey)

httr::set_config( config( ssl_verifypeer = 0L ) )
martsSer <- httr::GET(serURL)
serStr <- httr::content(martsSer, as='text')
serDF <- jsonlite::fromJSON(serStr)

serDT <- data.table::data.table(serDF$seriess)
saSer <- serDT[seasonal_adjustment_short == 'SA' & as.POSIXct(observation_end) > as.POSIXct('2002-01-01')]

updURL <- paste0(alfURI, 'release/dates?release_id=9&file_type=json&api_key=', fredKey)
serDates <- httr::GET(updURL)
datStr <- httr::content(serDates, as='text')
datDF <- jsonlite::fromJSON(datStr)
datDT <- data.table::data.table(datDF$release_dates)[as.POSIXct(date) > as.POSIXct('2002-01-01')]

thisSer <- saSer[1, id]
thisRel <- datDT[1,date]
thisURL <- paste0(
	alfURI, 
	'series/observations?series_id=',
	thisSer,
	'&vintage_dates=', thisRel, 
	'&observation_start=2000-01-01&file_type=json&api_key=', 
	fredKey)
thisSerVin <- httr::GET(thisURL)
vinStr <- httr::content(thisSerVin, as='text')
vinDF <- jsonlite::fromJSON(vinStr)
vinDT <- data.table::data.table(vinDF$observations)


#alfred <- read_html('https://alfred.stlouisfed.org/categories')
#alLinks <- alfred %>% html_nodes('a') %>% html_attr('href')
#dataLinks <- paste0('https://alfred.stlouisfed.org', alLinks[grepl('category?', alLinks, fixed=T)])

