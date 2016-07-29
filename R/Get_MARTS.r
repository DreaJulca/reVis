#Author: Andrea Julca
#Link to census data takes format:
#download.file(http://www2.census.gov/retail/releases/historical/marts/rs1602.xls)
#GET('http://www2.census.gov/retail/releases/historical/marts/rs0001.txt')
library(parallel)
library(data.table)
library(httr)
library(rvest)
library(RCurl)
library(RODBC)

histUrl <- 'http://www2.census.gov/retail/releases/historical/marts/rs'
userID <- Sys.info()[[7]]

cl <- makePSOCKcluster(detectCores())

clusterEvalQ(cl, library(data.table))
clusterEvalQ(cl, library(httr))
clusterEvalQ(cl, library(RODBC))
clusterExport(cl, c('histUrl', 'userID'))

histData <- parLapply(cl, 0:16, function(yr){
	lapply(1:12, function(mo){
	#test
	#yr <- 1; mo <-3;
		txtUrl <- paste0(histUrl, sprintf('%02d', yr), sprintf('%02d', mo), '.txt')
		xlsUrl <- paste0(histUrl, sprintf('%02d', yr), sprintf('%02d', mo), '.xls')
		myPath <- marts/rs'
	})
})