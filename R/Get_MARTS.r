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
xlsPath <- paste0(
	'c:/users/',
	userID,
	'/Documents/GitHub/reVis/xls/marts/'
) 
csvPath <- paste0(
	'c:/users/',
	userID,
	'/Documents/GitHub/reVis/csv/marts/'
) 

dir.create(xlsPath)
dir.create(csvPath)

cl <- makePSOCKcluster(detectCores())

clusterEvalQ(cl, library(data.table))
clusterEvalQ(cl, library(httr))
clusterEvalQ(cl, library(RODBC))
clusterExport(cl, c('histUrl', 'userID', 'xlsPath', 'csvPath'))

histData <- parLapply(cl, 0:16, function(yr){
	lapply(1:12, function(mo){
	#test
	#yr <- 1; mo <-3;
		txtUrl <- paste0(histUrl, sprintf('%02d', yr), sprintf('%02d', mo), '.txt')
		thisResp <- GET(txtUrl)
		if(thisResp$status_code == 200){
			myPath <- paste0(
				xlsPath, 
				'rs', 
				sprintf('%02d', yr), 
				sprintf('%02d', mo), 
				'.txt'
			)
			if(!file.exists(myPath)){
				tryCatch(
					{download.file(txtUrl, myPath, mode='wb')},
					error = function(e) {
						download.file(
							gsub('.txt', '.pdf', txtUrl, fixed=T), 
							gsub('.txt', '.pdf', myPath, fixed=T), 
							mode='wb'
						)
					},
					finally = {return('')}
				)
			}
		} else {
			xlsUrl <- paste0(histUrl, sprintf('%02d', yr), sprintf('%02d', mo), '.xls')
			myPath <- paste0(xlsPath,
				'rs', 
				sprintf('%02d', yr), 
				sprintf('%02d', mo), 
				'.xls'
			)
			if(!file.exists(myPath)){
				tryCatch(
					{download.file(xlsUrl, myPath, mode='wb')},
					error = function(e) {
						download.file(
							gsub('.xls', '.pdf', txtUrl, fixed=T), 
							gsub('.xls', '.pdf', myPath, fixed=T), 
							mode='wb'
						)
					},
					finally = {return('')}
					
				)
			}
		}
	})
})
stopCluster(cl)
