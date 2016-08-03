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
	#yr <- 0; mo <-4;
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
			pdfPath <- paste0(
				xlsPath, 
				'adv', 
				sprintf('%02d', yr), 
				sprintf('%02d', mo), 
				'.pdf'
			)
			if(!file.exists(myPath)){
				tryCatch(
					{download.file(txtUrl, myPath, mode='wb')},
					error = function(e) {
						download.file(
							gsub(
								'.txt', '.pdf', 
								gsub('/rs', '/adv', txtUrl, fixed=T), 
								fixed=T
							), 
							pdfPath, 
							mode='wb'
						)
					},
					finally = {return('')}

				)
			}
			
			csvFileA <- paste0(
				csvPath, 
				'rs',	
				sprintf('%02d', yr), 
				sprintf('%02d', mo), 
				'_txtTable1A.csv'
			)
			
			csvFileB <- paste0(
				csvPath, 
				'rs',	
				sprintf('%02d', yr), 
				sprintf('%02d', mo), 
				'_txtTable1B.csv'
			)
			
			AllTxt <- readLines(myPath)
			strLnA <- grep('TABLE 1A', toupper(AllTxt), fixed=T)
			strLnB <- grep('TABLE 1B', toupper(AllTxt), fixed=T)
			#strLn2 <- grep('TABLE 2', AllTxt, fixed=T)
			endLn <- grep('(*) Advance', AllTxt, fixed=T)
			#endLn2 <- grep('(*) Advance', AllTxt, fixed=T)
			
			endLn <- ifelse(exists('endLn'), endLn, length(AllTxt))
			strLnA <- ifelse(exists('strLnA'), strLnA, 1)
			strLnB <- ifelse(exists('strLnB'), strLnB, 1)
			
			try(writeLines(AllTxt[strLnA:(endLn[1]-1)], csvFileA), silent=TRUE)
			try(writeLines(AllTxt[strLnB:(endLn[2]-1)], csvFileB), silent=TRUE)
			
			
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
					{
						download.file(xlsUrl, myPath, mode='wb')
					},
					error = function(e) {
						download.file(
							gsub('xls', 'pdf',
								gsub('/rs', '/adv', xlsUrl, fixed=T),
								fixed=T
							), 
							pdfPath, 
							mode='wb'
						)
					},
					finally = {return('')}
					
				)
			}
			csvFilePath <- paste0(
				csvPath, 
				'rs',	
				sprintf('%02d', yr), 
				sprintf('%02d', mo), 
				'.xls'
			)
			#Create connection (using odbcConnectExcel2007 if extension is not .xlsx)
			if(tolower(substr(myPath, nchar(myPath)-1, nchar(myPath))) == 'x'){
				conn <- odbcConnectExcel(myPath)
			} else {
				conn <- odbcConnectExcel2007(myPath)
			}
			myTabs <- sqlTables(conn)$TABLE_NAME
			dataTabs <- gsub("'", "", myTabs[substr(tolower(myTabs), 1, nchar('cover')) != 'cover'], fixed=T)
			dataTabs <- gsub("'", "", dataTabs[substr(tolower(dataTabs), nchar(dataTabs) - nchar('print_area') + 1, nchar(dataTabs)) != 'print_area'], fixed=T)
			#Actually, we only really care about that first tab for now... but we'll get them all
			fillerList <- lapply(dataTabs, function(thisTab){
				#test
				# thisTab <- dataTabs[1]
				#rename this so that it's not the same as tbl		
				csvLoc <- gsub(
					'.xls', 
					paste0('_',thisTab,'.csv'), 
					tolower(csvFilePath), 
					fixed=T
				)
#				if(file.exists(csvLoc)){
#					return('')
#				} else {
					tryCatch({
						thisQry <- data.table(sqlQuery(conn, paste0("select * from ['", thisTab, "']")))
						tabStrName <- attributes(thisQry)$names[1]
						data.table::setnames(thisQry, old=c(tabStrName), new=c('F1'))
						thisQry[!(is.na(F1)&is.na(F2)&is.na(F3)&is.na(F4)), TableName := tabStrName]
						write.csv(
							#sqlQuery(conn, paste0("select * from ['", thisTab, "']")), 
							thisQry[!is.na(TableName)],
							file = gsub('.csvx', '.csv',	csvLoc, fixed=T)
						)
					 }, 
						error = function(e) {try(
							write.csv(
								sqlQuery(conn, paste0("select * from ['", thisTab, "']")), 
								file = gsub('.csvx', '.csv',	csvLoc, fixed=T)
							)
						)},
					 finally = {
						return('')
					})
				
			})
		}
	})
})

	myPath <- paste0('C:/Users/', userID, '/Documents/GitHub/reVis/csv/marts')
	
	allFolds <- list.dirs(path = myPath)
	
	allFiles <- rbindlist(lapply(allFolds, function(thisPath){
		theseFiles <- data.table(list.files(path = thisPath, full.names = TRUE));
		return(theseFiles);
	}));
	
	allCSVs <- allFiles[tolower(substr(V1, nchar(V1)-2, nchar(V1))) == 'csv']
	
	csvDpce <- allCSVs[grep('/und/section2all', tolower(V1), fixed = T)]
	
#QUARTERLY DATA first
	qtrDpce <- csvDpce[grep('20405u qtr', tolower(V1), fixed=T)]


stopCluster(cl)

