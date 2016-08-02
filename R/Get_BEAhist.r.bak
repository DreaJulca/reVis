#Clear workspace
rm(list=ls())

library(data.table)
library(parallel)
library(rvest)
library(httr)
#As it turns out, SQL-type queries are much faster!
library(RODBC)
#library(xlsx)
#library(rattle)



userID <- Sys.info()[[7]]
myPath <- paste0('C:/Users/', userID, '/Documents/reVis/')

#Actually, these aren't really needed
#dir.create(myPath)
#dir.create(paste0(myPath, 'xls/'))
#dir.create(paste0(myPath, 'xls/NIPA/'))
#dir.create(paste0(myPath, 'csv/'))
#dir.create(paste0(myPath, 'csv/NIPA'))


archive <- 'http://www.bea.gov/histdata/histChildLevels.cfm?HMI=7'

links <- read_html(archive) %>% html_nodes('a') 

dataLinks <- paste0(
	'http://www.bea.gov/histdata/', 
	links[
		grepl(
			'fileStructDisplay.cfm?HMI=7&amp;DY=', 
			links, 
			fixed=T
		)
	] %>% 
		html_attr('href')
)

cl <- makePSOCKcluster(3*detectCores())
clusterEvalQ(cl, library(rvest))
clusterEvalQ(cl, library(data.table))
clusterExport(cl, 'dataLinks')

dlLinks <- parLapply(cl, dataLinks, function(thisLink){
	#test
	#thisLink <- dataLinks[1]
	thisPage <- read_html(thisLink) 
	theseLinks <- data.table(paste0('http://www.bea.gov',  thisPage %>% html_nodes('td') %>% html_nodes('a') %>% html_attr('href')))
	return(theseLinks)
})

stopCluster(cl)
#Links take form:
#http://www.bea.gov/histdata/Releases/GDP_and_PI/2016/Q1/Second_May-27-2016/UND/Section0ALL_xls.xls

DTLs <- rbindlist(dlLinks)



cl <- makePSOCKcluster(detectCores())
clusterExport(cl, c('DTLs', 'userID'))
clusterEvalQ(cl, library(data.table))
#As it turns out, SQL-type queries are so much faster!
clusterEvalQ(cl, library(RODBC))
#clusterEvalQ(cl, library(xlsx))
#clusterEvalQ(cl, library(parallel))
#clusterEvalQ(cl, library(rattle))


#reVis <- parLapplyLB(cl, DTLs[, V1], function(thisUrl){
reVis <- parLapply(cl, DTLs[, V1], function(thisUrl){
	#Allow more Java heap space
#	options(java.parameters = "-Xmx1000m")

#reVis <- rbindlist(lapply(DTLs[, V1], function(thisUrl){
	#test
	#thisUrl <- DTLs[, V1][1]
	foldLoc <- paste0(
		'C:/Users/',userID,'/Documents/reVis/xls/NIPA',
			strsplit(
				strsplit(
					tolower(thisUrl), 
					'gdp_and_pi', 
					fixed=T)[[1]][2], 
				'section'
				)[[1]][1]
			)
	
	fileLoc <- paste0(
		'C:/Users/',userID,'/Documents/reVis/xls/NIPA',
#			strsplit(
#				'section',
				strsplit(
					tolower(thisUrl), 
					'gdp_and_pi', 
					fixed=T)[[1]][2] 
#				)[1]
			)

		yrFold <- substr(foldLoc, 1, 46)
		qtFold <- substr(foldLoc, 1, 49)
		
		
		yrFoldC <- gsub('reVis/xls', 'reVis/csv', yrFold, fixed=T)
		qtFoldC <- gsub('reVis/xls', 'reVis/csv', qtFold, fixed=T)
		foldLocC <- gsub('reVis/xls', 'reVis/csv', foldLoc, fixed=T)
		#A sort of intermediate location - 
		# won't actuall exist, but we'll use it later for each sheet
		fileLocC <- gsub('reVis/xls', 'reVis/csv', fileLoc, fixed=T)
#We want to do this for CSV area, too
	try(
	if(foldLoc != paste0('C:/Users/',userID,'/Documents/reVis/xls/NIPANA')){
			
		#Creates folders if needed
		dir.create(foldLoc, showWarnings = FALSE, recursive = TRUE)
		dir.create(foldLocC, showWarnings = FALSE, recursive = TRUE)

		
		#Store a local copy if it hasn't already been stored
		if(!file.exists(fileLoc)) {download.file(thisUrl, fileLoc, mode = 'wb');}

		vint <- gsub('/', '', substr(qtFold, 41, 58), fixed=T);
		rCyc <- ifelse(
			nchar(gsub('advance', '', tolower(foldLoc), fixed=T)) < nchar(foldLoc),
			'1', ifelse(
				nchar(gsub('second', '', tolower(foldLoc), fixed=T)) < nchar(foldLoc),
					'2', '3'
				)
			);

		
		#Create connection (using odbcConnectExcel2007 if extension is not .xlsx)
		if(tolower(substr(fileLoc, nchar(fileLoc)-1, nchar(fileLoc))) == 'x'){
			conn <- odbcConnectExcel(fileLoc)
		} else {
			conn <- odbcConnectExcel2007(fileLoc)
		}
		myTabs <- sqlTables(conn)$TABLE_NAME
		dataTabs <- gsub("'", "", myTabs[substr(tolower(myTabs), 1, nchar('contents')) != 'contents'], fixed=T)
		
#Parallelizing on all cores causes failure... this may be where we WANT to use it, though, since reading from xlsx takes 4eva.
#Problem: This doesn't seem to respect the sequential nature of lapply...
#		cl <- makePSOCKcluster(detectCores()/2)
#		clusterExport(cl, c('fileLoc', 'sht'))
#		clusterEvalQ(cl, library(xlsx))
#		fillerList <- parLapply(cl, sht, function(thisSht){
		fillerList <- lapply(dataTabs, function(thisTab){
				#rename this so that it's not the same as tbl		
			csvLoc <- gsub(
				'.xls', 
				paste0('_',thisTab,'.csv'), 
				tolower(fileLocC), 
				fixed=T
			)

			if(file.exists(csvLoc)){
				return('')
			} else {
			tryCatch({
				write.csv(
					sqlQuery(conn, paste0("select * from ['", thisTab, "']")), 
					file = gsub('.xlsx', '.csv',	csvLoc, fixed=T)
				)
			 }, 
				error = function(e) {try(
					write.csv(
						sqlQuery(conn, paste0("select * from ['", thisTab, "']")), 
						file = gsub('.xlsx', '.csv',	csvLoc, fixed=T)
					)
				)},
			 finally = {
				return('')
			})
		}
		})
#		stopCluster(cl)	
#		try(stopCluster(cl))
	})
	return( as.data.table(list()))
})

stopCluster(cl)

rm(reVis)