library(parallel)
library(rvest)
library(httr)
library(data.table)
library(xlsx)
library(rattle)

userID <- Sys.info()[[7]]
myPath <- paste0('C:/Users/', userID, '/Documents/reVis/')

dir.create(myPath)
dir.create(paste0(myPath, 'xls/'))
dir.create(paste0(myPath, 'xls/NIPA/'))
dir.create(paste0(myPath, 'csv/'))


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
#For some reason, doesn't recognize V1
#We may want to consider it, but don't actually need this
#undLinks <-  DTLs[,1][grepl('und', DTLs[,1], fixed=T)]

cl <- makePSOCKcluster(detectCores())
clusterExport(cl, c('DTLs', 'userID'))
clusterEvalQ(cl, library(data.table))
clusterEvalQ(cl, library(xlsx))
clusterEvalQ(cl, library(parallel))
clusterEvalQ(cl, library(rattle))


reVis <- parLapplyLB(cl, DTLs[, V1], function(thisUrl){
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
		
	vintres<-data.frame()
		dir.create(yrFold)
		dir.create(qtFold)
		dir.create(foldLoc)

	try(
	if(foldLoc != paste0('C:/Users/',userID,'/Documents/reVis/xls/NIPANA')){
			
		#Store a local copy - helpful for debugging. CAN SUPPRESS IF YOU'VE ALREADY DLed EVERYTHING!
		download.file(thisUrl, fileLoc, mode = 'wb');

		vint <- gsub('/', '', substr(qtFold, 41, 58), fixed=T);
		rCyc <- ifelse(
			nchar(gsub('advance', '', tolower(foldLoc), fixed=T)) < nchar(foldLoc),
			'1', ifelse(
				nchar(gsub('second', '', tolower(foldLoc), fixed=T)) < nchar(foldLoc),
					'2', '3'
				)
			);

		release <- toupper(vint);
		vintage <- rCyc;
		tmpwb<-loadWorkbook(file=fileLoc);
		sheet<-getSheets(tmpwb);
		sht<-c(2:length(sheet));

#Parallelizing this causes system failure
#		cl2 <- makePSOCKcluster(2)
#		clusterExport(cl2, c('fileLoc', 'sht'))
#		clusterEvalQ(cl2, library(xlsx))
#		fillerList <- parLapply(cl2, sht, function(thisSht){
		fillerList <- lapply(sht, function(thisSht){
				#rename this so that it's not the same as tbl		
			write.csv(
				read.xlsx(fileLoc,thisSht), 
				file = gsub(
					'.xls', 
					paste0('sht',thisSht,'.csv'), 
					tolower(fileLoc), 
					fixed=T
				)
			)
			return('')
		})
	})
})

stopCluster(cl)



