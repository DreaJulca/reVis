# Get just section 2 data - short-term solution
rm(list=ls())

library(data.table)
library(parallel)
library(rvest)
library(httr)
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

DTLs_all <- rbindlist(dlLinks)
DTLs <- DTLs_all[grep('und/section2', tolower(V1), fixed = TRUE)]
DTLs
#For some reason, doesn't recognize V1
#We may want to consider it, but don't actually need this
#undLinks <-  DTLs[,1][grepl('und', DTLs[,1], fixed=T)]

#cl <- makePSOCKcluster(detectCores())
#clusterExport(cl, c('DTLs', 'userID'))
#clusterEvalQ(cl, library(data.table))
#clusterEvalQ(cl, library(xlsx))
#clusterEvalQ(cl, library(parallel))
#clusterEvalQ(cl, library(rattle))

#Can't run parallel if still gathering from all sections...
#reVis <- parLapply(cl, DTLs[, V1], function(thisUrl){
reVis <- lapply(DTLs[, V1], function(thisUrl){
	#test
	#thisUrl <- DTLs[, V1][1]
	foldLoc <- paste0(
		'C:/Users/',userID,'/Documents/reVis/csv/NIPA',
			strsplit(
				strsplit(
					tolower(thisUrl), 
					'gdp_and_pi', 
					fixed=T)[[1]][2], 
				'section'
				)[[1]][1]
			)
	
	fileLoc <- paste0(
		'C:/Users/',userID,'/Documents/reVis/csv/NIPA',
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
	if(foldLoc != paste0('C:/Users/',userID,'/Documents/reVis/csv/NIPANA')){
			
		
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


		#Match Alice's names
		release <- toupper(vint);
		vintage <- rCyc;
		
		#Create empty frame for each

		tmpwb<-loadWorkbook(file=fileLoc);
		sheet<-getSheets(tmpwb);
		#sht<-c(2:length(sheet));


#Parallelizing this causes system failure... this may be where we WANT to use it tho
#		cl2 <- makePSOCKcluster(4)
#		clusterExport(cl2, c('fileLoc', 'sht'))
#		clusterEvalQ(cl2, library(xlsx))
#		clusterEvalQ(cl2, library(rattle))
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
#		stopCluster(cl2)	
	})
})

#stopCluster(cl)

	#If program bombs, just use the area from "Get_BEAhist.r"
	myPath <- paste0('C:/Users/', userID, '/Documents/reVis/xls/NIPA')
	
	allFolds <- list.dirs(path = myPath)
	
	allFiles <- rbindlist(lapply(allFolds, function(thisPath){
		theseFiles <- data.table(list.files(path = thisPath, full.names = TRUE));
		return(theseFiles);
	}));
	
	allCSVs <- allFiles[tolower(substr(V1, nchar(V1)-2, nchar(V1))) == 'csv']
	
	csvDpce <- allCSVs[grep('/und/section2all', tolower(V1), fixed = T)]
	
	cl <- makePSOCKcluster(2*detectCores())
	clusterEvalQ(cl, library(data.table))
	clusterExport(cl, 'csvDpce')
	
	pceHistData <- parLapply(cl, csvDpce[,V1], function(thisFile){
 #In this case, lines 1-6 are meta
# 	 thisFile <-	paste0(
#	 		'C:/users/',
#	 		userID, 
#	 		'/documents/reVis/xls/NIPA/2005/q1/final_june-29-2005/und/section2all_xlssht3.csv'
#	 	)

	readDT <- fread(thisFile, header = FALSE)
	if (dim(readDT) == c(1, 1)){
		warning(paste0('Empty file: ', thisFile))
		return(readDT)
	} else {
	 cleanDT <- readDT[!is.na(V6)]
	 yrs <- t(cleanDT[2])
	 pds <- t(cleanDT[3])
	 if(
	 	unique(pds[5:length(pds)]) == c('1', '2', '3', '4') || 
	 	unique(pds[5:length(pds)]) == c('1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12')
	 ){
	 	colDates <- paste0(yrs[5:length(yrs)], '.', pds[5:length(pds)])
	 } else {
	 	colDates <- yrs[5:length(yrs)]
	 }
	 	
	 data.table::setnames(cleanDT, c(
	 	'XLS_Line', 
	 	'LineNumber', 
	 	'LineDescription', 
	 	'SeriesCode', 
	 	colDates
	 ))
	 nameDT <- readDT[1, V2]
	 cleanDT[, TableName := nameDT]
	 cleanDT[, FileName := thisFile]

	 fnlDT <- cleanDT[!is.na(as.numeric(LineNumber))]
	 
	 return(fnlDT)
	}
})

stopCluster(cl)
#memory.limit(size = 4095)

phDT <- rbindlist(pceHistData, fill=T, use.names=T)
namesArr <- attributes(phDT)$names

noMon <- namesArr[
	!grepl('.5', namesArr, fixed = T) &
	!grepl('.6', namesArr, fixed = T) &
	!grepl('.7', namesArr, fixed = T) &
	!grepl('.8', namesArr, fixed = T) &
	!grepl('.9', namesArr, fixed = T) &
	!grepl('.10', namesArr, fixed = T) &
	!grepl('.11', namesArr, fixed = T) &
	!grepl('.12', namesArr, fixed = T) &
	grepl('.', namesArr, fixed = T) 
]

tab245u <- phDT[grep('Table.2.4.5U', TableName, fixed = T)]

mphDT <- tab245u[
	!is.na(`2002.12`) | 
	!is.na(`2003.12`) | 
	!is.na(`2004.12`) | 
	!is.na(`2005.12`) | 
	!is.na(`2006.12`) | 
	!is.na(`2007.12`)
]

qphDT <- tab245u[!is.na(`2002.1`)] & is.na(`2002.12`)]
aphDT <- tab245u[!is.na(`2002`)]

aNames <- sort(namesArr[!grepl('.', namesArr, fixed = TRUE)])
mNames <- c(sort(namesArr[grepl('.', namesArr, fixed = TRUE)]), aNames[is.na(as.numeric(aNames))])
qNames <- c(sort(noMon), aNames[is.na(as.numeric(aNames))])

aArr <- paste0(
	'ADT <- aphDT[, .(', 
	paste(
		paste0(
			'`', 
			aNames[aNames != 'V1'], 
			'`'
		), 
	collapse = ','),
	')]'
)

qArr <- paste0(
	'QDT <- qphDT[, .(', 
	paste(
		paste0(
			'`', 
			qNames[qNames != 'V1'], 
			'`'
		), 
	collapse = ','),
	')]'
)

mArr <- paste0(
	'MDT <- mphDT[, .(', 
	paste(
		paste0(
			'`', 
			mNames[mNames != 'V1'], 
			'`'
		), 
	collapse = ','),
	')]'
)

eval(parse(text=aArr))
eval(parse(text=qArr))
eval(parse(text=mArr))

QDT[, Vin := gsub('/', '', substr(FileName, 41,52), fixed=T)]
#ifelse(
#	grep('preliminary', tolower(FileName), fixed=T),
#	paste0(gsub('/', '', substr(FileName, 41,49), fixed=T), 'adv'),
#	ifelse(
#		grep('final', tolower(FileName), fixed=T),
#		paste0(gsub('/', '', substr(FileName, 41,49), fixed=T), 'thi'),
#		gsub('/', '', substr(FileName, 41,52), fixed=T)
#	))
#]

QDT[grep('preliminary', tolower(FileName), fixed=T), Vin := paste0(gsub('/', '', substr(FileName, 41,49), fixed=T), 'adv')] 
QDT[grep('final', tolower(FileName), fixed=T), Vin := paste0(gsub('/', '', substr(FileName, 41,49), fixed=T), 'thi')] 

#Gives us 2004Q3 advance estimate; need to use generalizable method for this
QDT[Vin == '2004q3adv', .(LineNumber, LineDescription, SeriesCode, `2004.3`)]


qAvail <- unique(QDT[,paste0('`', gsub('q', '.', substr(Vin, 1, 6)), '`')])

getQAdv <- paste0("qAdv <- QDT[substr(Vin, 7,9)=='adv',.(
	LineNumber, 
	LineDescription, 
	SeriesCode, 
	Vin, ",
	paste(qAvail, collapse = ','),
	 "
)]")

eval(parse(text=getQAdv))

#mArr <- 
#qArr <- 

