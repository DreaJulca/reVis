# Get just section 2 data - short-term solution
rm(list=ls())

library(data.table)
library(parallel)
library(rvest)
library(httr)
#As it turns out, SQL-type queries are so much faster!
library(RODBC)
#library(xlsx)
#library(rattle)



userID <- Sys.info()[[7]]
myPath <- paste0('C:/Users/', userID, '/Documents/reVis/')

dir.create(myPath)
dir.create(paste0(myPath, 'xls/'))
dir.create(paste0(myPath, 'xls/NIPA/'))
dir.create(paste0(myPath, 'csv/'))
dir.create(paste0(myPath, 'csv/NIPA'))


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
####CHANGE LINE BELOW IF YOU WANT DIFFERENT DATASETS!
DTLs <- DTLs_all[grep('und/section2', tolower(V1), fixed = TRUE)]



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


		#Match Alice's names
		release <- toupper(vint);
		vintage <- rCyc;
		
		#Create empty frame for each
		conn <- odbcConnectExcel(fileLoc)
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
		}})
#		stopCluster(cl)	
#		try(stopCluster(cl))
	})
	return( as.data.table(list()))
})

stopCluster(cl)



#SEPARATE PARSER


	#If program bombs, just use the XLS area from "Get_BEAhist.r" instead of CSV
	myPath <- paste0('C:/Users/', userID, '/Documents/reVis/csv/NIPA')
	
	allFolds <- list.dirs(path = myPath)
	
	allFiles <- rbindlist(lapply(allFolds, function(thisPath){
		theseFiles <- data.table(list.files(path = thisPath, full.names = TRUE));
		return(theseFiles);
	}));
	
	allCSVs <- allFiles[tolower(substr(V1, nchar(V1)-2, nchar(V1))) == 'csv']
	
	csvDpce <- allCSVs[grep('/und/section2all', tolower(V1), fixed = T)]
	
	qtrDpce <- csvDpce[grep('20405u qtr', tolower(V1), fixed=T)]
	
	
	
	cl <- makePSOCKcluster(2*detectCores())
	clusterEvalQ(cl, library(data.table))
	clusterExport(cl, 'csvDpce')
	
	pceQtrHist <- parLapply(cl, qtrDpce[,V1], function(thisFile){
 #In this case, lines 1-6 are meta
# 	 thisFile <-	qtrDpce[,V1][1]

	readDT <- fread(thisFile)
	if (dim(readDT) == c(1, 1)){
		warning(paste0('Empty file: ', thisFile))
		return(readDT)
	} else {
		cleanDT <- readDT[!is.na(F3)]
		yrs <- t(readDT[7])
		qtr <- t(readDT[8])
		colDates <- paste0(yrs[5:length(yrs)], 'q', qtr[5:length(qtr)])
			
		data.table::setnames(cleanDT, c(
			'XLS_Line', 
			'LineNumber', 
			'LineDescription', 
			'SeriesCode', 
			colDates
		))
		 #Just return filename.  We'll use it later to do vintaging
		cleanDT[, FileName := thisFile]
		cleanDT[, LineNumber := ifelse(
			as.numeric(XLS_Line) > 8 & as.numeric(XLS_Line) < 344,
			as.numeric(XLS_Line) - 8, 
			ifelse(as.numeric(XLS_Line) > 344,
				as.numeric(XLS_Line) - 7, 
				NA
			)
			)]
		fnlDT <- cleanDT[!is.na(LineNumber)]
		
		return(fnlDT)
	}
})

stopCluster(cl)

#memory.limit(size = 4095)

pceDTq <- rbindlist(pceQtrHist, fill=T, use.names=T)
qNames <- attributes(phDT)$names

pceDTq[, Vin := tolower(gsub('/', '', substr(FileName, 41,52), fixed=T))]
pceDTq[grep('pre', Vin, fixed = T), Vin := gsub('pre', 'adv', Vin, fixed = T)] 
pceDTq[grep('fin', Vin, fixed = T), Vin := gsub('fin', 'thi', Vin, fixed = T)] 

#Gives us 2004Q3 advance estimate; need to use generalizable method for this
pceDTq[Vin == '2004q3adv', .(LineNumber, LineDescription, SeriesCode, `2004q3`)]

#Find available periods where there is both an advance and a third estimate
qVinAv <- sort(unique(pceDTq[, Vin]))
qAdvAv <- paste0('`', substr(sort(unique(pceDTq[substr(Vin, 7, 9) == 'adv', Vin])), 1, 6), '`')
qSecAv <- paste0('`', substr(sort(unique(pceDTq[substr(Vin, 7, 9) == 'sec', Vin])), 1, 6), '`')
qThiAv <- paste0('`', substr(sort(unique(pceDTq[substr(Vin, 7, 9) == 'thi', Vin])), 1, 6), '`')

qAvail <- qAdvAv[qAdvAv %in% qThiAv]


getQAdv <- paste0("qAdv <- pceDTq[substr(Vin, 7,9)=='adv' & 
	!is.na(SeriesCode) & gsub(' ', '', SeriesCode) != '',.(
	LineNumber, 
	LineDescription, 
	SeriesCode, 
	Vin, ",
	paste(qAvail, collapse = ','),
	 "
)]")

getQThi <- paste0("qThi <- pceDTq[substr(Vin, 7,9)=='thi' & 
	!is.na(SeriesCode) & gsub(' ', '', SeriesCode) != '',.(
	LineNumber, 
	LineDescription, 
	SeriesCode, 
	Vin, ",
	paste(qAvail, collapse = ','),
	 "
)]")

eval(parse(text=getQAdv))
eval(parse(text=getQThi))

qAdv[, VinPeriod := substr(Vin, 1, 6)]
qThi[, VinPeriod := substr(Vin, 1, 6)]

#Well as it turns out, our line numbers are not especially retrievable.
#Some codes are repeated in our sets, but their values shouldn't differ.
qAdv[, Code := ifelse(
	substr(SeriesCode, 1, 1) == 'E',
	substr(SeriesCode, 3, 5),
	substr(SeriesCode, 2, 4)
	)]
qThi[, Code := ifelse(
	substr(SeriesCode, 1, 1) == 'E',
	substr(SeriesCode, 3, 5),
	substr(SeriesCode, 2, 4)
	)]

data.table::setkey(qAdv, key = Code, VinPeriod)
data.table::setkey(qThi, key = Code, VinPeriod)

qAdv <- unique(qAdv)
qThi <- unique(qThi)

data.table::setkey(qAdv, key = Code, VinPeriod)
data.table::setkey(qThi, key = Code, VinPeriod)

#If we join as 
qThi[qAdv]


#We see that we want to do [period] - i.[period] to get revision in level
qDiffStr <- paste0('ril', substr(qAvail, 2, 7), ' = as.numeric(', qAvail, ')-as.numeric(i.', substr(qAvail, 2, 7), ')')

getRilQ <- paste0("rilQ <- qThi[qAdv][,.(
	LineNumber, 
	LineDescription, 
	Code, 
	SeriesCode, 
	VinPeriod, 
	KeyVal = paste(Code, VinPeriod),",
	paste(qDiffStr, collapse = ','),
	 "
)]")

eval(parse(text=getRilQ))

setkey(rilQ, key = KeyVal)

unique(rilQ[`ril2004q1` != 0, VinPeriod])
#This is true for all quarters in 2004; however...
unique(rilQ[`ril2014q1` != 0, VinPeriod])

unique(rilQ[, Code])

dateCol <- sort(paste0('ril', substr(qAvail, 2, 7)))

rilQ[,lapply(.SD %in% qRilDataVals, sum), by = KeyVal]

#Let's get rid of those NA vals
qClearNAs <- paste(paste0('rilQ[,', dateCol, ' := ifelse(is.na(', dateCol, '), 0, ', dateCol, ')]'), collapse = ';')
eval(parse(text=qClearNAs))

getSums <- paste0('revDT <- rilQ[, .(',paste(paste0('sum',dateCol,'=sum(',dateCol,')'), collapse = ','),'), by=Code]')
eval(parse(text=getSums))
write.csv(revDT, file=paste0('c:/Users/', userID, '/Documents/revDT.csv'))


