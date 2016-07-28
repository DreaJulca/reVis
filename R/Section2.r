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

	readDT <- fread(thisFile, header = FALSE)
	if (dim(readDT) == c(1, 1)){
		warning(paste0('Empty file: ', thisFile))
		return(readDT)
	} else {
		cleanDT <- readDT[!is.na(V3)]
		yrs <- t(readDT[8])
		qtr <- t(readDT[9])
		colDates <- paste0(yrs[5:length(yrs)], 'q', qtr[5:length(qtr)])
		tabName <- gsub('#', '.', readDT[1,V2], fixed=T)
		data.table::setnames(cleanDT, c(
			'XLS_Line', 
			'LineNumber', 
			'LineDescription', 
			'SeriesCode', 
			colDates
		))
		 #Just return filename.  We'll use it later to do vintaging
		cleanDT[, FileName := thisFile]
		cleanDT[, TableName := tabName]
		cleanDT[, LineNumber := as.numeric(LineNumber)]
		fnlDT <- cleanDT[!is.na(LineNumber)]
		
		return(fnlDT)
	}
})

stopCluster(cl)

#memory.limit(size = 4095)

pceDTq <- rbindlist(pceQtrHist, fill=T, use.names=T)
qNames <- attributes(pceDTq)$names

pceDTq[, Vin := tolower(gsub('/', '', substr(FileName, 41,52), fixed=T))]
pceDTq[grep('pre', Vin, fixed = T), Vin := gsub('pre', 'sec', Vin, fixed = T)] 
pceDTq[grep('fin', Vin, fixed = T), Vin := gsub('fin', 'thi', Vin, fixed = T)] 
pceDTq[
	!is.na(SeriesCode) & gsub(' ', '', SeriesCode) != '',  
	Code := ifelse(
		substr(SeriesCode, 1, 1) == 'E',
		substr(SeriesCode, 3, 5),
		substr(SeriesCode, 2, 4)
	)] 

pceDTq[!is.na(Code)]
#Gives us 2004Q3 advance estimate; need to use generalizable method for this
pceDTq[Vin == '2004q3adv', .(LineNumber, LineDescription, SeriesCode, `2004q3`)]


#Find available periods where there is both an advance and a third estimate
#This was my original approach, but there are more second estimates than I thought
qVinAv <- sort(unique(pceDTq[, Vin]))
qtrsAv <- sort(unique(pceDTq[, substr(Vin, 1, 6)]))
qAdvAv <- paste0('`', substr(sort(unique(pceDTq[substr(Vin, 7, 9) == 'adv', Vin])), 1, 6), '`')
qSecAv <- paste0('`', substr(sort(unique(pceDTq[substr(Vin, 7, 9) == 'sec', Vin])), 1, 6), '`')
qThiAv <- paste0('`', substr(sort(unique(pceDTq[substr(Vin, 7, 9) == 'thi', Vin])), 1, 6), '`')

qAvail <- qAdvAv[qAdvAv %in% qThiAv]


outDT <- rbindlist(lapply(2:length(qtrsAv), function(indx){
	#test
	# indx <- 2
	thisQtr <- qtrsAv[indx]
	prevQtr <- qtrsAv[(indx-1)]
	
	thisAdv <- unique(pceDTq[
		Vin == paste0(thisQtr, 'adv'), 
		.(
			Code, 
			AdvLvl = eval(
				parse(
					text=paste0('as.numeric(`', thisQtr, '`)')
				)
			), 
			AdvPct = eval(
				parse(
					text=paste0('(as.numeric(`', thisQtr, '`)/as.numeric(`', prevQtr, '`))-1')
				)
			)
		)
	])
	
	thisSec <- unique(pceDTq[
		Vin == paste0(thisQtr, 'sec'), 
		.(
			Code, 
			SecLvl = eval(
				parse(
					text=paste0('as.numeric(`', thisQtr, '`)')
				)
			), 
			SecPct = eval(
				parse(
					text=paste0('(as.numeric(`', thisQtr, '`)/as.numeric(`', prevQtr, '`))-1')
				)
			)
		)
	])

	thisThi <- unique(pceDTq[
		Vin == paste0(thisQtr, 'thi'), 
		.(
			Code, 
			ThiLvl = eval(
				parse(
					text=paste0('as.numeric(`', thisQtr, '`)')
				)
			), 
			ThiPct = eval(
				parse(
					text=paste0('(as.numeric(`', thisQtr, '`)/as.numeric(`', prevQtr, '`))-1')
				)
			)
		)
	])

	data.table::setkey(thisAdv, key = Code)
	data.table::setkey(thisSec, key = Code)
	data.table::setkey(thisThi, key = Code)

	thisDT <- thisAdv[thisSec[thisThi]]
	thisDT[,TimePeriod := thisQtr]
	
	return(thisDT)

}))

write.csv(outDT, file=paste0('c:/Users/', userID, '/Documents/tab245u_vin.csv'), row.names = FALSE)

#####################################################
#Junk drawer - Other stuff I was playing around with 
getQAdv <- paste0("qAdv <- pceDTq[substr(Vin, 7,9)=='adv' & 
	!is.na(Code),.(
	LineNumber, 
	LineDescription, 
	SeriesCode, 
	Vin, ",
	paste(qAdvAv, collapse = ','),
#	paste(qAvail, collapse = ','),
	 "
)]")

getQSec <- paste0("qSec <- pceDTq[substr(Vin, 7,9)=='sec' & 
	!is.na(Code),.(
	LineNumber, 
	LineDescription, 
	SeriesCode, 
	Vin, ",
	paste(qSecAv, collapse = ','),
	 "
)]")

getQThi <- paste0("qThi <- pceDTq[substr(Vin, 7,9)=='thi' & 
	!is.na(Code),.(
	LineNumber, 
	LineDescription, 
	SeriesCode, 
	Vin, ",
	paste(qThiAv, collapse = ','),
#	paste(qAvail, collapse = ','),
	 "
)]")

eval(parse(text=getQAdv))
eval(parse(text=getQSec))
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

#This returns meaningless zero set
rilQ[,lapply(.SD %in% dateCol, sum), by = KeyVal]

#Let's get rid of those NA vals
qClearNAs <- paste(paste0('rilQ[,', dateCol, ' := ifelse(is.na(', dateCol, '), 0, ', dateCol, ')]'), collapse = ';')
eval(parse(text=qClearNAs))

getSums <- paste0('revDT <- rilQ[, .(',paste(paste0('sum',dateCol,'=sum(',dateCol,')'), collapse = ','),'), by=Code]')
eval(parse(text=getSums))
write.csv(revDT, file=paste0('c:/Users/', userID, '/Documents/revDT.csv'))


