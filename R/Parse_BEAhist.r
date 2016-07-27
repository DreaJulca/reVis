library(data.table)
library(parallel)


	userID <- Sys.info()[[7]]
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


