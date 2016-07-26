library(data.table)
library(parallel)

	userID <- Sys.info()[[7]]
	myPath <- paste0('C:/Users/', userID, '/Documents/reVis/xls/NIPA')
	
	allFolds <- list.dirs(path = myPath)
	
	allFiles <- rbindlist(lapply(allFolds, function(thisPath){
		theseFiles <- data.table(list.files(path = thisPath, full.names = TRUE));
		return(theseFiles);
	}));
	
	allCSVs <- allFiles[tolower(substr(V1, nchar(V1)-2, nchar(V1))) == 'csv']
	
	cl <- makePSOCKcluster(2*detectCores())
	clusterEvalQ(cl, library(data.table))
	clusterExport(cl, 'allCSVs')
	
	beaHistData <- parLapply(cl, allCSVs[,V1], function(thisFile){
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
