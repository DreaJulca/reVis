library(data.table)

	userID <- Sys.info()[[7]]
	myPath <- paste0('C:/Users/', userID, '/Documents/reVis/xls/NIPA')
	
	allFolds <- list.dirs(path = myPath)
	
	allFiles <- rbindlist(lapply(allFolds, function(thisPath){
		theseFiles <- data.table(list.files(path = thisPath, full.names = TRUE));
		return(theseFiles);
	}));
	
	allCSVs <- allFiles[tolower(substr(V1, nchar(V1)-2, nchar(V1))) == 'csv']
	
 #In this case, lines 1-6 are meta; lines 
 readDT <- fread(paste0('C:/users/',userID, '/documents/reVis/xls/NIPA/2016/q1/advance_april-28-2016/section7all_xlssht39.csv'))
 cleanDT <- readDT[!is.na(`NA..3`)]
 nameDT <- names(readDT)[2]
 
