library(data.table)

	userID <- Sys.info()[[7]]
	myPath <- paste0('C:/Users/', userID, '/Documents/reVis/xls/NIPA')
	
	allFolds <- list.dirs(path = myPath)
	
	#allFiles <- 


 #In this case, lines 1-6 are meta; lines 
 readDT <- fread('C:/users/niacj1/documents/reVis/xls/NIPA/2016/q1/advance_april-28-2016/section7all_xlssht39.csv')
 cleanDT <- readDT[!is.na(`NA..3`)]
 nameDT <- names(readDT)[2]