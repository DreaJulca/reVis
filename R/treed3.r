library(beaR)
library(data.table)
library(data.tree)
library(treemap)
library(jsonlite)

load(paste0('C:/Users/', Sys.info()[[7]], '/documents/nationalIndex.rdata'))

natIndRooter <- as.data.table(as.data.frame(nationalIndex))
#data.table::setkey(nationalIndex, key = datasetName, TableID, LineNumber)
natIndRooter [, tabSplitter := paste(datasetName, TableID)]
#natIndRooter [, nodeID := paste0(datasetName, '_table', TableID, '_line', LineNumber)]
#natIndRooter [, rootID := paste0(datasetName, '_table', TableID, '_line', rootTabLine)]
data.table::setkey(natIndRooter, key = tabSplitter)

natIndRooter[, tier0 := min(tier), by = tabSplitter]
natIndRooter[tier > tier0, tier1 := min(tier), by = tabSplitter]
natIndRooter[tier > tier1, tier2 := min(tier), by = tabSplitter]
natIndRooter[tier > tier2, tier3 := min(tier), by = tabSplitter]
natIndRooter[tier > tier3, tier4 := min(tier), by = tabSplitter]
natIndRooter[tier > tier4, tier5 := min(tier), by = tabSplitter]
natIndRooter[tier > tier5, tier6 := min(tier), by = tabSplitter]
natIndRooter[tier > tier6, tier7 := min(tier), by = tabSplitter]
natIndRooter[tier > tier7, tier8 := min(tier), by = tabSplitter]
natIndRooter[tier > tier8, tier9 := min(tier), by = tabSplitter]
natIndRooter[tier > tier9, tier10 := min(tier), by = tabSplitter]
natIndRooter[tier > tier10, tier11 := min(tier), by = tabSplitter]
natIndRooter[tier > tier11, tier12 := min(tier), by = tabSplitter]
natIndRooter[tier > tier12, tier13 := min(tier), by = tabSplitter]
natIndRooter[tier > tier13, tier14 := min(tier), by = tabSplitter]
natIndRooter[tier > tier14, tier15 := min(tier), by = tabSplitter]

natIndTier0 <- 	natIndRooter[tier == tier0]
natIndTier1 <- 	natIndRooter[tier == tier1]
natIndTier2 <- 	natIndRooter[tier == tier2]
natIndTier3 <- 	natIndRooter[tier == tier3]
natIndTier4 <- 	natIndRooter[tier == tier4]
natIndTier5 <- 	natIndRooter[tier == tier5]
natIndTier6 <- 	natIndRooter[tier == tier6]
natIndTier7 <- 	natIndRooter[tier == tier7]
natIndTier8 <- 	natIndRooter[tier == tier8]
natIndTier9 <- 	natIndRooter[tier == tier9]
natIndTier10 <-	natIndRooter[tier == tier10]


#This was the last time we used natIndRooter in the original code; in fact, we can 
#just export the node relationships to CSV and use d3 v4.0's "Stratify" method!

natIndRooter[, name := paste0(datasetName, '_Tab', TableID, '_L', LineNumber)]

natIndRooter[!is.na(rootTabLine),
	parent := paste0(datasetName, '_Tab', TableID, '_L', rootTabLine)
]

natIndRooter[is.na(rootTabLine),
	parent := datasetName
]

toRoot <-  data.table(
	name=c(
		"NEA",
		unique(natIndRooter[, datasetName])
	), 
	parent=c(
		"",
		rep(
			"NEA", 
			length(
				unique(
					natIndRooter[, datasetName]
				)
			)
		)
	)
)

csvHier <- rbindlist(list(toRoot, natIndRooter[, .(name, parent)]))

write.table(csvHier, file="C:/users/niacj1/documents/ESA_BEA proj/nationalStruc.csv", sep=",", row.names=F)


#However, if this doesn't work out, here's the rest of the hierarchy stuff


thisKids <- natIndTier1[, .(tabSplitter, thisLine = LineNumber, LineNumber = rootTabLine)]
setkey(natIndTier0, key = tabSplitter, LineNumber) 
setkey(thisKids, key = tabSplitter, LineNumber)

#natIndMap1 <- natIndTier0[thisKids][, .(tabSplitter, root = LineNumber, kids = thisLine)]
natIndMap1 <- merge(natIndTier0, thisKids, all.x = TRUE)[, .(tabSplitter, root = LineNumber, kids = thisLine)]

thisKids <- natIndTier2[, .(tabSplitter, thisLine = LineNumber, kids = rootTabLine)]
setkey(natIndMap1, key = tabSplitter, kids) 
setkey(thisKids, key = tabSplitter, kids)

natIndMap2 <- 
	merge(
		natIndMap1, 
		thisKids, 
		all.x = TRUE
	)[, 
		.(
			tabSplitter, 
			root, 
			kids,
			gkids = thisLine
		)
	]

thisKids <- natIndTier3[, .(tabSplitter, thisLine = LineNumber, gkids = rootTabLine)]
setkey(natIndMap2, key = tabSplitter, gkids) 
setkey(thisKids, key = tabSplitter, gkids)

natIndMap3 <- 
	merge(
		natIndMap2, 
		thisKids, 
		all.x = TRUE
	)[, .(tabSplitter, root, kids, gkids, ggkids = thisLine)]

	
thisKids <- natIndTier4[, .(tabSplitter, thisLine = LineNumber, ggkids = rootTabLine)]
setkey(natIndMap3, key = tabSplitter, ggkids) 
setkey(thisKids, key = tabSplitter, ggkids)

natIndMap4 <-  
	merge(
		natIndMap3, 
		thisKids, 
		all.x = TRUE
	)[, .(tabSplitter, root, kids, gkids, ggkids, g3kids = thisLine)]

	
thisKids <- natIndTier5[, .(tabSplitter, thisLine = LineNumber, g3kids = rootTabLine)]
setkey(natIndMap4, key = tabSplitter, g3kids) 
setkey(thisKids, key = tabSplitter, g3kids)

natIndMap5 <-  
	merge(
		natIndMap4, 
		thisKids, 
		all.x = TRUE
	)[, .(tabSplitter, root, kids, gkids, ggkids, g3kids, g4kids = thisLine)]



thisKids <- natIndTier6[, .(tabSplitter, thisLine = LineNumber, g4kids = rootTabLine)]
setkey(natIndMap5, key = tabSplitter, g4kids) 
setkey(thisKids, key = tabSplitter, g4kids)

natIndMap6 <-  
	merge(
		natIndMap5, 
		thisKids, 
		all.x = TRUE
	)[, .(tabSplitter, root, kids, gkids, ggkids, g3kids, g4kids, g5kids = thisLine)]


thisKids <- natIndTier7[, .(tabSplitter, thisLine = LineNumber, g5kids = rootTabLine)]
setkey(natIndMap6, key = tabSplitter, g5kids) 
setkey(thisKids, key = tabSplitter, g5kids)

natIndMap7 <-  
	merge(
		natIndMap6, 
		thisKids, 
		all.x = TRUE
	)[, .(tabSplitter, root, kids, gkids, ggkids, g3kids, g4kids, g5kids, g6kids = thisLine)]


thisKids <- natIndTier8[, .(tabSplitter, thisLine = LineNumber, g6kids = rootTabLine)]
setkey(natIndMap7, key = tabSplitter, g6kids) 
setkey(thisKids, key = tabSplitter, g6kids)

natIndMap8 <-  
	merge(
		natIndMap7, 
		thisKids, 
		all.x = TRUE
	)[, .(tabSplitter, root, kids, gkids, ggkids, g3kids, g4kids, g5kids, g6kids, g7kids = thisLine)]

#natIndMap8[,  dataset := strsplit(tabSplitter, ' ', fixed = TRUE)[[1]]]
natIndMap8[, dataset := gsub('[[:blank:]][[:digit:]]+', '', tabSplitter)]
natIndMap8[, TableID := gsub('NIPA ', '', tabSplitter, fixed = T)]
natIndMap8[, TableID := gsub('NIUnderlyingDetail ', '', TableID, fixed = T)]
natIndMap8[, TableID := gsub('FixedAssets ', '', TableID, fixed = T)]
natIndMap8[, tabSplitter := NULL]

natIndMap8[!is.na(root),
	rootJS := paste0(dataset, '_Tab', TableID, '_L', root)
] 
natIndMap8[!is.na(kids),
	kidsJS := paste0(dataset, '_Tab', TableID, '_L', kids)
] 
natIndMap8[!is.na(gkids),
	gkidsJS := paste0(dataset, '_Tab', TableID, '_L', gkids)
] 
natIndMap8[!is.na(ggkids),
	ggkidsJS := paste0(dataset, '_Tab', TableID, '_L', ggkids)
] 
natIndMap8[!is.na(g3kids),
	g3kidsJS := paste0(dataset, '_Tab', TableID, '_L', g3kids)
] 
natIndMap8[!is.na(g4kids),
	g4kidsJS := paste0(dataset, '_Tab', TableID, '_L', g4kids)
] 
natIndMap8[!is.na(g5kids),
	g5kidsJS := paste0(dataset, '_Tab', TableID, '_L', g5kids)
] 
natIndMap8[!is.na(g6kids),
	g6kidsJS := paste0(dataset, '_Tab', TableID, '_L', g6kids)
] 
natIndMap8[!is.na(g7kids),
	g7kidsJS := paste0(dataset, '_Tab', TableID, '_L', g7kids)
] 


setkey(natIndMap8, key = dataset, TableID, root, kids, gkids, ggkids, g3kids, g4kids, g5kids, g6kids, g7kids )



#Convert to DF
natDF <- as.data.frame(
	natIndMap8[, .(
		dataset, 
		TableID, 
		rootJS, 
		kidsJS, 
		gkidsJS, 
		ggkidsJS, 
		g3kidsJS, 
		g4kidsJS, 
		g5kidsJS, 
		g6kidsJS, 
		g7kidsJS
	)]
)

#Set pathString for conversion to data.tree
natDF$pathString <- paste(
	'NEA', 
	natDF$datasetJS, 
	natDF$TableIDJS, 
	natDF$rootJS, 
	natDF$kidsJS, 
	natDF$gkidsJS, 
	natDF$ggkidsJS, 
	natDF$g3kidsJS, 
	natDF$g4kidsJS, 
	natDF$g5kidsJS, 
	natDF$g6kidsJS, 
	natDF$g7kidsJS, 
	sep = '/')

#Create tree
#NOTE: TAKES LONG AF
nationalTree <- as.Node(natDF)

#Clone a pruned tree
#NOTE: THIS ALSO TAKES LONG AF
ntClone <- Clone(
	nationalTree, 
	pruneFun = function(x) {
		x$Get('name')[[1]] != 'NA'
	}, 
	attributes = FALSE
)
#print(nationalTree)
#nationalTree$FixedAssets$`114`$`1`$`13`$`14`$`NA`$Get('name')[1]
#nationalTree$FixedAssets$`114`$`1`$`13`$`14`$`NA`$path
#ntPaths <- ntClone
#Convert to nested list

#Not sure which of these two approaches is better; leaning toward #2
#Approach 1:
nationalList <- as.list(ntClone)#, nodeName = paste(x$path, collapse = ''))
#Convert, finally, to JSON
nationalJSON <- jsonlite::toJSON(nationalList, pretty = TRUE)

#Approach 2:
neaList <- ToListExplicit(ntClone, unname = TRUE, nameName = 'name', childrenName = 'children')
#Convert, finally, to JSON
neaminJSON <- jsonlite::toJSON(neaList)#, pretty = TRUE)
nationalJSON <- jsonlite::toJSON(neaList, pretty = TRUE)

sysuser <- Sys.info()[7]
jsFile <- paste0('C:/Users/', sysuser, '/Documents/national.json')
sink(jsFile)
nationalJSON
sink()
minjsFile <- paste0('C:/Users/', sysuser, '/Documents/nea.min.json')
sink(minjsFile)
neaminJSON
sink()
#
#neaList <- ToListExplicit(nationalTree)



#nationalTDF <- ToDataFrameTree(nationalTree, pruneFun = function(x){!is.na(x)})

#nationalJSON <- jsonlite::toJSON(nationalList)
#nationalJSON <- jsonlite::toJSON(natDF)
