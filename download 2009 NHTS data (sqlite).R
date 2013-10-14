setwd("E:/data_NHTS")

# name the database (.db) file to be saved in the working directory
nhts.dbname <- "nhts09.db"

# unhappy with all the scientific notation in your output?
# uncomment this line to increase the scientific notation threshold
options( scipen = 15 )


############################################
# no need to edit anything below this line #

# # # # # # # # #
# program start #
# # # # # # # # #

# if the NHTS database file already exists in the current working directory, print a warning
if ( file.exists( paste( getwd() , nhts.dbname , sep = "/" ) ) ) warning( "the database file already exists in your working directory.\nyou might encounter an error if you are running the same year as before or did not allow the program to complete.\ntry changing the sbo.dbname in the settings above." )


require(RSQLite) 			# load RSQLite package (creates database files in R)
require(RSQLite.extfuns) 	# load RSQLite package (allows mathematical functions, like SQRT)
require(survey) 			# load survey package (analyzes complex design surveys)

# connect to an rsqlite database on the local disk
db <- dbConnect( SQLite() , nhts.dbname )

# load the mathematical functions in the r package RSQLite.extfuns
init_extensions(db)

# path declarations
trip.file <- "E:/data_NHTS/2009/csv/DAYV2PUB.CSV"
repwts.file <- "E:/data_NHTS/2009/Replicates/ReplicatesASCII/per50wt.csv"

# read the trip file directly into the rsqlite database you just created.
dbWriteTable( db , 'trips_' , trip.file , sep = "," , header = TRUE )

# read the replicate weight file directly into the rsqlite database you created.
dbWriteTable( db , 'wts_' , repwts.file , sep = "," , header = TRUE )

# re-write the same tables, but with lowercase column names	
dbSendQuery( 
	db , 
	paste(
		'CREATE TABLE trips AS SELECT' ,
		paste( 
			dbListFields( db , 'trips_' ) , 
			tolower( dbListFields( db , 'trips_' ) ) , 
			collapse = ', ' , 
			sep = ' as '
		) ,
		"FROM trips_"
	)
)

# and since the data table `trips_` has a bunch of messy capital-letter column names
dbRemoveTable( db , 'trips_' )
# delete it from the rsqlite database

# add a new numeric column called `one` to the `y` data table
dbSendQuery( db , 'ALTER TABLE trips ADD COLUMN one DOUBLE PRECISION' )
# and fill it with all 1s for every single record.
dbSendQuery( db , 'UPDATE trips SET one = 1' )

dbSendQuery( 
	db , 
	paste(
		'CREATE TABLE wts AS SELECT' ,
		paste( 
			dbListFields( db , 'wts_' ) , 
			tolower( dbListFields( db , 'wts_' ) ) , 
			collapse = ', ' , 
			sep = ' as '
		) ,
		"FROM wts_"
	)
)

# and since the data table `wts_` has a bunch of messy capital-letter column names
dbRemoveTable( db , 'wts_' )
# delete it from the rsqlite database

# Define replicate weight field names
hhrep.names <- paste0("hhwgt", 1:100)
vehrep.names <- hhrep.names
perrep.names <- paste0("wtperfin", 1:100)
dayrep.names <- paste0("daywgt", 1:100)

# Create a unique personid on which to join relevant data and replicate weights
dbSendQuery(db, "ALTER TABLE trips ADD COLUMN perid INT")
dbSendQuery(db, "UPDATE trips SET perid = houseid * 10 + personid")
dbSendQuery(db, "ALTER TABLE wts ADD COLUMN perid INT")
dbSendQuery(db, "UPDATE wts SET perid = houseid * 10 + personid")

# Read field names from the four data tables
#hh.names <- names(dbGetQuery(db, "select * from hh_2009 limit 1"))
#per.names <- names(dbGetQuery(db, "select * from per_2009 limit 1"))
#veh.names <- names(dbGetQuery(db, "select * from veh_2009 limit 1"))
trip.names <- names(dbGetQuery(db, "SELECT * FROM trips LIMIT 1"))

# Recode missing values in the data tables
# for(i in hh.names) try(dbSendUpdate(db, paste0("update hh_2009 set ", i, "= NULL where ", i, " < 0")), silent = TRUE)
# for(i in per.names) try(dbSendUpdate(db, paste0("update per_2009 set ", i, "= NULL where ", i, " < 0")), silent = TRUE)
# for(i in veh.names) try(dbSendUpdate(db, paste0("update veh_2009 set ", i, "= NULL where ", i, " < 0")), silent = TRUE)
for(i in trip.names) try(dbSendQuery(db, paste0("UPDATE trips SET ", i, "= NULL WHERE ", i, " < 0")))

# Join each data table and its appropriate replicate weights

# Add indices to speed up the join
dbSendQuery( db , "CREATE INDEX idx1 ON trips ( perid )" )
dbSendQuery( db , "CREATE INDEX idx2 ON wts ( perid )" )

# Trips
dbSendQuery(db, paste0("CREATE TABLE trips_m AS SELECT ", 
	paste(paste0("t1.", trip.names), collapse = ", "),
	", ", 
	paste(paste0("t2.", dayrep.names), collapse = ", "),
	" FROM trips as t1 INNER JOIN wts AS t2 ON t1.perid = t2.perid"))

# Create the complex survey design object

# This line is needed to (mostly) match the posted results
options(survey.replicates.mse = TRUE)

y <- 
	svrepdesign(
		weights = ~wttrdfin, 
		repweights = "daywgt[1-9]", 
		type = "Fay",
		rho = (1-1/sqrt(99)),
		data = "trips_m",
		combined.weights = T ,
		dbtype = "SQLite",
		dbname = "nhts09.db"
	)

save(y, file = "NHTS_2009.rda")

# Person trips
svytotal(~one, y)
confint(svytotal(~I(one), y), df = degf(y)+1)

# Person-miles of travel
svytotal(~trpmiles, y, na.rm = TRUE)
confint(svytotal(~trpmiles, y, na.rm = TRUE), df = degf(y)+1)
confint(svytotal(~trpmiles, y, na.rm = TRUE), df = degf(y)-1)

# Number of trips taken by public transit - this is not correct yet because the question is whether transit was used at all on the travel day
# From: http://nhts.ornl.gov/2009/pub/UsersGuideV2.pdf#page=78
svytotal(~I(usepubtr == 1), y, na.rm = TRUE)
confint(svytotal(~I(usepubtr == 1), y, na.rm = TRUE), df = degf(y))


# disconnect from the current database
dbDisconnect( db )
