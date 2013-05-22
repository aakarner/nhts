# ---------------------------------------------------
# This script automates the importation of National Household Transportation Survey Data for 2009. The appropriate 
# data are obtained from the NHTS site (http://nhts.ornl.gov/download.shtml) and loaded into a MonetDB database.
# A complex survey object is then created for each data table to facilitate speedy analysis. 
#
# Much code borrowed liberally from Anthony Damico's US government survey data project:
# https://github.com/ajdamico/usgsd
# ---------------------------------------------------

# Warning: monetdb required
# Prior to running this script, MonetDB must be installed on the local machine. Follow each step outlined on this page: 
# https://github.com/ajdamico/usgsd/blob/master/MonetDB/monetdb%20installation%20instructions.R                                   #
# Credit to Anthony Damico

require(MonetDB.R)
require(sqlsurvey)
	
# Set your working directory as required 
# setwd("E:/data_NHTS")

# ---------------------------------------------------
# Create the database (these commands only need to be executed once)
# ---------------------------------------------------

#ONLY RUN ONCE: create a monetdb executable (.bat) file for the NHTS data
# batfile <-
# 	monetdb.server.setup(
# 		
# 		# set the path to the directory where the initialization batch file and all data will be stored
# 		database.directory = paste0(getwd() , "/MonetDB"),
# 		# must be empty or not exist
# 		
# 		# find the main path to the monetdb installation program
# 		monetdb.program.path = "C:/Program Files/MonetDB/MonetDB5" ,
# 		
# 		# choose a database name
# 		dbname = "NHTS" ,
# 		
# 		# choose a database port
# 		# this port should not conflict with other monetdb databases
# 		# on your local computer.  two databases with the same port number
# 		# cannot be accessed at the same time
# 		dbport = 58008
# 	)
# 
# ---------------------------------------------------
# connect to database
# ---------------------------------------------------

# The batch file used to start the MonetDB server is here
batfile

# In the future set it to your working directory
#batfile <- "E:/data_NHTS/MonetDB/NHTS.bat"

pid <- monetdb.server.start(batfile)
dbname <- "NHTS" 
dbport <- 58008
monet.url <- paste0("monetdb://localhost:" , dbport , "/" , dbname)
db <- dbConnect(MonetDB.R() , monet.url , "monetdb" , "monetdb")

# ---------------------------------------------------
# Disconnect from database and stop the server (once analysis is complete)
# ---------------------------------------------------

# dbDisconnect(db)
# monetdb.server.stop(pid)

# ---------------------------------------------------
# Variable defintion
# ---------------------------------------------------

NHTS.file.types <- c("DAY", "HH", "PER", "VEH")

# ---------------------------------------------------
# Download and import person, household, vehicle and trip data for 2009
# ---------------------------------------------------
tf <- tempfile(); td <- tempdir()

# TODO: Generalize the download call
NHTS.file.location <- ("http://nhts.ornl.gov/2009/download/Ascii.zip")

# store a command: "download the NHTS zipped file to the temporary file location"
download.command <- expression(download.file(NHTS.file.location, tf ,mode = "wb"))

# try the download immediately.
# run the above command, using error-handling.
download.error <- tryCatch(eval(download.command), silent = T)

# if the download results in an error..
if(class(download.error) == "try-error" ) {

	# wait 3 minutes..
	Sys.sleep(3 * 60)
	
	# ..and try the download a second time
	download.error <- tryCatch(eval(download.command), silent = T)
}

# if the download results in a second error..
if(class(download.error) == "try-error") {

	# wait 3 more minutes..
	Sys.sleep(3 * 60)
	
	# ..and try the download a third time.
	# but this time, if it fails, crash the program with a download error
	eval(download.command)
}

# once the download has completed..

# unzip the file's contents to the temporary directory
fn <- unzip(tf, exdir = td, overwrite = TRUE)

# delete all the files that do not include the text 'CSV' in their filename
file.remove(fn[!grepl("CSV", fn)])

# limit input files to csvs
fn <- fn[grepl("CSV", fn)]

# convert csv headers to lowercase so that they can be read into the MonetDB database
# approach courtest of Anthony Damico via: http://stackoverflow.com/questions/15886048/how-to-edit-or-modify-or-change-a-single-line-in-a-large-text-file-with-r

for(i in 1:length(fn)) {
	tf2 <- tempfile(pattern = paste0(NHTS.file.types[i]), fileext = ".csv")

	fr <- file(fn[i], open = "rt")  # open file connection to read
	fw <- file(tf2, open = "wt")  # open file connection to write 
	header <- readLines(fr, n = 1)  # read in header
	header <- tolower(header)  # modify header    
	writeLines(header, con = fw)  # write header to file

	while(length(body <- readLines(fr, n = 10000)) > 0) {
	  writeLines(body, fw)  # pass rest of file in chunks of 10000
	}
	
	close(fr); close(fw)  # close connections
	unlink(fn[i]) # delete the original file
	fn[i] <- tf2 # and replace it with the new version
}
	
for(i in 1:length(fn)) {
	
	# quickly figure out the number of lines in the data file
	
	chunk_size <- 1000
	testcon <- file(fn[i], open = "r")
	nooflines <- 0
	while((linesread <- length(readLines(testcon, chunk_size))) > 0)
		nooflines <- nooflines + linesread
	close(testcon)
	
	# and write it into the open database
	
	monetdb.read.csv(db, fn[i], paste0(NHTS.file.types[i], "_2009"), nrows = nooflines, locked = TRUE, na.strings = "XX")
}

# ---------------------------------------------------
# Download and import replicate weights for 2009
# ---------------------------------------------------

tf <- tempfile(); td <- tempdir()

# The NHTS replicate weights are posted in proprietary sas7bdat format. I've converted them using the sas7bdat package
# and posted them to dropbox. Future versions of the script will automate download and conversion from the NHTS site.
NHTS.wts.loc <- ("http://dl.dropboxusercontent.com/u/1725115/replicates_csv.zip")

# store a command: "download the zipped replicate weights file to the temporary file location"
download.command <- expression(download.file(NHTS.wts.loc, tf, mode = "wb"))

# try the download immediately.
# run the above command, using error-handling.
download.error <- tryCatch(eval(download.command), silent = FALSE)

# if the download results in an error..
if(class(download.error) == "try-error") {

	# wait 3 minutes..
	Sys.sleep(3 * 60)
	
	# ..and try the download a second time
	download.error <- tryCatch(eval(download.command), silent = TRUE)
}

# if the download results in a second error..
if (class(download.error) == "try-error") {

	# wait 3 more minutes..
	Sys.sleep(3 * 60)
	
	# ..and try the download a third time.
	# but this time, if it fails, crash the program with a download error
	eval(download.command)
}

# once the download has completed..

# unzip the file's contents to the temporary directory
fn <- unzip(tf, exdir = td, overwrite = TRUE)

# delete all the files that do not include the text 'CSV' in their filename
file.remove(fn[!grepl("csv", fn)])

# limit input files to csvs
fn <- fn[grepl("csv", fn)]

rep.types <- c("hh", "per")

for(i in rep.types) {
	
	# quickly figure out the number of lines in the data file
	
	chunk_size <- 1000
	testcon <- file(paste0(i, "50wt.csv"), open = "r")
	nooflines <- 0
	while((linesread <- length(readLines(testcon, chunk_size))) > 0)
		nooflines <- nooflines + linesread
	close(testcon)
	
	# import the replicate weights to the database
	monetdb.read.csv(db, paste0(i, "50wt.csv"), paste0(i, "_repwts_", 2009), nrows = nooflines, locked = TRUE, na.strings=c("NA"))
	
}

# define replicate weight field names
hhrep.names <- paste0("hhwgt", 1:100)
vehrep.names <- hhrep.names

# the person replicate weights file contains both person and day weights 
perrep.names <- paste0("wtperfin", 1:100)
dayrep.names <- paste0("daywgt", 1:100)

# create a unique personid on which to join relevant data and replicate weights
dbSendUpdate(db, "alter table per_2009 add column perid int")
dbSendUpdate(db, "update per_2009 set perid = houseid * 10 + personid")
dbSendUpdate(db, "alter table day_2009 add column perid int")
dbSendUpdate(db, "update day_2009 set perid = houseid * 10 + personid")
dbSendUpdate(db, "alter table per_repwts_2009 add column perid int")
dbSendUpdate(db, "update per_repwts_2009 set perid = houseid * 10 + personid")

# read field names from the four data tables
hh.names <- names(dbGetQuery(db, "select * from hh_2009 limit 1"))
per.names <- names(dbGetQuery(db, "select * from per_2009 limit 1"))
veh.names <- names(dbGetQuery(db, "select * from veh_2009 limit 1"))
day.names <- names(dbGetQuery(db, "select * from day_2009 limit 1"))

# recode missing values in the data tables
for(i in hh.names) try(dbSendUpdate(db, paste0("update hh_2009 set ", i, "= NULL where ", i, " < 0")), silent = TRUE)
for(i in per.names) try(dbSendUpdate(db, paste0("update per_2009 set ", i, "= NULL where ", i, " < 0")), silent = TRUE)
for(i in veh.names) try(dbSendUpdate(db, paste0("update veh_2009 set ", i, "= NULL where ", i, " < 0")), silent = TRUE)
for(i in day.names) try(dbSendUpdate(db, paste0("update day_2009 set ", i, "= NULL where ", i, " < 0")), silent = TRUE)

# join each data table and its appropriate replicate weights

# households
dbSendUpdate(db, paste0("create table hh_merged_2009 as select ", 
	paste(paste0("t1.", hh.names), collapse = ", "),
	", ", 
	paste(paste0("t2.", hhrep.names), collapse = ", "),
	" from hh_2009 as t1 inner join hh_repwts_2009 as t2 on t1.houseid = t2.houseid with data"))

# vehicles
dbSendUpdate(db, paste0("create table veh_merged_2009 as select ", 
	paste(paste0("t1.", veh.names), collapse = ", "),
	", ", 
	paste(paste0("t2.", vehrep.names), collapse = ", "),
	" from veh_2009 as t1 inner join hh_repwts_2009 as t2 on t1.houseid = t2.houseid with data"))

# persons
dbSendUpdate(db, paste0("create table per_merged_2009 as select ", 
	paste(paste0("t1.", per.names), collapse = ", "),
	", ", 
	paste(paste0("t2.", perrep.names), collapse = ", "),
	" from per_2009 as t1 inner join per_repwts_2009 as t2 on t1.perid = t2.perid with data"))

# trips
dbSendUpdate(db, paste0("create table day_merged_2009 as select ", 
	paste(paste0("t1.", day.names), collapse = ", "),
	", ", 
	paste(paste0("t2.", dayrep.names), collapse = ", "),
	" from day_2009 as t1 inner join per_repwts_2009 as t2 on t1.perid = t2.perid with data"))


# create a sqlrepsurvey complex sample design object for each data table

nhts.h.design <-
	sqlrepsurvey(
		weight = "wthhfin",
		repweights =  paste0("hhwgt", 1:100),
		scale = 1/100,
		rscales = rep(1, 100),
		mse = TRUE,
		table.name = "hh_merged_2009",
		key = "houseid",
		check.factors = 9,
		database = monet.url,
		driver = MonetDB.R() )

nhts.p.design <- 
	sqlrepsurvey(
		weight = "wtperfin",
		repweights =  perrep.names,
		scale = 1/100,
		rscales = rep(1, 100),
		mse = TRUE,
		table.name = "per_merged_2009",
		key = "perid",
		check.factors = 9,
		database = monet.url,
		driver = MonetDB.R() )

nhts.v.design <- 
	sqlrepsurvey(
		weight = "wthhfin",
		repweights = vehrep.names,
		scale = 1/100,
		rscales = rep(1, 100),
		mse = TRUE,
		table.name = "veh_merged_2009",
		key = "houseid",
		check.factors = 9,
		database = monet.url,
		driver = MonetDB.R() )

nhts.t.design <- 
	sqlrepsurvey(
		weight = "daywgt",
		repweights = dayrep.names,
		scale = 1/100,
		rscales = rep(1, 100),
		mse = TRUE,
		table.name = "day_merged_2009",
		key = "perid",
		check.factors = 9,
		database = monet.url,
		driver = MonetDB.R() )

# save the complex survey objects into their own rda file to be usef later
save( nhts.h.design, nhts.p.design, nhts.v.design, nhts.t.design, file = "NHTS_2009.rda")


# ---------------------------------------------------
# Disconnect from database and stop the server
# ---------------------------------------------------

dbDisconnect(db)
monetdb.server.stop(pid)
