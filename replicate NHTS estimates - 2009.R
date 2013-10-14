#####################################################
# this script matches the nationwide statistics at  #
# http://nhts.ornl.gov/2009/pub/stt.pdf  					  #
#####################################################

require(survey) 					# load survey package (analyzes complex design surveys)

# ---------------------------------------------------
# connect to database
# ---------------------------------------------------

# connect to the database containing the NHTS records

# monetDB? sqlite? choose your own adventure.

# prepare necessary data

# define replicate weight field names
hhrep.names <- paste0("hhwgt", 1:100)
perrep.names <- paste0("wtperfin", 1:100)

# # # # run your analysis commands # # # #

# prepare household file data

# extract only variables necessary to replicate nationwide statistics from the merged hh file
# this circumvents a current annoying bug with sqlsurvey that doesn't allow conditioning variables
# to be specified in calls to the svy functions where the objects are backed by monetDB databases
z.data <- dbGetQuery(db, paste0("SELECT hhsize, hhvehcnt, drvrcnt, wrkcount, wthhfin FROM hh_merged_2009"))
z.repwts <- dbGetQuery(db, paste0("SELECT ", paste0(hhrep.names, collapse = ", "), " FROM hh_merged_2009"))

# this line is needed to properly replicate the official confidence intervals
options(survey.replicates.mse = TRUE)

y <- 
	svrepdesign(
		weights = ~wthhfin,
		repweights = z.repwts,
		type = "Fay",
		rho = (1-1/sqrt(99)),
		data = z.data)

# household size categories
svytotal(~I(hhsize == 1), y)
svytotal(~I(hhsize == 2), y)
svytotal(~I(hhsize == 3), y)
svytotal(~I(hhsize > 3), y)

confint(svytotal(~I(hhsize == 1), y), df = degf(y)+1)
confint(svytotal(~I(hhsize == 2), y), df = degf(y)+1)
confint(svytotal(~I(hhsize == 3), y), df = degf(y)+1)
confint(svytotal(~I(hhsize > 3), y), df = degf(y)+1)

# total HH vehicles
svytotal(~I(hhvehcnt), y)
confint(svytotal(~I(hhvehcnt), y), df = degf(y)+1)

# this will blow your ram if left in, remove the large variables before moving on
rm(z.data)
rm(z.repwts)
rm(y)
gc()

# prepare person file data

z.data <- dbGetQuery(db, "SELECT r_age, r_sex, driver, worker, wtperfin FROM per_merged_2009")
z.repwts <- dbGetQuery(db, paste0("SELECT ", paste0(perrep.names, collapse = ", "), " FROM per_merged_2009"))

y <- 
	svrepdesign(
		weights = ~wtperfin,
		repweights = z.repwts,
		type = "Fay",
		rho = (1-1/sqrt(99)),
		data = z.data)

# All male = 1, all female = 2
svytotal(~I(r_sex == 1), y)
confint(svytotal(~I(r_sex == 1), y), df = degf(y)+1)

svytotal(~I(r_age < 16), y)
confint(svytotal(~I(r_age < 16), y), df = degf(y))

# Drivers
svytotal(~I(driver == 1), y, na.rm = TRUE)
confint(svytotal(~I(driver == 1), y, na.rm = TRUE), df = degf(y)+1)

# Male drivers
svytotal(~I(driver == 1 & r_sex == 1), y, na.rm = TRUE)
confint(svytotal(~I(driver == 1 & r_sex == 1), y, na.rm = TRUE), df = degf(y)+1)

# Female drivers
svytotal(~I(driver == 1 & r_sex == 2), y, na.rm = TRUE)
confint(svytotal(~I(driver == 1 & r_sex == 2), y, na.rm = TRUE), df = degf(y)+1)

# Workers
svytotal(~I(worker == 1), y, na.rm = TRUE)
confint(svytotal(~I(worker == 1), y, na.rm = TRUE), df = degf(y)+1)

# clean up
rm(z.data)
rm(z.repwts)
rm(y)
gc()

# disconnect from database
dbDisconnect(db)
monetdb.server.stop(pid)

# the trip file was analyzed using a sqlite database-backed survey object
# and the following commands, assuming the object 'y' is attached defined with reference
# to the trip table, as defined here:
# https://github.com/aakarner/nhts/blob/master/download%202009%20NHTS%20data%20(sqlite).R

# Person trips - calculated using a column of ones added to the trip file
# svytotal(~one, y)
# confint(svytotal(~I(one), y), df = degf(y)+1)

# Person-miles of travel
# svytotal(~trpmiles, y, na.rm = TRUE)
# confint(svytotal(~trpmiles, y, na.rm = TRUE), df = degf(y)+1)
