library(RevoScaleR) ## This is automatcally started when using R Client

## Path to folder containing the sample files
## The xdf files will be created here as well
file.folder <- 'C:/Data/NYC_taxi_samples'

## Read in first 1000 rows of one file
input.csv <- file.path(file.folder, 'yellow_tripsample_2016-01.csv')

## Set the variable class of the columns
col.classes <- c('VendorID' = "factor",
                 'tpep_pickup_datetime' = "character",
                 'tpep_dropoff_datetime' = "character",
                 'passenger_count' = "integer",
                 'trip_distance' = "numeric",
                 'pickup_longitude' = "numeric",
                 'pickup_latitude' = "numeric",
                 'RateCodeID' = "factor",
                 'store_and_fwd_flag' = "factor",
                 'dropoff_longitude' = "numeric",
                 'dropoff_latitude' = "numeric",
                 'payment_type' = "factor",
                 'fare_amount' = "numeric",
                 'extra' = "numeric",
                 'mta_tax' = "numeric",
                 'tip_amount' = "numeric",
                 'tolls_amount' = "numeric",
                 'improvement_surcharge' = "numeric",
                 'total_amount' = "numeric",
                 'u' = "numeric")


#############################################################################
## Read in a chunk of the file in a df for testing
#############################################################################

nyc.sample.df <- read.csv(input.csv, nrows = 1000, colClasses = col.classes)
## Take a quick look
head(nyc.sample.df, 5)


#############################################################################
## Reading in the whole data
#############################################################################

input.xdf <- file.path(file.folder, 'yellow_tripdata_2016.xdf') ## File does not exist yet
library(lubridate)

most.recent.day <- ymd('2016-07-01') ## Data exists only until 2016-06

st <- Sys.time()
for (i in 1:6) {
    file.date <- most.recent.day - months(i) ## Looping back in months
    input.csv <- file.path(file.folder, sprintf('yellow_tripsample_%s.csv', substr(file.date, 1, 7))) ## Add YEAR-MONTH (2016-06)
    append <- ifelse(i == 1, 'none', 'rows')

    ## Load data into the xdf
    message('Importing CSV file to XDF')
    rxImport(input.csv, input.xdf, colClasses = col.classes, 
        overwrite = TRUE, append = append)
}
Sys.time() - st ## Time to import files to xdf


#############################################################################
## XDF vs CSV
#############################################################################

## Process the XDF file
nyc.xdf <- RxXdfData(input.xdf) ## Pointer to the file

system.time(
    rxsum.xdf <- rxSummary( ~ fare_amount, data = nyc.xdf) ## Calc. summary for only fare_amount
)

## Take a look a the data
rxsum.xdf

## Process the CSV file
input.csv <- file.path(file.folder, 'yellow_tripsample_2016-01.csv') ## Looking only at one month
nyc.csv <- RxTextData(input.csv, colClasses = col.classes)

system.time(
    rxsum.csv <- rxSummary( ~ fare_amount, data = nyc.csv)
)

## takes much longer time than XDF
rxsum.csv


#############################################################################
# Checking column types
#############################################################################


## Command under shows info about a XDF or df
rxGetInfo(nyc.xdf, numRows = 5, getVarInfo = TRUE)
rxGetInfo(nyc.xdf, getVarInfo = TRUE) ## Similar to the str() function


#############################################################################
# Simple transformation
#############################################################################

## Write to the xdf file
rxDataStep(inData = nyc.xdf, outFile = nyc.xdf,
           transforms = list(tip_percent = ifelse(fare_amount > 0 & tip_amount < fare_amount,
           round(tip_amount * 100 / fare_amount, 0), NA)),
           overwrite = TRUE)

rxSummary( ~ tip_percent, data = nyc.xdf)

## Similar transformation (on-the-fly now), but now not writing to file
x <- rxSummary( ~ tip_percent2, data = nyc.xdf,
          transforms = list(tip_percent2 = ifelse(fare_amount > 0 & tip_amount < fare_amount,
          round(tip_amount * 100 / fare_amount, 0), NA)))

print(x)
attributes(x) ## Look at what is inside x
x$sDataFrame$Mean

## month:year means interaction, i.e. all possible combinations
rxCrossTabs( ~ month:year, nyc.xdf,
## Since month and year does not exist, that must be created using transform
            transforms = list(
            year = as.integer(substr(tpep_pickup_datetime, 1, 4)),
            month = as.integer(substr(tpep_pickup_datetime, 6, 7)),
            year = factor(year, levels = 2014:2016), ## Need to convert to categorical data
            month = factor(month, levels = 1:12)))


## Now, do the similar crosstab, but using lubridate package
rxCrossTabs( ~ month:year, data = nyc.xdf,
            transforms = list(
                date = ymd_hms(tpep_pickup_datetime),
                year = factor(year(date), levels = 2014:2016),
                month = factor(month(date), levels = 1:12)),
                transformPackages = "lubridate") ## This shows that some functions here are from a certain package


#############################################################################
## Complex transformations  
#############################################################################

xforms <- function(data) {
    ## Transformation function for extracting some date and time features
    require(lubridate)
    weekday.labels <- c('Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat')
    cut.levels <- c(1, 5, 9, 12, 16, 18, 22)
    hour.labels <- c('1AM-5AM', '5AM-9AM', '9AM-12PM', '12PM-4PM', '4PM-6PM',
                     '6PM-10PM', '10PM-1AM')

    ## Pickup data
    pickup.datetime <- ymd_hms(data$tpep_pickup_datetime, tz = 'UTC')
    levels(pickup.datetime) <- hour.labels

    ## Use NA as an own level
    pickup.hour <- addNA(cut(hour(pickup.datetime), breaks = cut.levels))
    ## Day of week
    pickup.dow <- factor(wday(pickup.datetime), levels = 1:7,
                         labels = weekday.labels)
    
    ## Doing similar with dropoff
    dropoff.datetime <- ymd_hms(data$tpep_dropoff_datetime, tz = 'UTC')
    levels(dropoff.datetime) <- hour.labels

    ## Use NA as an own level
    dropoff.hour <- addNA(cut(hour(dropoff.datetime), breaks = cut.levels))
    ## Day of week
    dropoff.dow <- factor(wday(dropoff.datetime), levels = 1:7,
                         labels = weekday.labels)

    ## Add these transformations to the data 
    data$pickup.hour <- pickup.datetime
    data$pickup.dow <- pickup.datetime
    data$dropoff.hour <- dropoff.hour
    data$trip.duration <- as.integer(as.duration(dropoff.datetime - pickup.datetime))

    return(data)
}

## Run the transformation. This is very important. Testing the code on a 
## smaller df, and not a large xdf file
library(lubridate)
head(xforms(nyc.sample.df)) ## If this works on the df, it will most likely work on the xdf

## Now run on the xdf
head(rxDataStep(nyc.sample.df, transformFunc = xforms, 
                transformPackages = 'lubridate')) ## Important with the package specification

st <- Sys.time()
rxDataStep(nyc.xdf, nyc.xdf, overwrite = TRUE, transformFunc = xforms,
           transformPackages = 'lubridate')
Sys.time() - st

#############################################################################
## Examining the xdf file
#############################################################################
rxs1 <- rxSummary(~ pickup.hour + pickup.dow + trip.duration, nyc.xdf)
