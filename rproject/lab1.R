## Lab 1 - DAT213x Analyzing Big Data with Microsoft R Server
##
## Name: Emir Lejlic
## Date: 19.12.16

## File and folder info
folder <- 'C:/Data/NYC_taxi_samples'
fname.raw <- 'nyc_lab1_raw.xdf'
fname <- 'nyc_lab1.xdf'
fpath.raw <- file.path(folder, fname.raw)
fpath <- file.path(folder, fname)

## Pointer to the xdf file
nyc.xdf.raw <- RxXdfData(fpath.raw)
## Create the pointer to the new file
nyc.xdf <- RxXdfData(fpath)

## Copy raw data into a new xdf file
rxDataStep(inData = nyc.xdf.raw, outFile = nyc.xdf)

## Shows variable info
rxGetVarInfo(nyc.xdf.raw)

## Show number of rows and columns
rxGetInfo(nyc.xdf.raw)
rxGetInfo(nyc.xdf, getVarInfo = TRUE) ## Similar to str()

## Summaries
rxSummary( ~ ., nyc.xdf) ## Show summary of everything
rxSummary( ~ trip_distance + passenger_count, nyc.xdf) ## Show only sum of these

## Look at the first five rows of two selected columns
rxGetInfo(nyc.xdf, numRows = 5, varsToKeep = c('fare_amount', 'RatecodeID'))


#############################################################################
## Task 1: Convert two columns to factor values
#############################################################################

## First take a look at the raw vars
rxGetVarInfo(nyc.xdf, varsToKeep = c('RatecodeID', 'payment_type'))


## Now, change RatecodeID to factor, payment_type already is
## Using the NYC data dictionary to set the levels
rxDataStep(nyc.xdf, nyc.xdf, overwrite = TRUE, 
           transforms = list(Ratecode_type_desc = factor(RatecodeID, levels = 1:6, labels = c('Standard Rate', 'JFK', 'Newark', 'Nassau or Westchester', 'Negotiated fare', 'Group ride')),
                             payment_type_desc = as.character(payment_type),
                             payment_type_desc = factor(payment_type_desc, levels = 1:2, labels = c('Credit card', 'Cash'))))

## How many "Standard Rate" codes were used
rxSummary( ~ Ratecode_type_desc, nyc.xdf) ## Show summary of everything

## How many payments in cash are there
rxSummary(~ payment_type_desc, data = nyc.xdf)
