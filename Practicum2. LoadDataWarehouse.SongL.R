
# title: Practicum II Load Data Warehouse
# author: Liyang Song
# course: CS5200
# date: Spring 2023


# Remove existing objects to facilitate repeated tests
rm(list = ls())

# Record start time to test running time of the script
bt <- Sys.time()

# Initialize MySQL connection
require(RMySQL)
require(dplyr)

db_user <- 'root' 
db_password <- 'liyang5200'
db_name <- 'pubmed'
db_host <- 'localHost'
db_port <- 3306

mysqlConn <-  dbConnect(RMySQL::MySQL(), 
                   user = db_user, 
                   password = db_password,
                   dbname = db_name, 
                   host = db_host, 
                   port = db_port)

# Initialize tables in database
# The star schema is drawn in [ERD](https://lucid.app/lucidchart/84413526-9174-417b-ab29-31b955bc979c/edit?viewport_loc=80%2C215%2C1277%2C564%2CZDzPD5Y~_fZm&invitationId=inv_69cbf6db-e27b-4ffc-ae26-26751f50fa8a)
# As the analytical query examples involves year, quarter dimensions and numbers of authors and articles
# `JournalFacts` is created as the fact table including FKs to dimension tables and `articleNum` and `authorNum`
# `JournalDims` and `DateDims` are created as two dimension table

dbExecute(mysqlConn, "DROP TABLE IF EXISTS JournalFacts")
dbExecute(mysqlConn, "DROP TABLE IF EXISTS JournalDims")
dbExecute(mysqlConn, "DROP TABLE IF EXISTS DateDims")

sql <- "CREATE TABLE IF NOT EXISTS JournalDims (
            journalDim_id INTEGER,
            issn TEXT,
            title TEXT,
            isoAbbreviation TEXT,
            PRIMARY KEY (journalDim_id))"
dbExecute(mysqlConn, sql)

sql <- "CREATE TABLE IF NOT EXISTS DateDims (
            dateDim_id INTEGER,
            month INTEGER,
            quarter INTEGER,
            year INTEGER,
            PRIMARY KEY (dateDim_id))"
dbExecute(mysqlConn, sql)

sql <- "CREATE TABLE IF NOT EXISTS JournalFacts (
            journalFact_id INTEGER,
            journalDim_id INTEGER,
            dateDim_id INTEGER,
            articleNum INTEGER,
            authorNum INTEGER,
            PRIMARY KEY (journalFact_id),
            FOREIGN KEY (journalDim_id) REFERENCES JournalDims(journalDim_id),
            FOREIGN KEY (dateDim_id) REFERENCES dateDims(dateDim_id))"
dbExecute(mysqlConn, sql)

# Initialize data frames corresponding to each table
df.journaldims <- data.frame(journalDim_id <- integer(),
                              issn <- character(),
                              title <- character(),
                              isoAbbreviation <- character(),
                              stringsAsFactors = F)

df.datedims <- data.frame(dateDim_id <- integer(),
                          month <- integer(),
                          quarter <- integer(),
                          year <- integer(),
                          stringsAsFactors = F)

df.journalfacts <- data.frame(journalFact_id <- integer(),
                              journalDim_id <- integer(),
                              dateDim_id <- integer(),
                              articleNum <- integer(),
                              authorNum <- integer(),
                              stringsAsFactors = F)

# Initialize RSQLite connection to load data created in Part 1
require(RSQLite)
dbFile <- "pubmed.db"
sqliteConn <- dbConnect(RSQLite::SQLite(), dbFile)

# Extract data and insert into corresponding data frames
# Considering the scale of data, extract only required data by a SQL into `df.total` rather than importing the whole database
# Then extract data from `df.total` to each data frames
sql <- "SELECT j.journal_id AS journalDim_id, 
               j.issn,
               j.title, 
               j.isoAbbreviation, 
               CAST(SUBSTRING(i.pubDate, 1, 4) AS INT) AS year,
               CAST(SUBSTRING(i.pubDate, 6, 7) AS INT) AS month,
               COUNT(DISTINCT a.pmid) AS articleNum, 
               COUNT(DISTINCT aa.author_id) AS authorNum
        FROM Journals j
        JOIN Issues i USING (journal_id)
        JOIN Articles a USING (issue_id)
        JOIN ArticleAuthor aa USING (pmid)
        GROUP BY journalDim_id, year, month"
df.total <- dbGetQuery(sqliteConn, sql)

# Data for table `JournalDims`
# Each record would be a distinct journal
df.journaldims <- rbind(df.journaldims, distinct(df.total, journalDim_id, issn, title, isoAbbreviation))

# Data for table `DateDims`
# Each record would be a distinct year and month (considering no requirement for day-level analysis)
# Column `quarter` would be calculated from column `month`
df.yearmonth <- distinct(df.total, year, month)
df.datedims <- rbind(df.datedims, data.frame(dateDim_id = seq(1, nrow(df.yearmonth)),
                                             month = df.yearmonth$month,
                                             quarter = as.integer(ceiling(df.yearmonth$month/3)),
                                             year = df.yearmonth$year,
                                             stringsAsFactors = F))

# Data for table `JournalFacts`
# Loop through `df.total` to find FKs to the two dimension tables
for (i in 1:nrow(df.total)) {
    journalDim_id <- df.total$journalDim_id[i]
    dateDim_id <- df.datedims$dateDim_id[which(df.datedims$month == df.total$month[i] &
                                                    df.datedims$year == df.total$year[i])]
    df.journalfacts <- rbind(df.journalfacts, data.frame(journalFact_id = i,
                                                         journalDim_id = journalDim_id,
                                                         dateDim_id = dateDim_id,
                                                         articleNum = df.total$articleNum[i],
                                                         authorNum = df.total$authorNum[i],
                                                         stringsAsFactors = F))
    }

# Write data frames to database tables
dbSendQuery(mysqlConn, "SET GLOBAL local_infile = true")
dbWriteTable(mysqlConn, "journaldims", df.journaldims, overwrite = F, append = T, row.names = F)
dbWriteTable(mysqlConn, "datedims", df.datedims, overwrite = F, append = T, row.names = F)
dbWriteTable(mysqlConn, "journalfacts", df.journalfacts, overwrite = F, append = T, row.names = F)

# Record total running time
et <- Sys.time()
cat("Time elapsed: ", round((et - bt), 2), " seconds.")

# Disconnect to database
dbDisconnect(mysqlConn)
dbDisconnect(sqliteConn)
rm(list = ls())
