
# title: Practicum II Load XML to DB
# author: Liyang Song
# course: CS5200
# date: Spring 2023

# Remove existing objects to facilitate repeated tests
rm(list = ls())

# Record start time to test running time of the script
bt <- Sys.time()

# Initialize RSQLite connection
# Database file will be created to store outputs
require(RSQLite)
require(XML)
require(hash)
require(httr)

dbFile <- "pubmed.db"
conn <- dbConnect(RSQLite::SQLite(), dbFile)

# Initialize tables in database
# The normalized relational schema is drawn in [ERD](https://lucid.app/lucidchart/84413526-9174-417b-ab29-31b955bc979c/edit?viewport_loc=-6%2C199%2C1914%2C845%2C0_0&invitationId=inv_69cbf6db-e27b-4ffc-ae26-26751f50fa8a)
# Apart from `Articles`, `Journals`, `Authors`, there are also `Issues` and `ArticleAuthor` being created
# Table `Issues` is for each journal issue with specific `volume`, `issue`, and `pubDate`
# Table `ArticleAuthor` is for the many-to-many relationship between `Articles` and `Authors`
dbExecute(conn, "DROP TABLE IF EXISTS Articles")
dbExecute(conn, "DROP TABLE IF EXISTS Issues")
dbExecute(conn, "DROP TABLE IF EXISTS Journals")
dbExecute(conn, "DROP TABLE IF EXISTS Authors")
dbExecute(conn, "DROP TABLE IF EXISTS ArticleAuthor")

sql <- "CREATE TABLE IF NOT EXISTS Articles (
            pmid INTEGER,
            issue_id INTEGER,
            articleTitle TEXT,
            PRIMARY KEY (pmid),
            FOREIGN KEY (issue_id) REFERENCES Issues(issue_id))"
dbExecute(conn, sql)

sql <- "CREATE TABLE IF NOT EXISTS Issues (
            issue_id INTEGER,
            journal_id INTEGER,
            volume TEXT,
            issue TEXT,
            pubDate DATE,
            PRIMARY KEY (issue_id),
            FOREIGN KEY (journal_id) REFERENCES Journals(journal_id))"
dbExecute(conn, sql)

sql <- "CREATE TABLE IF NOT EXISTS Journals (
            journal_id INTEGER,
            issn TEXT,
            title TEXT,
            isoAbbreviation TEXT,
            PRIMARY KEY (journal_id))"
dbExecute(conn, sql)

sql <- "CREATE TABLE IF NOT EXISTS Authors (
            author_id INTEGER,
            lastName TEXT,
            foreName TEXT,
            initials TEXT,
            suffix TEXT,
            affiliation TEXT,
            collectiveName TEXT,
            PRIMARY KEY (author_id))"
dbExecute(conn, sql)

sql <- "CREATE TABLE IF NOT EXISTS ArticleAuthor (
            article_author_id INTEGER,
            pmid INTEGER,
            author_id INTEGER,
            PRIMARY KEY (article_author_id),
            FOREIGN KEY (pmid) REFERENCES Articles(pmid),
            FOREIGN KEY (author_id) REFERENCES Authors(author_id))"
dbExecute(conn, sql)

# Parse XML file into DOM
xmlUrl<- "http://cs5200xmlfile.s3.us-east-2.amazonaws.com/pubmed-tfm-xml/pubmed22n0001-tf.xml"
xmlDOM <- xmlParse(content(GET(xmlUrl), as = "text", encoding = "UTF-8"), validate = T)
root <- xmlRoot(xmlDOM)
articleNum <- xmlSize(root)

# Initialize data frames corresponding to each table
df.articles <- data.frame(pmid <- integer(),
                          issue_id <- integer(),
                          articleTitle <- character(),
                          stringsAsFactors = F)

df.issues <- data.frame(issue_id <- integer(),
                        journal_id <- integer(),
                        volume <- character(),
                        issue <- character(),
                        pubDate <- character(),
                        stringsAsFactors = F)

df.journals <- data.frame(journal_id <- integer(),
                          issn <- character(),
                          title <- character(),
                          isoAbbreviation <- character(),
                          stringsAsFactors = F)

df.authors <- data.frame(author_id <- integer(),
                        lastName <- character(),
                        foreName <- character(),
                        initials <- character(),
                        suffix <- character(),
                        affiliation <- character(),
                        collectiveName <- character(),
                        stringsAsFactors = F)

df.articleAuthor <- data.frame(article_author_id <- integer(),
                               pmid <- integer(),
                               author_id <- integer())

# Maps to transfer month and season to required month format
# Some journals are published by season rather than month, here we transfer them to the first month of each season
seasonMap = c(Spring = "03", Summer = "06", Fall = "09", Winter = "12")
monthMap = c(Jan = "01", Feb = "02", Mar = "03", Apr = "04", May = "05", Jun = "06", 
             Jul = "07", Aug = "08", Sep = "09", Oct = "10", Nov = "11", Dec = "12")

# Functions to extract data from XML that would be saved in various data frames
# The result would be a list of different elements regarding one entity
getOneArticle <- function(node) {
    pmid <- as.integer(xmlGetAttr(node, "PMID"))
    
    articleTitle <- xpathSApply(node, "./PubDetails/ArticleTitle", xmlValue)
    
    oneArticle = list(pmid, articleTitle)
    return(oneArticle)
}

getOneIssue <- function(node) {
    path <- "./PubDetails/Journal/JournalIssue/"
    
    # Deal with `Volume` element lacked
    volume <- xpathSApply(node, paste0(path, "Volume"), xmlValue)
    volume <- ifelse(length(volume) > 0, volume, "")
    
    # Deal with `Issue` element lacked
    issue <- xpathSApply(node, paste0(path, "Issue"), xmlValue)
    issue <- ifelse(length(issue) > 0, issue, "")
    
    # Deal with `Year` or `MedlineDate` element lacked
    year <- xpathSApply(node, paste0(path, "PubDate/Year"), xmlValue)
    medlineDate <- xpathSApply(node, paste0(path, "PubDate/MedlineDate"), xmlValue)
    year <- ifelse(length(year) > 0,
                   year,
                   ifelse(length(medlineDate) > 0,
                          substring(medlineDate, 1, 4),
                          "0000"))
    
    # Deal with `Month` or `Season` or `MedlineDate` element lacked
    month <- xpathSApply(node, paste0(path, "PubDate/Month"), xmlValue)
    season <- xpathSApply(node, paste0(path, "PubDate/Season"), xmlValue)
    month <- ifelse(length(month) > 0,
                    ifelse(is.na(monthMap[month]), month, monthMap[month]),
                    ifelse(length(medlineDate) > 0,
                           monthMap[substring(medlineDate, 6, 8)],
                           ifelse(length(season) > 0,
                                  seasonMap[season],
                                  "00")))
    
    # Deal with `Day` element lacked
    day <- xpathSApply(node, paste0(path, "PubDate/Day"), xmlValue)
    day <- ifelse(length(day) > 0, day, "00")
    
    # Build required standard `pubDate` format `YYYY-MM-DD`
    pubDate <- paste(year, month, day, sep="-")
    
    oneIssue <- list(volume, issue, pubDate)
    return(oneIssue)
}

getOneJournal <- function(node) {
    path <- "./PubDetails/Journal/"
    
    # Deal with `ISSN` element lacked
    issn <- xpathSApply(node, paste0(path, "ISSN"), xmlValue)
    issn <- ifelse(length(issn) > 0, issn, "")
    
    title <- xpathSApply(node, paste0(path, "Title"), xmlValue)
    
    isoAbbreviation <- xpathSApply(node, paste0(path, "ISOAbbreviation"), xmlValue)
    
    oneJournal <- list(issn, title, isoAbbreviation)
    return(oneJournal)
}

getAuthors <- function(node) {
    # As there are several `Author`s in each `Article` element, a list is created to store all `oneAuthor`s
    authors <- list()
    authorsNode <- xpathSApply(node, "./PubDetails/AuthorList/Author")
    
    for (i in 1:length(authorsNode)) {
        path <- paste0("./PubDetails/AuthorList/Author[", i, "]/")
        
        # Deal with `LastName` element lacked
        lastName <- xpathSApply(node, paste0(path, "LastName"), xmlValue)
        lastName <- ifelse(length(lastName) > 0, lastName, "")
        
        # Deal with `ForeName` element lacked
        foreName <- xpathSApply(node, paste0(path, "ForeName"), xmlValue)
        foreName <- ifelse(length(foreName) > 0, foreName, "")
        
        # Deal with `Initials` element lacked
        initials <- xpathSApply(node, paste0(path, "Initials"), xmlValue)
        initials <- ifelse(length(initials) > 0, initials, "")
        
        # Deal with `Suffix` element lacked
        suffix <- xpathSApply(node, paste0(path, "Suffix"), xmlValue)
        suffix <- ifelse(length(suffix) > 0, suffix, "")
        
        # Deal with `Affiliation` element lacked
        affiliation <- xpathSApply(node, paste0(path, "Affiliation"), xmlValue)
        affiliation <- ifelse(length(affiliation) > 0, affiliation, "")
        
        # Deal with `CollectiveName` element lacked
        collectiveName <- xpathSApply(node, paste0(path, "CollectiveName"), xmlValue)
        collectiveName <- ifelse(length(collectiveName) > 0, collectiveName, "")
        
        oneAuthor <- list(lastName, foreName, initials, suffix, affiliation, collectiveName)
        authors <- append(authors, list(oneAuthor))
    }

    return(authors)
}

# Hash tables to store unique records that have been inserted to data frames
issueHash = new.env(hash=T)
journalHash = new.env(hash=T)
authorHash = new.env(hash=T)
articleAuthorHash = new.env(hash=T)

# Loop through each `Article` node to extract data and insert into corresponding data frames
# For each entity, compare with corresponding hash table to check duplicates
# If not exists in the hash table, create a new record in the hash table and the data frame
# Then get the PK of the data frame to be used as FKs in other tables
for (i in 1:articleNum) {
    articleNode <- root[[i]]
    
    # Data for table `Journal`
    oneJournal <- getOneJournal(articleNode)
    oneJournalStr <- toString(oneJournal)

    if (is.null(journalHash[[oneJournalStr]])) {
        journalHash[[oneJournalStr]] <- TRUE
        journalNum <- as.integer(length(journalHash))
        df.journals <- rbind(df.journals, data.frame(journal_id = journalNum,
                                                     issn = oneJournal[[1]],
                                                     title = oneJournal[[2]],
                                                     isoAbbreviation = oneJournal[[3]],
                                                     stringsAsFactors = F))
    }
    
    journal_id <- df.journals$journal_id[which(df.journals$issn == oneJournal[[1]] &
                                               df.journals$title == oneJournal[[2]] &
                                               df.journals$isoAbbreviation == oneJournal[[3]])]
    
    # Data for table `Issues`
    oneIssue <- append(getOneIssue(articleNode), journal_id)
    oneIssueStr <- toString(oneIssue)
    
    if (is.null(issueHash[[oneIssueStr]])) {
        issueHash[[oneIssueStr]] <- TRUE
        issueNum <- as.integer(length(issueHash))
        df.issues <- rbind(df.issues, data.frame(issue_id = issueNum,
                                                 journal_id = journal_id,
                                                 volume = oneIssue[[1]],
                                                 issue = oneIssue[[2]],
                                                 pubDate = oneIssue[[3]],
                                                 stringsAsFactors = F))
    }
    
    issue_id <- df.issues$issue_id[which(df.issues$journal_id == journal_id &
                                         df.issues$volume == oneIssue[[1]] &
                                         df.issues$issue == oneIssue[[2]] &
                                         df.issues$pubDate == oneIssue[[3]])]
    
    # Data for table `Articles`
    # The loop is for each `Article` node, so no need to check duplicates here
    oneArticle <- getOneArticle(articleNode)
    
    df.articles <- rbind(df.articles, data.frame(pmid = oneArticle[[1]],
                                                 issue_id = issue_id,
                                                 articleTitle = oneArticle[[2]],
                                                 stringsAsFactors = F))
    
    pmid <- df.articles$pmid[i]
    
    # Data for table `Authors`
    authors <- getAuthors(articleNode)
    
    for (oneAuthor in authors) {
        oneAuthorStr <- toString(oneAuthor)

        if (is.null(authorHash[[oneAuthorStr]])) {
            authorHash[[oneAuthorStr]] <- TRUE
            authorNum <- as.integer(length(authorHash))
            
            df.authors <- rbind(df.authors, data.frame(author_id = authorNum,
                                                       lastName = oneAuthor[[1]],
                                                       foreName = oneAuthor[[2]],
                                                       initials = oneAuthor[[3]],
                                                       suffix = oneAuthor[[4]],
                                                       affiliation = oneAuthor[[5]],
                                                       collectiveName = oneAuthor[[6]],
                                                       stringsAsFactors = F))
        }
        
        author_id <- df.authors$author_id[which(df.authors$lastName == oneAuthor[[1]] &
                                                df.authors$foreName == oneAuthor[[2]] &
                                                df.authors$initials == oneAuthor[[3]] &
                                                df.authors$suffix == oneAuthor[[4]] &
                                                df.authors$affiliation == oneAuthor[[5]] &
                                                df.authors$collectiveName == oneAuthor[[6]])]
    
    
        # Data for table `ArticleAuthor`
        oneArticleAuthor <- list(pmid, author_id)
        oneArticleAuthorStr <- toString(oneArticleAuthor)
        
        if (is.null(articleAuthorHash[[oneArticleAuthorStr]])) {
            articleAuthorHash[[oneArticleAuthorStr]] <- TRUE
            articleAuthorNum <- as.integer(length(articleAuthorHash))
            
            df.articleAuthor <- rbind(df.articleAuthor, data.frame(article_author_id = articleAuthorNum,
                                                                   pmid = pmid,
                                                                   author_id = author_id,
                                                                   stringsAsFactors = F))
        }
    }
}

# Write data frames to database tables
dbWriteTable(conn, "Journals", df.journals, overwrite = F, append = T, row.names = F)
dbWriteTable(conn, "Issues", df.issues, overwrite = F, append = T, row.names = F)
dbWriteTable(conn, "Articles", df.articles, overwrite = F, append = T, row.names = F)
dbWriteTable(conn, "Authors", df.authors, overwrite = F, append = T, row.names = F)
dbWriteTable(conn, "ArticleAuthor", df.articleAuthor, overwrite = F, append = T, row.names = F)

# Record total running time
# Normally the running time is 16-18 minutes in my computer (AMD5600U+32g)
et <- Sys.time()
cat("Time elapsed: ", round((et - bt), 2), " minutes.")

# Disconnect to database
dbDisconnect(conn)
rm(list = ls())
