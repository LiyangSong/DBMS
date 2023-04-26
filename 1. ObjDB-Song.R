# Subject: CS5200 Build Hierarchical Document Database
# Author: Liyang Song
# Date: Jan 20, 2023

# Note: I created a user interaction design but the professor said I should not
# My initial purpose is to make it easier to call and test functions
# So I make all those related codes hashtagged

# Initiate a global variable to store the name of data base folder
rootDir <- "docDB"

#
main <- function() {
  
  # Create folders under the given path
  # The given path can be ignored which will be set as its default value "."
  # Error message will be thrown out when the given path does not exist
  # Folder creation will only be executed when the target folder does not exist
  configDB <- function(root, path = ".") {
    if (path == "") {
      path <- "."
    }
    
    if (!dir.exists(path)) {
      cat("Error: Not a valid path.\n")
    } else {
      root <- paste0(path, "/", root)
      if (!dir.exists(root)) {
        dir.create(root)
        cat(root, "folder has been created under", path, ".\n")
      } else {
        cat(root, "folder exists already under", path, ".\n")
      }
    }
  }
  
  # Generate path for tag folders under the root directory
  # Error message will be thrown out when the given tag does not start with "#"
  genObjPath <- function(root, tag) {
    if (!substr(tag, 1, 1) == "#") {
      cat("Error: Not a valid tag.\n")
    } else {
      tag <- sub("#", "", tag)
      objPath <- paste0(root, "/", tag)
      cat(objPath, "\n")
      return(objPath)
    }
  }
  
  # Create a function to deal with file name strings
  # Error message will be thrown out when the given file name
  # does not contain "#" or "." or have "#" or "." at head or tail position
  # Name Sting will be split by "#"
  # Space at head or tail position of results would be trimmed
  # Different name formats in Win and Mac systems are all considered
  # Return results composed by the required file names and tag name vectors
  splitNames <- function(fileName) {
    if (!grepl("#", fileName) ||
        !grepl("\\.", fileName) ||
        (substr(fileName, 1, 1) == "#") || 
        (substr(fileName, 1, 1) == ".") || 
        (substr(fileName, nchar(fileName), nchar(fileName)) == "#") || 
        (substr(fileName, nchar(fileName), nchar(fileName)) == ".")) {
      cat("Error: Not a valid file name.\n")
    } else {
      names <- strsplit(fileName, "#")[[1]]
      names <- gsub("^\\s+|\\s+$", "", names)
      
      if (grepl("\\.", names[1])) {
        tags <- names[2:length(names)]
        actualFileName <- names[1]
      } else {
        tailLeft <- strsplit(tail(names, 1), "\\.")[[1]][1]
        tailRight <- strsplit(tail(names, 1), "\\.")[[1]][2]
        
        if (length(names) > 2){
          tags <- names[2:(length(names)-1)]
        } else {
          tags <- list()
        }
        
        tags <- append(tags, tailLeft)
        actualFileName <- paste0(names[1], ".", tailRight)
      }
      
      tags <- paste0("#", tags)
      result <- list(a=tags, b=actualFileName)
      return(result) 
    }
  }
  
  # Call the splitNames function to deal with file names
  # Print the required file names part from splitNames function result
  getTags <- function(fileName) {
    tryCatch({
      tags <- splitNames(fileName)$a
      cat(tags, "\n")
      return(tags)
    }, error = function(e) {
      cat("Error: split file name fails.\n")
    })
  }
  
  # Call the splitNames function to deal with file names
  # Print the tag name vectors part from splitNames function result
  getFileName <- function(fileName) {
    tryCatch({
      actualFileName <- splitNames(fileName)$b
      cat(actualFileName, "\n")
      return(actualFileName)
    }, error = function(e) {
      cat("Error: split file name fails.\n")
    })
  }
  
  # Retrieve all files in the given folder
  # Call configDB function to create the rootDir folder
  # Call getTags function to get tags of all files
  # Call genObjPath and configDB function to create tag folders under the rootDir folder
  # Call getFileName function to get required names of all files
  # Copy files to corresponding tag folders and rename files to their required names
  storeObjs <- function(folder, root, verbose=TRUE) {
    if (!dir.exists(folder)) {
      cat("Error: Not a valid folder.\n")
    } else {
      fileNames <- list.files(path = folder)
      if (length(fileNames) < 1) {
        cat("Error: No file found in the", folder, "folder.\n")
      } else {
        configDB(root)
        
        for (fileName in fileNames) {
          originPath <- paste0(folder, "/", fileName)
          actualFileName <- getFileName(fileName)
          tags <- getTags(fileName)
          
          if (verbose) {
            folderNames <- ""
            for (tag in tags) {
              folderNames <- paste0(folderNames, substr(tag, 2, nchar(tag)), ", ")
            }
            folderNames <- substr(folderNames, 1, nchar(folderNames)-2) 
            cat("Copying", actualFileName, "to", folderNames, "\n")
          }
          
          for (tag in tags) {
            targetDir <- genObjPath(root, tag)
            configDB(targetDir)
            targetPath <- paste0(targetDir, "/", actualFileName)
            if (!file.exists(targetPath)) {
              file.copy(originPath, targetPath)
              cat(actualFileName, "has been copied under", targetDir, "folder.\n")
            } else {
              cat(actualFileName, "exists already under", targetDir, "folder.\n")
            }
          }
        }
      }
    }
  }
  
  # Remove all files and folders under the rootDir folder
  # Error message will be thrown out when the rootDir folder does not exist
  clearDB <- function(root) {
    if (!dir.exists(root)) {
      cat("Error: rootDir folder has not been created.\n")
    } else {
      unlink(paste0(root,"/*"), recursive = TRUE)
      cat("rootDir folder has been cleared.\n")
    }
  }
  
  # Provide test options to the user and get user choices
  # choice <- function() {
  #   cat("=============================\n")
  #   cat("[1] configDB(root, path)\n")
  #   cat("[2] genObjPath(root, tag)\n")
  #   cat("[3] getTags(fileName)\n")
  #   cat("[4] getFileName(fileName)\n")
  #   cat("[5] storeObjs(foler, root, verbose)\n")
  #   cat("[6] clearDB(root)\n")
  #   cat("[7] Quit Test\n")
  #   cat("=============================\n")
  #   
  #   return(readline(prompt = "Choose one function to test: "))
  # }
  
  # Execute functions according to user choices from choice function
  # Error message will be thrown out when the choice is not within 1-7
  # test <- function() {
  #   while (TRUE) {
  #     i <- choice()
  #     if (i == 1) {
  #       path <- readline(prompt = "Input a path to set up rootDir (optional): ")
  #       configDB(rootDir, path)
  #     } else if (i == 2) {
  #       tag <- readline(prompt = "Input a tag name: ")
  #       genObjPath(rootDir, tag)
  #     } else if (i == 3) {
  #       fileName <- readline(prompt = "Input a fileName: ")
  #       getTags(fileName)
  #     } else if (i == 4) {
  #       fileName <- readline(prompt = "Input a fileName: ")
  #       getFileName(fileName)
  #     } else if (i == 5) {
  #       folder <- readline(prompt = "Input the file folder: ")
  #       verbose <- readline(prompt = "Input the verbose: ")
  #       storeObjs(folder, rootDir, verbose)
  #     } else if (i == 6) {
  #       clearDB(rootDir)
  #     } else if (i == 7) {
  #       break
  #     } else {
  #       cat("Error: not a valid selection.\n")
  #     }
  #   }
  # }
  
  # test()
  
  # Test functions directly
  cat("=============================\n")
  cat("Test configDB: \n")
  configDB(rootDir, "")
  
  cat("=============================\n")
  cat("Test genObjPath: \n")
  genObjPath(rootDir, "#city")
  
  cat("=============================\n")
  cat("Test getTags: \n")
  getTags("Hongkong #city #summer #night.JPG")
  
  cat("=============================\n")
  cat("Test getFileName: \n")
  getFileName("Hongkong #city #summer #night.JPG")
  
  cat("=============================\n")
  cat("Test storeObjs: \n")
  storeObjs("files", rootDir, TRUE)
  
  cat("=============================\n")
  cat("Test clearDB: \n")
  clearDB(rootDir)
}

main()
quit()
