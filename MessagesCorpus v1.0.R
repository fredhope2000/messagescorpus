#Message Corpus Creator!

#Copyright 2014 by Fred Hope, in collaboration with Mark Myslin.

#files to be read must be in the form: [Name]-.*
#( [Name] can't contain hyphens)


#inputs two lines, returns the number of seconds elapsed between the first line and the second line
compareTimestamps <- function(line1,line2)
{
	time1 <- as.POSIXct(line1)
	time2 <- as.POSIXct(line2)
	return (as.numeric(difftime(time2,time1,units="secs")))
}

#this function inputs a number (of seconds) and outputs the timestamp as a string
convertTime <- function(seconds) {
	newdate <- as.POSIXlt(as.numeric(seconds), origin="2001-01-01")
	return(as.character(newdate))
	}

#adds escape charaters to (,),etc in string
escapedVersion <- function(string)
{
	if (nchar(string) < 1)
	{
		return(string)
	}
	newstring <- ""
	for (i in 1:nchar(string))
	{
		character <- substr(string,i,i)
		if (character == "(" || character == ")" || character == "+" || character == "-" || character == "[" || character == "]" || character == "." || character == "*" || character == "^" || character == "?")
		{
			paste("\\", character, sep="") -> character
		}
		paste(newstring, character, sep="") -> newstring
	}
	return(newstring)
}

#inputs "Name\tTimestamp\tMessage", outputs Name
extractName <- function(string)
{
	gsub(" *\t.*","",string) -> string
	return(string)
}

#inputs "Name\tTimestamp\tMessage", outputs Timestamp
extractTimestamp <- function(string)
{
	sub(".*?\t","",string) -> string
	gsub("[ \\*]\t.*","",string) -> string
	return(string)
}

findLatencies <- function(string=1,start=0,stop=0)
{
	if (start > 0 && stop >= start)
	{
		stop <- min(stop,length(tables[[string]]))
		table <- tables[[string]][start:stop]
	} else
	{
		table <- tables[[string]]
	}
	line <- numeric()
	otherName <- numeric()
	latencies <- vector(mode="list",length=2)
	for (i in seq_along(table))
	{
		name <- extractName(table[i])
		time <- extractTimestamp(table[i])
		if (length(otherName) == 0 && name != myName)
		{
			otherName <- name
			names(latencies) <- c(myName,otherName)
			latencies[[myName]] <- vector(mode="list",length=2)
			latencies[[otherName]] <- vector(mode="list",length=2)
			names(latencies[[myName]]) <- c("latencies","timestamps")
			names(latencies[[otherName]]) <- c("latencies","timestamps")
		}
		if (i > 1 && (name != mostRecentName || compareTimestamps(mostRecentTime,time) >= 3600))
		{
			latencies[[name]][[1]] <- append(latencies[[name]][[1]],compareTimestamps(mostRecentTime,time))
			latencies[[name]][[2]] <- append(latencies[[name]][[2]],as.POSIXct(time))
		}
		mostRecentTime <- time
		mostRecentName <- name
	}
	return(latencies)
}

generateHistograms <- function(latencies)
{
	breaks <- seq(0, log(max(c(latencies[[1]]$latencies,latencies[[2]]$latencies)),base=10)+0.2, by = 0.2)
	hist(log(latencies[[1]]$latencies,base=10),breaks=breaks,plot=FALSE) -> myHistogram
	hist(log(latencies[[2]]$latencies,base=10),breaks=breaks,plot=FALSE) -> theirHistogram
	return(list(myHistogram,theirHistogram))
}

generateProportions <- function(histograms)
{
	breaks <- numeric()
	proportions <- numeric()
	for (i in 1:(length(histograms[[1]]$breaks) - 1))
	{
		breaks[i] <- histograms[[1]]$breaks[i]
		proportions[i] <- (histograms[[1]]$counts[i] + 1)/(histograms[[2]]$counts[i] + 1)
	}
	result <- list(breaks,proportions)
	names(result) <- c("x","y")
	return(result)
}

getHour <- function(string)
{
	return(as.POSIXlt(string)$hour)
}

neighborhood <- function(index,length=15,table=1)
{
	if (length < 1) { length <- 15 }
	catTables(table=table,start=(index-3),stop=(index+length-4))
}

#index can be any number within the bounds of tables, or any name in names(tables). use index=0 (default) to search all conversations
searchCorpus <- function(pattern,index=0,ignore.case=TRUE)
{
	if (index != 0)
	{
		if (is.numeric(index) && length(tables) < index)
		{
			return(cat(paste("No table of index ",index," exists. Index must be less than or equal to ",length(tables),".\n",sep="")))
		} else
		{
			if (length(tables[[index]]) > 0)
			{
				results <- numeric()
				if (length(grep(pattern,tables[[index]],ignore.case=ignore.case)) > 0)
				{
					if (length(grep(index,names(tables))) <= 0)
					{
						name <- names(tables)[index]
					} else
					{
						name <- index
					}
					lines <- grep(pattern,tables[[index]],ignore.case=ignore.case)
					maxlinelength <- nchar(max(lines))
					for (j in seq_along(lines))
					{
						while (nchar(lines[j]) < maxlinelength)
						{
							lines[j] <- paste(0,lines[j],sep="")
						}
					}
					strings <- grep(pattern,tables[[index]],ignore.case=ignore.case,value=TRUE)
					results <- c(paste("---Matches in conversations with ",name,"---",sep=""),paste(lines,strings,sep="\t"))
				}
				if (length(results) > 0)
				{
					return(cat(results,sep="\n"))
				} else
				{
					return(cat("No results found.\n"))
				}
			} else
			{
				return(cat("No table found under that name.\n"))
			}
		}
	} else
	{
		results <- numeric()
		for (i in seq_along(tables))
		{
			if (length(grep(pattern,tables[[i]],ignore.case=ignore.case)) > 0)
			{
				lines <- grep(pattern,tables[[i]],ignore.case=ignore.case)
				maxlinelength <- nchar(max(lines))
				for (j in seq_along(lines))
				{
					while (nchar(lines[j]) < maxlinelength)
					{
						lines[j] <- paste(0,lines[j],sep="")
					}
				}
				strings <- grep(pattern,tables[[i]],ignore.case=ignore.case,value=TRUE)
				results <- c(results,paste("---Matches in conversations with ",names(tables)[i],"---",sep=""),paste(lines,strings,sep="\t"))
			}
		}
		if (length(results) > 0)
		{
			return(cat(results,sep="\n"))
		} else
		{
			return(cat("No results found.\n"))
		}
	}
}

#inputs "<integer>x</integer>", outputs x
senderCodeFromString <- function(string)
{
	string <- sub("<integer>","",string)
	string <- sub("</integer>","",string)
	return (as.integer(string))
}

#inputs a vector where each element is a message line. sorts them chronologically and returns sorted vector
sortByTime <- function(originalstatements)
{
	length <- length(originalstatements)
	if (length < 1) { return(originalstatements) }
	justdates <- numeric(length)
	sortedstatements <- numeric(length)
	for (i in 1:length)
	{
		justdates[i] <- paste(extractTimestamp(originalstatements[i]),format(i,width=20),sep="")
	}
	sort(justdates) -> justdates
	for (i in 1:length)
	{
		justdates[i] <- substr(justdates[i],20,99)
	}
	justdates <- as.integer(justdates)
	for (i in 1:length)
	{
		sortedstatements[i] <- originalstatements[justdates[i]]
	}
	return(sortedstatements)
}

latenciesBySize <- function(min=0,max=-1,unit="sec")
{
	if (exists("latencies"))
	{
		if (length(latencies) < 1)
		{
			latenciesBySize <- findLatencies(string=1)
		} else
		{
			latenciesBySize <- latencies
		}
	} else
	{
		latenciesBySize <- findLatencies(string=1)
	}
	if (unit == "min")
	{
		min <- min * 60
		max <- max * 60
	} else if (unit == "hour")
	{
		min <- min * 3600
		max <- max * 3600
	} else if (unit == "day")
	{
		min <- min * 86400
		max <- max * 86400
	}
	if (max < 0)
	{
		max <- max(c(latenciesBySize[[1]]$latencies,latenciesBySize[[2]]$latencies))
	}
	i <- 1
	while (i <= length(latenciesBySize[[1]]$latencies))
	{
		isInSizeInterval <- (latenciesBySize[[1]]$latencies[i] >= min && latenciesBySize[[1]]$latencies[i] <= max)
		if (!isInSizeInterval)
		{
			latenciesBySize[[1]]$latencies <- latenciesBySize[[1]]$latencies[-i]
			latenciesBySize[[1]]$timestamps <- latenciesBySize[[1]]$timestamps[-i]
			i <- i - 1
		}
		i <- i + 1
	}
	i <- 1
	while (i <= length(latenciesBySize[[2]]$timestamps))
	{
		isInSizeInterval <- (latenciesBySize[[2]]$latencies[i] >= min && latenciesBySize[[2]]$latencies[i] <= max)
		if (!isInSizeInterval)
		{
			latenciesBySize[[2]]$latencies <- latenciesBySize[[2]]$latencies[-i]
			latenciesBySize[[2]]$timestamps <- latenciesBySize[[2]]$timestamps[-i]
			i <- i - 1
		}
		i <- i + 1
	}
	return(latenciesBySize)
}

latenciesByHour <- function(from=0,to=0)
{
	if (exists("latencies"))
	{
		if (length(latencies) < 1)
		{
			latenciesByHour <- findLatencies(string=1)
		} else
		{
			latenciesByHour <- latencies
		}
	} else
	{
		latenciesByHour <- findLatencies(string=1)
	}
	i <- 1
	while (i <= length(latenciesByHour[[1]]$timestamps))
	{
		if (to >= from)
		{
			isInTimeInterval <- (getHour(latenciesByHour[[1]]$timestamps[i]) >= from && getHour(latenciesByHour[[1]]$timestamps[i]) < to)
		} else
		{
			isInTimeInterval <- (getHour(latenciesByHour[[1]]$timestamps[i]) >= from || getHour(latenciesByHour[[1]]$timestamps[i]) < to)
		}
		if (!isInTimeInterval)
		{
			latenciesByHour[[1]]$latencies <- latenciesByHour[[1]]$latencies[-i]
			latenciesByHour[[1]]$timestamps <- latenciesByHour[[1]]$timestamps[-i]
			i <- i - 1
		}
		i <- i + 1
	}
	i <- 1
	while (i <= length(latenciesByHour[[2]]$timestamps))
	{
		if (to >= from)
		{
			isInTimeInterval <- (getHour(latenciesByHour[[2]]$timestamps[i]) >= from && getHour(latenciesByHour[[2]]$timestamps[i]) < to)
		} else
		{
			isInTimeInterval <- (getHour(latenciesByHour[[2]]$timestamps[i]) >= from || getHour(latenciesByHour[[2]]$timestamps[i]) < to)
		}
		if (!isInTimeInterval)
		{
			latenciesByHour[[2]]$latencies <- latenciesByHour[[2]]$latencies[-i]
			latenciesByHour[[2]]$timestamps <- latenciesByHour[[2]]$timestamps[-i]
			i <- i - 1
		}
		i <- i + 1
	}
	return(latenciesByHour)
}

#sort the lines and remove duplicate entries
cleanUpTable <- function(table)
{
	table <- sortByTime(table)
	i <- length(table)
	while (i >= 1)
	{
		if (is.na(table[i]))
		{
			table <- table[-i]
		}
		i <- i - 1
	}
	i <- length(table)
	while (i > 1)
	{
		if (gsub("\\*\t"," \t",table[i]) == gsub("\\*\t"," \t",table[i-1]))
		{
			table <- table[-i]
		} else if (i > 2 && gsub("\\*\t"," \t",table[i]) == gsub("\\*\t"," \t",table[i-2]))
		{
			table <- table[-i]
		} else if (i > 3 && gsub("\\*\t"," \t",table[i]) == gsub("\\*\t"," \t",table[i-3]))
		{
			table <- table[-i]
		} else
		{
			gsub("&lt;","<",table[i]) -> table[i]
			gsub("&gt;",">",table[i]) -> table[i]
			gsub("&amp;","&",table[i]) -> table[i]
		}
		i <- i - 1
	}
	return(table)
}

#this is the main function of this file. it inputs a vector of lines from a log file, and outputs a formatted table of messages
generateTable <- function(myName,myEmail,otherName,maxNameLength,file)
{

#remove huge blocks of alphanumeric characters, which represent media
file <- file[!(grepl("\t\t\t[A-Za-z0-9/\\+]{52}",file))]

#fix names to be the same length
while (nchar(myName) < maxNameLength)
{
	paste(myName," ",sep="") -> myName
}
while (nchar(otherName) < maxNameLength)
{
	paste(otherName," ",sep="") -> otherName
}

gsub("\t*<","<",file) -> file

#determine who started the conversation
line <- grep(paste("E:",myEmail,sep=""),file,fixed=TRUE)
if (length(line) == 0)
{
	return("*Error processing file. Ensure \"myEmail\" variable is set correctly.*")
} else if (substr(file[line+1],1,8) == "<string>" && length(grep(myEmail,file[line+1],fixed=TRUE)) == 0)
{
	conversationStarter <- 2
} else
{
	line <- line + 1
	while (substr(file[line],1,8) != "<string>" && line <= length(file))
	{
		line <- line + 1
	}
	if (line > length(file))
	{
		conversationStarter <- 1
	} else
	{
		if (length(grep(myEmail,file[line],fixed=TRUE)) > 0)
		{
			conversationStarter <- 1
		} else
		{
			conversationStarter <- 2
		}
	}
}

#if there are messages of more than one line, concatenate them together
line <- 1
while (line <= length(file))
{
	if (nchar(file[line]) > 0 && substr(file[line],1,1) != "<" && substr(file[line-1],1,8) == "<string>")
	{
		file[line-1] <- paste(file[line-1],file[line],sep="\n\t")
		file <- file[-line]
		line <- line - 1
	}
	line <- line + 1
}

#remove all but the following lines:
#<key>NS.time</key>, <key>NS.string</key>, <key>Sender</key>, <string>.*, <real>.*, <integer>.*
file <- file[(grepl("<key>NS.time",file,fixed=TRUE) | grepl("<key>NS.string",file,fixed=TRUE) | grepl("<key>Sender</key>",file,fixed=TRUE) | grepl("<string>",file,fixed=TRUE) | grepl("<real>",file,fixed=TRUE) | grepl("<integer>",file,fixed=TRUE))]

#also remove <string>.* and <real>.* and <integer>.* lines too, unless they follow NS.string, NS.time, or Sender lines respectively
line <- 1
while (line <= length(file))
{
	if (nchar(file[line]) > 2)
	{
		if (line > 1) { previousline <- line - 1 } else { previousline <- line }
		thislineshort <- substr(file[line],1,6)
		thislinelong <- substr(file[line],1,12)
		prevline <- substr(file[previousline],1,12)
		if (thislineshort == "<strin" && prevline != "<key>NS.stri")
		{
			file <- file[-line]
			line <- line - 1
		} else if (thislineshort == "<strin" && substr(file[line],9,17) == "</string>")
		{
			file <- file[-line]
			line <- line - 1
		} else if (thislineshort == "<real>" && prevline != "<key>NS.time")
		{
			file <- file[-line]
			line <- line - 1
		} else if (thislineshort == "<integ" && prevline != "<key>Sender<")
		{
			file <- file[-line]
			line <- line - 1
		}
	}
	line <- line + 1
}

#now that we know which <string> lines to keep, we can get rid of the <key> lines, even the <key>NS.string ones
#also change the hex strings to media
line <- 1
while (line <= length(file))
{
	linelength <- nchar(file[line])
	if (linelength > 2)
	{
		if (substr(file[line],1,5) == "<key>")
		{
			file <- file[-line]
			line <- line - 1
		}
		gsub("[[:alnum:]]{8}-[[:alnum:]]{4}-[[:alnum:]]{4}-[[:alnum:]]{4}-[[:alnum:]]{12}","(MEDIA)",file[line]) -> file[line]
	} else if (linelength < 1)
	{
		file <- file[-line]
		line <- line - 1
	}
	line <- line + 1
}

#get rid of the <string> tags themselves
gsub("<string>","",file) -> file
gsub("</string>","",file) -> file

#remove line 2 from the file, if it's the home user's phone/email (it often is)
while ((length(grep("1?[[:digit:]]{10}",file[2])) > 0 && substr(file[2],1,6) != "<real>") || length(grep(".+@.+\\..+",file[2])) > 0)
{
	file <- file[-2]
}

#the lines with a number between <real> tags are timestamps. <integer> tags are sender codes
grep("<real>",file) -> timestamps
grep("<integer>",file) -> sendercodes

#remove the last line, if it's the other person's phone/email (it often is)
line <- length(file)
if (!((line - 1) %in% sendercodes) && !(((line - 2) %in% sendercodes) && ((line - 1) %in% timestamps)) && (length(grep("1?[[:digit:]]{10}",file[line])) > 0 || length(grep(".+@.+\\..+",file[line])) > 0))
{
	file <- file[-line]
	line <- line - 1
}

#if the last line is now a timestamp, which it sometimes is, remove it, as it is meaningless
if (nchar(file[line]) > 2)
{
	while (line >= 1 && substr(file[line],1,6) == "<real>")
	{
		file <- file[-line]
		line <- line - 1
	}
}	

grep("<integer>",file) -> sendercodes
grep("<real>",file) -> timestamps

#if there are timestamps exactly two lines apart, it's because a blank message got deleted, so delete the extra timestamp
line <- 1
while (line < length(file))
{
	nextline <- line + 2
	if ((line %in% timestamps) & (nextline %in% timestamps))
	{
		line <- line - 1
		file <- file[-line]
		file <- file[-line]
		grep("<real>",file) -> timestamps
		grep("<integer>",file) -> sendercodes
		line <- line - 1
	} else if ((line %in% sendercodes) & ((line+1) %in% sendercodes))
	{
		file <- file[-line]
		grep("<real>",file) -> timestamps
		grep("<integer>",file) -> sendercodes
		line <- line - 1
	}
	line <- line + 1
}

#if a (MEDIA) message immediately follows a regular message (they were sent together), concatenate them
line <- 1
while (line <= length(file))
{
	if (file[line] == "(MEDIA)")
	{
		if (!((line-1) %in% timestamps) && !((line-1) %in% sendercodes))
		{
			paste("(MEDIA)",file[line-1]) -> file[line-1]
			file <- file[-line]
			line <- line - 1
			grep("<real>",file) -> timestamps
			grep("<integer>",file) -> sendercodes
		} else if (!((line+1) %in% timestamps) && !((line+1) %in% sendercodes))
		{
			paste("(MEDIA)",file[line+1]) -> file[line+1]
			file <- file[-line]
			line <- line - 1
			grep("<real>",file) -> timestamps
			grep("<integer>",file) -> sendercodes
		}
	}
	line <- line + 1
}

#strip the <real> and </real> from these strings so only the actual number remains. then convert it to a readable date/time
for (timestamp in timestamps)
{
	sub("</real>","",file[timestamp]) -> file[timestamp]
	sub("<real>","",file[timestamp]) -> realnumber
	paste("<real>",convertTime(realnumber)) -> file[timestamp]
}

grep("<integer>",file) -> sendercodes
grep("<real>",file) -> timestamps

#Note: the next few steps seem a bit arbitrary and repetitive, but this is the only way we have found to make it work for every file,
#due to oddities in the message logs that are beyond our control

#a sender code of 0 usually indicates a "iMessage with x" message that isn't a real message. delete these
i <- 1
while (i <= length(sendercodes))
{
	if (senderCodeFromString(file[sendercodes[i]]) == 0 && ((sendercodes[i] + 1) %in% timestamps) && (length(grep("iMessage with",file[sendercodes[i]+2])) > 0))
	{
		line <- sendercodes[i]
		file <- file[-line]
		file <- file[-line]
		file <- file[-line]
		i <- i - 1
		grep("<integer>",file) -> sendercodes
		grep("<real>",file) -> timestamps
	}
	i <- i + 1
}

#again, remove line 2 from the file, if it's the home user's phone/email (it often is)
while ((length(grep("1?[[:digit:]]{10}",file[2])) > 0 && substr(file[2],1,6) != "<real>") || length(grep(".+@.+\\..+",file[2])) > 0)
{
	file <- file[-2]
}

grep("<integer>",file) -> sendercodes
grep("<real>",file) -> timestamps

#also check for other stray phone/emails
i <- 1
while (i <= length(sendercodes))
{
	line <- sendercodes[i]
	if (((length(grep("1?[[:digit:]]{10}",file[line+1])) > 0 && substr(file[line+1],1,6) != "<real>") || length(grep(".+@.+\\..+",file[line+1])) > 0) && ((line + 2) %in% timestamps))
	{
		linetoremove <- line + 1
		file <- file[-linetoremove]
		i <- i - 1
		grep("<integer>",file) -> sendercodes
		grep("<real>",file) -> timestamps
	}
	i <- i + 1
}

#when a message doesn't have a timestamp, give it one equal to the previous timestamp before that
howmanyTimestampsToAdd <- 0
timestampsToAdd <- array(0, dim=c(2,length(sendercodes)))

mostRecentTimestamp <- 2 #default value, shouldn't ever be needed
for (sendercode in sendercodes)
{
	if ((sendercode+1) %in% timestamps)
	{
		mostRecentTimestamp <- sendercode + 1
	} else
	{
		howmanyTimestampsToAdd <- howmanyTimestampsToAdd + 1
		timestampsToAdd[1,howmanyTimestampsToAdd] <- sendercode
		timestampsToAdd[2,howmanyTimestampsToAdd] <- paste(file[mostRecentTimestamp],"*",sep="")
	}
}

i <- howmanyTimestampsToAdd
while (i > 0)
{
	append(file,timestampsToAdd[2,i],after=as.numeric(timestampsToAdd[1,i])) -> file
	i <- i - 1
}

grep("<integer>",file) -> sendercodes
grep("<real>",file) -> timestamps

#a sender code of 0 usually indicates a "iMessage with x" message that isn't a real message. delete these
i <- 1
while (i <= length(sendercodes))
{
	if (senderCodeFromString(file[sendercodes[i]]) == 0)
	{
		line <- sendercodes[i]
		
		file <- file[-line]
		file <- file[-line]
		file <- file[-line]
		i <- i - 1
		grep("<integer>",file) -> sendercodes
		grep("<real>",file) -> timestamps
	}
	i <- i + 1
}

#anything between a sender code and its timestamp isn't a real message, delete it
i <- 1
while (i <= length(sendercodes))
{
	if ((sendercodes[i] + 2) %in% timestamps)
	{
		line <- sendercodes[i] + 1
		file <- file[-line]
		grep("<integer>",file) -> sendercodes
		grep("<real>",file) -> timestamps
	} else if (!(((sendercodes[i] + 1) %in% timestamps) || ((sendercodes[i] + 2) %in% timestamps) || ((sendercodes[i] + 1) %in% sendercodes) || ((sendercodes[i] + 2) %in% sendercodes)))
	{
		line <- sendercodes[i] + 1
		file <- file[-line]
		i <- i - 1
		grep("<integer>",file) -> sendercodes
		grep("<real>",file) -> timestamps
	}
	i <- i + 1
}

grep("<integer>",file) -> sendercodes
grep("<real>",file) -> timestamps

#strip the <real> from the timestamps
for (timestamp in timestamps)
{
	timestampstring <- file[timestamp]
	if (substr(timestampstring,27,27) != "*")
	{
		#timestamps without asterisks can get a space at the end instead, so they're all the same length
		paste(timestampstring," ",sep="") -> timestampstring
	}
	sub("<real> ","",timestampstring) -> file[timestamp]
	
}

#get a vector of the various sender codes, should be at most two (0 has already been removed)
#the lower-numbered sender code is the person who started the conversation
senders <- sort(unique(senderCodeFromString(file[sendercodes])))

if (length(senders) == 0)
{
	return("") #no messages in this file, apparently
} else if (length(senders) == 1)
{
	if (conversationStarter == 1) #you started the conversation
	{
		gsub(paste("<integer>",senders[1],"</integer>",sep=""),myName,file) -> file
	} else #other person started the conversation
	{
		gsub(paste("<integer>",senders[1],"</integer>",sep=""),otherName,file) -> file
	}
} else
{
	if (conversationStarter == 1)
	{
		gsub(paste("<integer>",senders[1],"</integer>",sep=""),myName,file) -> file
		gsub("<integer>[[:digit:]]*</integer>",otherName,file) -> file
	} else
	{
		gsub(paste("<integer>",senders[1],"</integer>",sep=""),otherName,file) -> file
		gsub("<integer>[[:digit:]]*</integer>",myName,file) -> file
	}
}

#at this point, the lines in the file should go: sender code, timestamp, message, sender code, timestamp, message, etc
#take every three lines and concatenate them into one vector element
i <- 1
table <- numeric(length(file) / 3)
for (line in seq_along(file))
{
	if ((line %% 3) == 1)
	{
		paste(file[line],file[line+1],file[line+2],sep="\t") -> table[i]
		i <- i + 1
	}
}

return(table)
} #end of generateTable()

catTables <- function(table=1,start=1,stop=0)
{
	if (table > length(tables))
	{
		cat(paste("Table",table,"does not exist."))
	} else
	{
			cat(paste("---",names(tables)[table],"---\n",sep=""))
			if (start != 1 || stop != 0)
			{
				if (start >= 1 && start <= length(tables[[table]]) && stop >= start && stop <= length(tables[[table]]))
				{
					cat(tables[[table]][start:stop],sep="\n")
					cat("\n")
				} else {
					cat(tables[[table]],sep="\n")
					cat("\n")
				}
			} else {
				cat(tables[[table]],sep="\n")
				cat("\n")
			}
	}
}

maxLength <- function(strings)
{
	maxlength <- 1
	for (string in strings)
	{
		if (nchar(string) > maxlength) { maxlength <- nchar(string) }
	}
	return(maxlength)
}

prt <- proc.time()

#change these 7 lines as necessary
myName <- "MyName" #shows how your name displays in the conversation tables
myEmail <- "my@email.com" #your iCloud email shows up sometimes in the message logs, so put it here so things get parsed correctly
setwd("/Users/username/messagescorpus/") #directory containing the message log files to be inputted
excludeNames <- c("My Name") #gets messy when we use conversations with ourselves, debug this later
#use the following two lists to organize multiple numbers/addresses for the same person so that the script will group them together as a single person.
#if the person is in your contacts under their actual name, you don't need to include that in the duplicateNames list, only additional emails/numbers they have used
#the duplicateNames list shouldn't have any repeats, but the names(duplicateNames) list can; see examples below
duplicateNames <- list("bobspersonalemail@gmail.com", "bobsworkemail@company.net", "(555) 555-5555") #e.g. 555-555-5555 is Jim Smith's old number that's not in your contacts anymore
names(duplicateNames) <- c("Bob Johnson", "Bob Johnson", "Jim Smith")
oppressScanMessages <- FALSE
offset <- 7 #how many digits the unique ID at the beginning of each filename is

filenames <- list.files()

#filenames <- filenames[which(file.info(filenames)$size < 100000000)] #use this if you want; the number is in bytes

#for phone number titles, use the whole number. for names, use until the first hyphen
#phone number titles contain invisible characters; remove them
otherNames <- numeric()
for (i in seq_along(filenames))
{
	if (length(grep("\\+1 \\([[:digit:]]{3}\\) [[:digit:]]{3}-[[:digit:]]{4}",filenames[i])) == 1)
	{
		if (substr(filenames[i],1+offset,1+offset) != "+")
		{
			filenames[i] <- paste(substr(filenames[i],1,offset),substr(filenames[i],2+offset,99),sep="")
		}
		if (substr(filenames[i],18+offset,18+offset) != "-")
		{
			filenames[i] <- paste(substr(filenames[i],1,17+offset),substr(filenames[i],19+offset,99),sep="")
		}
		append(otherNames, substr(filenames[i],1+offset,17)) -> otherNames
	} else
	{
		append(otherNames, substr(gsub("-20.*","",filenames[i]),1+offset,99)) -> otherNames
	}
}

for (name in names(duplicateNames))
{
	if (!(name %in% otherNames))
	{
		append(otherNames, name) -> otherNames
	}
}

sort(otherNames) -> otherNames

#don't log any files whose name matches exactly a name in excludeNames
otherNames <- unique(otherNames)
for (name in excludeNames)
{
	otherNames <- otherNames[!(grepl(paste(name,"$",sep=""),otherNames))]
}


#remove names that have been defined as being duplicate names
i <- 1
while (i <= length(otherNames))
{
	if (length(grep(paste(escapedVersion(otherNames[i]),"$",sep=""),duplicateNames)) > 0)
	{
		otherNames <- otherNames[-i]
		i <- i - 1
	} else if (length(grep(paste(escapedVersion(otherNames[i]),"\\-[[:digit:]]{4}\\-[[:digit:]]{2}\\-[[:digit:]]{2}\\-[[:digit:]]{2}\\.[[:digit:]]{2}.*",sep=""),filenames)) == 0 && length(grep(paste(escapedVersion(otherNames[i]),"$",sep=""),duplicateNames)) > 0)
	{
		if (length(grep(paste(escapedVersion(names(duplicateNames)[grep(paste(escapedVersion(otherNames[i]),"$",sep=""),duplicateNames)[1]]),"\\-[[:digit:]]{4}\\-[[:digit:]]{2}\\-[[:digit:]]{2}\\-[[:digit:]]{2}\\.[[:digit:]]{2}.*",sep=""),filenames)) == 0)
		{
			otherNames <- otherNames[-i]
			i <- i - 1
		}
	}
	i <- i + 1
}

i <- 1
while (i <= length(duplicateNames))
{
	matchesInOtherNames <- grep(paste(escapedVersion(names(duplicateNames)[i]),"$",sep=""),otherNames)
	print(matchesInOtherNames)
	if (length(matchesInOtherNames) > 0)
	{
		matches <- grep(paste(escapedVersion(names(duplicateNames)[i]),"$",sep=""),names(duplicateNames))
		filenamefound <- 0
		for (match in matches)
		{
			if (length(grep(paste(escapedVersion(duplicateNames[[match]]),"\\-[[:digit:]]{4}\\-[[:digit:]]{2}\\-[[:digit:]]{2}\\-[[:digit:]]{2}\\.[[:digit:]]{2}.*",sep=""),filenames)) > 0 || length(grep(paste(escapedVersion(names(duplicateNames)[match]),"\\-[[:digit:]]{4}\\-[[:digit:]]{2}\\-[[:digit:]]{2}\\-[[:digit:]]{2}\\.[[:digit:]]{2}.*",sep=""),filenames)) > 0)
			{
				filenamefound <- 1
			}
		}
		if (filenamefound == 0)
		{
			otherNames <- otherNames[-matchesInOtherNames]
		}
	}
	i <- i + 1
}

filesByUniqueName <- vector(mode="list",length=length(otherNames))
names(filesByUniqueName) <- otherNames

tables <- vector(mode="list",length=length(otherNames))
names(tables) <- otherNames

correspondingFiles <- function(name)
{
	files <- grep(name,filenames,fixed=TRUE,value=TRUE)
	namelength <- nchar(name) + 1
	files <- files[which(substr(files,namelength+offset,namelength+offset+2) == "-20")]
	return(files)
}

#for each name, assign all the relevant files to that name
filesRead <- 0
for (name in otherNames)
{
	if (length(duplicateNames[[name]]) > 0)
	{
		for (i in seq_along(duplicateNames))
		{
			if (names(duplicateNames)[i] == name)
			{
				filesByUniqueName[[name]] <- c(filesByUniqueName[[name]],correspondingFiles(duplicateNames[[i]]))
			}
		}
	}
	filesByUniqueName[[name]] <- c(filesByUniqueName[[name]],correspondingFiles(name))
	maxNameLength <- maxLength(c(otherNames,myName))
	for (filename in filesByUniqueName[[name]])
	{
		if (oppressScanMessages == FALSE)
		{
			print(paste("Scanning",filename))
		}
		file <- scan(filename,what="",sep="\n",quiet=oppressScanMessages)
		newtable <- generateTable(myName,myEmail,name,maxNameLength,file)
		if (length(newtable) > 0 && nchar(newtable[1]) > 0)
		{
			tables[[name]] <- c(tables[[name]],newtable)
		}
		filesRead <- filesRead + 1
	}
}

print(paste("Total files read:",filesRead))

for (i in seq_along(tables))
{
	if (i <= length(tables))
	{
		if (length(tables[[i]]) > 1) { tables[[i]] <- cleanUpTable(tables[[i]]) }
	}
}

print((proc.time() - prt))
prt <- proc.time()
