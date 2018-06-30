## Messages Corpus Scripts Using R

This is the original Messages Corpus code that I wrote back in 2014. It involves running a bash script to copy, dedupe, and convert the files, and then an R script to parse them. This code is older and less accurate than the newer Python version, and is only here for historical reasons.

### Setup

Bash script:

- edit the mkdir and cd lines of the script near the beginning; it should be the path you want the converted XML files to be stored
- you can leave the "cd /" line alone, it's just a safety catch
- usage: ./convertichatsv1 /path/to/message/logs
- you need to have the plutil program installed; the script calls it at the end, to convert the XML files

R script:

- edit the lines near the end (starting at line 800) with your name, email, same path to the XML files as you entered in the Bash script, and any duplicate names of contacts (see comments in R script)
