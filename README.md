##Read Me for Messages Corpus Scripts

The original version of the Messages Corpus involved running a bash script and then an R script. I have since ported everything to a single Python script. The bash and R script shouldn't be used, but they are included here for historical reasons.

###Old Setup

Bash script:

- edit the mkdir and cd lines of the script near the beginning; it should be the path you want the converted XML files to be stored
- you can leave the "cd /" line alone, it's just a safety catch
- usage: ./convertichatsv1 /path/to/message/logs
- you need to have the plutil program installed; the script calls it at the end, to convert the XML files

R script:

- edit the lines near the end (starting at line 800) with your name, email, same path to the XML files as you entered in the Bash script, and any duplicate names of contacts (see comments in R script)

###New Setup

- Requirements: Python 3 and packages listed in requirements.txt
- Import the Python script and use the `copy_files`, `parse_files`, or `copy_and_parse_files` functions to parse iMessage logs into Python objects of messages.

See www.fredhope.com/messagescorpus or email fredhope2000@gmail.com for more information.