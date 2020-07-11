## Messages Corpus Scripts

`messagescorpus` reads and parses Messages logs on macOS to create a searchable dataset of your Messages history.

The original version of the Messages Corpus involved running a bash script and then an R script (see "R" folder). I have since ported everything to a single Python script. The bash and R script shouldn't be used, but they are included here for historical reasons.

### Setup

- Requirements: Python 3.6 and packages listed in requirements.txt
- Edit the constants at the top of the Python file to match your information (name, email used with iMessage, etc)
- Import the Python script and use the `copy_files`, `parse_files`, or `copy_and_parse_files` functions to parse iMessage logs into Python objects of messages.
- (Optional) to combine contacts together, create a `name_groups.json` in the base repo directory and populate it according to the details in `get_name_groups()`

### Usage / Examples

```
import messages_corpus as mc
messages = mc.copy_and_parse_files(years=['2017', '2018'])

messages['Dan'][0:5]  # The first 5 messages between you and Dan

len(messages)  # Number of distinct people you've conversed with

sum([len(v) for v in messages.values()])  # Total number of messages you've sent
```

### Caveats

- Multiway chats are not supported.
- The script can only read what is stored locally on your Mac, so if you sent messages that were only downloaded by another device, or are only stored in iCloud, this script will not find them.

See www.fredhope.com/messagescorpus for info about the R script, or email fredhope2000@gmail.com with any questions.