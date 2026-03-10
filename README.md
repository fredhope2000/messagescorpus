## Messages Corpus Scripts

`messagescorpus` reads and parses Messages logs on macOS to create a searchable dataset of your Messages history.

The current Python module reads directly from the local macOS Messages SQLite database (`~/Library/Messages/chat.db`) and returns message objects that can be searched or inspected in Python.

### Setup

- Requirements: Python 3.6 and packages listed in requirements.txt
- Create a `name_groups.json` in the base repo directory and populate it according to the details in `get_name_groups()` if you want to merge multiple identifiers for the same person.
- Update `MY_DISPLAY_NAME` in `messagescorpus/shared_utils.py` so your own sent messages are labeled correctly.

### SQLite Usage

```python
from messagescorpus.corpus import messages_from_sqlite, search_corpus
```

Read a single conversation as a flat list:

```python
messages = messages_from_sqlite(other_name_filter='Dan')
```

Read all conversations as a dictionary keyed by contact name:

```python
messages = messages_from_sqlite(return_as_list=False)
```

Each message has the same downstream structure as before:

```python
{
    'sender': 'Fred',
    'timestamp': '2024-02-03 18:42:10',
    'message': 'See you soon',
}
```

### Usage / Examples

```
from messagescorpus.corpus import messages_from_sqlite, search_corpus

# Read all conversations from the Messages SQLite database
messages = messages_from_sqlite(return_as_list=False)

# The first 5 messages between you and Dan
messages['Dan'][0:5]

# Number of distinct people you've conversed with
len(messages)

# Total number of messages you've sent
sum([len(v) for v in messages.values()])

# Search the logs for a particular phrase
search_corpus(messages['Dan'], 'world series', context=5)

# Read just one conversation as a list
messages = messages_from_sqlite(other_name_filter='Dan')
```

### Caveats

- Multiway chats are not supported.
- The script can only read what is stored locally on your Mac, so if you sent messages that were only downloaded by another device, or are only stored in iCloud, this script will not find them.
- `messages_from_sqlite(return_as_list=True)` only works when the query resolves to a single contact, which usually means supplying `other_name_filter`.

### Legacy

The original version of Messages Corpus involved copying and parsing text logs, plus an older bash/R workflow in the `R/` folder. That flow is preserved for historical reference, but it should not be used for new work.

Legacy examples and docs may still refer to:

- `copy_files`
- `parse_files`
- `copy_and_parse_files`
- editing constants in the older parser implementation

See www.fredhope.com/messagescorpus for info about the R script, or email fredhope2000@gmail.com with any questions.
