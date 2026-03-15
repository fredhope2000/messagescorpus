## Messages Corpus Scripts

`messagescorpus` reads and parses Messages logs on macOS to create a searchable dataset of your Messages history.

The current Python module reads directly from the local macOS Messages SQLite database (`~/Library/Messages/chat.db`) and returns thread-based message objects that can be searched or inspected in Python. A local Flask web app is also included for browsing and searching conversations in the browser.

### Setup

- Requirements: Python 3.6 and packages listed in requirements.txt
- Create a `name_groups.json` in the base repo directory and populate it according to the details in `get_name_groups()` if you want to merge multiple identifiers for the same person.
- Update `MY_DISPLAY_NAME` in `messagescorpus/shared_utils.py` so your own sent messages are labeled correctly.

### SQLite Usage

```python
from messagescorpus.corpus import message_dict_from_sqlite, messages_from_sqlite, search_corpus
```

Read a single conversation thread as a flat list:

```python
messages = messages_from_sqlite(other_name_filter='Dan')
```

Read all conversation threads as a dictionary keyed by thread name:

```python
messages = message_dict_from_sqlite()
```

Each message has the same downstream structure as before:

```python
{
    'sender': 'Fred',
    'timestamp': '2024-02-03 18:42:10',
    'message': 'See you soon',
}
```

Thread names are now conversation-level keys rather than just person-level keys:

- 1:1 chats still use the canonicalized other-person name
- group chats are named in Python from canonicalized participant names
- multiple raw thread ids that canonicalize to the same thread name are merged together

Per-message senders are derived from the SQL query's sender column, so group chats preserve who actually sent each message.

Media (photo or video attachments) appears as `<MEDIA>`.

### Usage / Examples

```
from messagescorpus.corpus import message_dict_from_sqlite, messages_from_sqlite, search_corpus

# Read all conversations from the Messages SQLite database
messages = message_dict_from_sqlite()

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

### Web App

Run the local browser app:

```bash
pip install -r requirements.txt
python3 webapp/app.py
```

Then open:

```text
http://127.0.0.1:5000/
```

Current web app features include:

- sidebar conversation browser with client-side name filtering
- read-only thread browsing
- scoped search within the selected conversation
- regex, context, and max-results controls
- in-process caching of thread data and thread names
- refresh controls to reload data from SQLite
- incremental "load older" and "load more context" browsing controls

### Caveats

- The script can only read what is stored locally on your Mac, so if you sent messages that were only downloaded by another device, or are only stored in iCloud, this script will not find them.
- `messages_from_sqlite()` only works when the query resolves to a single thread, which usually means supplying `other_name_filter`.
- Group-chat naming is currently inferred from chat membership rows in the database. It is much better than the old behavior, but there may still be edge cases in how Apple stores participants or thread ids.
- The `attributedBody` fallback is still heuristic when plain text is missing, so some unusual message types may not decode perfectly.
- The web app conversation list excludes chats without any known contacts (e.g., arbitrary phone numbers), for brevity.

### Legacy

The original version of Messages Corpus involved copying and parsing text logs, plus an older bash/R workflow in the `R/` folder. That flow is preserved for historical reference, but it should not be used for new work.

Legacy examples and docs may still refer to:

- `copy_files`
- `parse_files`
- `copy_and_parse_files`
- editing constants in the older parser implementation

See www.fredhope.com/messagescorpus for info about the R script, or email fredhope2000@gmail.com with any questions.
