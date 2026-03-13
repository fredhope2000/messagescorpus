import re
import os
import pandas as pd
import sqlite3
import tabulate
from termcolor import colored

from .shared_utils import MY_DISPLAY_NAME, get_name_groups, get_primary_other_name


"""
Message Corpus Creator!

Copyright 2014 by Fred Hope, in collaboration with Mark Myslin.
Ported to Python from R in 2018

NOTE: Much of this code (all of the string/log file parsing) has been rendered
obsolete now that Messages uses a database instead of text files.
The obsolete code has been moved to legacy_utils.
"""

RAW_MESSAGE_DB_PATH = os.path.join(os.environ['HOME'], 'Library', 'Messages', 'chat.db')
OBJECT_REPLACEMENT_CHAR = "\ufffc"
MEDIA_PLACEHOLDER = "<MEDIA>"

# https://gist.github.com/aaronhoffman/cc7ee127f00b6b5462fa7fc742c23d4f
SQLITE_QUERY = """
select
 m.rowid
,coalesce(m.cache_roomnames, h.id) ThreadId
,m.is_from_me
,case when m.is_from_me = 1 then m.account
 else h.id end as sender
,datetime((m.date / 1000000000) + 978307200, 'unixepoch', 'localtime') as TextDate /* after iOS11 date needs to be / 1000000000 */
,case when m.text is null then '' when m.text = ' ' then '<MEDIA>' else m.text end as MessageText
,m.service
,m.attributedBody
from
message as m
left join handle as h on m.handle_id = h.rowid
left join chat as c on m.cache_roomnames = c.room_name /* note: chat.room_name is not unique, this may cause one-to-many join */
left join chat_handle_join as ch on c.rowid = ch.chat_id
left join handle as h2 on ch.handle_id = h2.rowid

where
-- try to eliminate duplicates due to non-unique message.cache_roomnames/chat.room_name
(h2.service is null or m.service = h2.service)

order by m.date
"""

SQLITE_NAME_QUERY = """
select distinct
 coalesce(m.cache_roomnames, h.id) ThreadId
from
message as m
left join handle as h on m.handle_id = h.rowid
left join chat as c on m.cache_roomnames = c.room_name /* note: chat.room_name is not unique, this may cause one-to-many join */
left join chat_handle_join as ch on c.rowid = ch.chat_id
left join handle as h2 on ch.handle_id = h2.rowid

where
-- try to eliminate duplicates due to non-unique message.cache_roomnames/chat.room_name
(h2.service is null or m.service = h2.service)
and coalesce(m.cache_roomnames, h.id) is not null

order by ThreadId
"""

# Suppress pandas warnings when modifying the dataframes a certain way (it seems to complain even when we use df.loc)
pd.options.mode.chained_assignment = None

PHONE_NAME_RE = re.compile(r"^[\d\+\-\(\)\.\s]+$")
FAKE_CHAT_RE = re.compile("[a-z0-9]{32}")


def is_phone_like(name):
    return bool(name) and "@" not in name and PHONE_NAME_RE.fullmatch(name) is not None


def is_fake_chat(name):
    return bool(name) and FAKE_CHAT_RE.fullmatch(name) is not None


def normalize_message_text(message_text):
    if OBJECT_REPLACEMENT_CHAR in message_text:
        return message_text.replace(OBJECT_REPLACEMENT_CHAR, MEDIA_PLACEHOLDER)
    return message_text


# https://github.com/my-other-github-account/imessage_tools
def parse_message_text_from_sqlite_output_row(row):
    raw_text, attributed_body = row[5], row[7]
    if raw_text != '':
        return raw_text
    if attributed_body is None:
        return ''

    attributed_body = attributed_body.decode('utf-8', errors='replace')
    if 'NSNumber' in str(attributed_body):
        attributed_body = str(attributed_body).split('NSNumber')[0]
        if 'NSString' in attributed_body:
            attributed_body = str(attributed_body).split('NSString')[1]
            if 'NSDictionary' in attributed_body:
                attributed_body = str(attributed_body).split('NSDictionary')[0]
                attributed_body = attributed_body[6:-12]
                return attributed_body

    return attributed_body


def message_dict_from_sqlite(other_name_filter=None):
    name_groups = get_name_groups()
    with sqlite3.connect(RAW_MESSAGE_DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(SQLITE_QUERY)
        output = cursor.fetchall()
        cursor.close()
    print(f"Read {len(output)} messages from database")
    messages = {}
    for row in output:
        other_name = get_primary_other_name(row[1], name_groups=name_groups)
        if other_name_filter is not None and other_name != other_name_filter:
            continue
        messages[other_name] = messages.get(other_name, [])
        message_text = normalize_message_text(parse_message_text_from_sqlite_output_row(row))
        messages[other_name].append({
            'sender': MY_DISPLAY_NAME if row[2] else other_name,
            'timestamp': row[4],
            'message': message_text.strip(),
        })
    return messages


def messages_from_sqlite(other_name_filter=None):
    messages = message_dict_from_sqlite(other_name_filter=other_name_filter)
    if len(messages) > 1:
        raise ValueError(f'Messages could not be returned as a flat list because it contains multiple names: {messages.keys()}')
    return list(messages.values())[0]


def message_names_from_sqlite(include_phone_numbers=False):
    name_groups = get_name_groups()
    with sqlite3.connect(RAW_MESSAGE_DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(SQLITE_NAME_QUERY)
        output = cursor.fetchall()
        cursor.close()
    if include_phone_numbers:
        output = [row for row in output if not is_fake_chat(row[0])]
    else:
        output = [row for row in output if not is_phone_like(row[0]) and not is_fake_chat(row[0])]
    return sorted({get_primary_other_name(row[0], name_groups=name_groups) for row in output})


def color_with_substr_highlight(s, color, substr_range, substr_color):
    """
    Colorizes a string, with a substring of another color.
    Simpler instead of using indices would be s.split(substr) and then substr.join(...) but we actually don't want to highlight all instances
    of substr in case it was a regex group that doesn't actually match. So just highlight the actual match itself.

    :param s: string to be colorized
    :param color: string e.g. 'red', 'green' etc
    :substr_range tuple of substring to colorize differently. e.g (1,4) colorizes 'est' of 'Testing'
    :substr_color: string e.g. 'red', 'green' etc
    """

    idx_start, idx_end = substr_range
    return colored(s[:idx_start], color) + colored(s[idx_start:idx_end], substr_color) + colored(s[idx_end:], color)


def tabulate_df(df, substr_highlights=None, my_color='yellow', other_color='green'):
    """
    Pretty-prints a pandas DataFrame, colorizing the rows.
    If substr_highlights is included, colorize the substrings specified by it .

    :param df: pandas dataframe of a message list
    :substr_highlights: dictionary of {index: (substr_start, substr_end)}
        e.g. {2: (1, 4)} will highlight substring (1, 4) of the message in row index 2
    :my_color: string e.g. 'red', 'green' etc to be used where sender is MY_DISPLAY_NAME
    :other_color: string e.g. 'red', 'green' etc to be used where sender is not MY_DISPLAY_NAME
    """

    if substr_highlights is None:
        substr_highlights = {}
    df = df[['timestamp', 'sender', 'message']]
    for column in ['timestamp', 'message', 'sender']:  # Have to do sender last because we are also checking its original value
        if column == 'message':  # highlight the matched text a different color
            df[column] = df.apply(lambda row: color_with_substr_highlight(row[column], my_color if row.sender == MY_DISPLAY_NAME else other_color, substr_highlights.get(row.name, (0, 0)), 'red'), axis=1)
        else:
            df[column] = df.apply(lambda row: colored(row[column], my_color) if row.sender == MY_DISPLAY_NAME else colored(row[column], other_color), axis=1)
    return tabulate.tabulate(df, showindex=True, headers=df.columns)


def tabulate_messages(message_list, start_index=0):
    """
    Pretty-prints a list of messages by converting it to a pandas DataFrame.

    :param message_list: list of message objects
    :param start_index: optional index to start at, so the DataFrame indices show the original message indices instead of starting at 0
    """

    df = pd.DataFrame(message_list)
    if start_index:
        df.index = range(start_index, start_index + len(message_list))
    print(tabulate_df(df))


def search_corpus(message_obj, query, ignore_case=True, regex=False, regex_group=None, context=0, max_results=20, most_recent=True):
    """
    Searches a collection of messages for a substring or regex patter and returns the matching DataFrames and metadata.

    :param message_obj: one of the following:
        - dictionary of name:messages. E.g. the `messages` object that is returned by parse_files()
        - list of messages. E.g. if `messages` was returned by parse_files(), this can be messages['Dan']
    :param query: string or regex pattern to search
    :param ignore_case: boolean whether to search case-insensitive
    :param regex: use regex search (otherwise just substring search)
    :param regex_group: group number of regex pattern to return (otherwise return full match)
    :param context: number of rows on either side of matched row to display as well
    """

    regex_group = [regex_group] if regex_group else []
    regex_flags = re.IGNORECASE if ignore_case else 0
    def _search(query, message):
        if regex:
            match = re.search(query, message, flags=regex_flags)
            return match.span(*regex_group) if match else None
        else:
            match = message.lower().find(query.lower()) if ignore_case else message.find(query)
            return (match, match + len(query)) if match != -1 else None

    num_matches = 0
    matches = {}
    dfs = {}
    if isinstance(message_obj, list):
        message_list = message_obj
        matches[None] = []
        ordered_list = list(reversed(message_list)) if most_recent else message_list
        for idx, m in enumerate(ordered_list):
            match = _search(query, m['message'])
            if match:
                matches[None].append((idx, match))
                num_matches += 1
                if num_matches == max_results:
                    break
        if not matches[None]:
            return
        dfs[None] = pd.DataFrame(ordered_list)
    elif isinstance(message_obj, dict):
        for name, message_list in message_obj.items():
            if not message_list:
                continue
            matches[name] = []
            ordered_list = list(reversed(message_list)) if most_recent else message_list
            for idx, m in enumerate(ordered_list):
                match = _search(query, m['message'])
                if match:
                    matches[name].append((idx, match))
                    num_matches += 1
                    if num_matches == max_results:
                        break
            if not matches[name]:
                continue
            dfs[name] = pd.DataFrame(ordered_list)
            if num_matches == max_results:
                break
    else:
        raise TypeError(f"message_obj was {type(message_obj)} which is not recognized")

    return {
        'dfs': dfs,
        'matches': matches,
        'num_matches': num_matches,
    }


def print_from_corpus(message_obj, query, ignore_case=True, regex=False, regex_group=None, context=0, max_results=20, most_recent=True):
    """
    Searches a collection of messages and prints the results as tabulated DataFrames.
    """

    search_results = search_corpus(message_obj, query, ignore_case=ignore_case, regex=regex, regex_group=regex_group, context=context, max_results=max_results, most_recent=most_recent)
    if search_results is None:
        return

    dfs = search_results['dfs']
    matches = search_results['matches']
    num_matches = search_results['num_matches']
    for name, df in dfs.items():
        context_offset = context
        if most_recent:
            # So that each conversation snippet is still ordered naturally
            df = df.iloc[::-1]
            context_offset = -context
        if name is not None:
            print(f"*** MATCHES FOR {name} ***")
        for message_idx, substr_range in matches[name]:
            sub_df = df.loc[(message_idx-context_offset):(message_idx+context_offset), :]
            print(tabulate_df(sub_df, substr_highlights={message_idx: substr_range}))

    if num_matches == max_results:
        print(f"*** NOTE: Maximum of {num_matches} was reached. ***")
