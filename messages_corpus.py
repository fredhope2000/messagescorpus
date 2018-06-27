import datetime
import json
import re
import os
import time


"""
Message Corpus Creator!

Copyright 2014 by Fred Hope, in collaboration with Mark Myslin.
Ported to Python from R in 2018
"""


FILE_SUFFIX = '.ichat'
RAW_MESSAGE_LOG_DIR = '/Users/fred/Library/Messages/Archive/'
COPIED_MESSAGE_LOG_DIR = '/Users/fred/messages'
DUPLICATE_FILE_PATTERN = r'\-[0-9]+$'
ATTACHMENT_UUID_PATTERN = '<string>[A-Z0-9]{8}-[A-Z0-9]{4}-[A-Z0-9]{4}-[A-Z0-9]{4}-[A-Z0-9]{12}</string>'
PHONE_NUMBER_PATTERN = r'<string>\+?[0-9]{10,11}</string>'
MY_EMAIL = 'fredhope2000@gmail.com'
MY_CONTACT_INFO_IDS = ['e:', f'e:{MY_EMAIL}', MY_EMAIL]
MY_NAME = 'Fred Hope'
MY_SHORT_NAME = 'Fred'

"""
notes for convertichats:
exclude "Chat with " in filename (multiway chats)
f.replace('.ichat', '.icht').replace('.icht', '') or re.sub(r'\.icha?t$', '', f)
duplicates (ends with -1, -2 etc) are always in the same directory as each other
keep the duplicate with the highest number, eg if foo, foo-1, and foo-2, keep only foo-2 and rename to foo
"""




def dedupe_filenames(filenames):
    """
    Duplicate files look like foo.ichat, foo-1.ichat, foo-2.ichat, etc. The highest number is the most recent/complete file.
    So return just the highest-numbered file in each fileset
    """

    filenames = sorted([f.replace(FILE_SUFFIX, '') for f in filenames])

    # Keep list and set separate since set ordering is unclear
    filenames_set = set(filenames)

    # Pairing the filenames up with their stripped counterpart is more efficient than searching through the list various times
    filename_pairs = [(f, re.sub(DUPLICATE_FILE_PATTERN, '', f)) for f in filenames]

    # Make sure there aren't duplicate-looking files without a base filename also present, as this would be unexpected
    duplicate_base_filenames = set([f[1] for f in filename_pairs if f[0] != f[1]])
    assert duplicate_base_filenames <= filenames_set, "Found duplicate filenames whose base doesn't appear"

    filename_mapping = {f[1]: [] for f in filename_pairs}
    for filename, base_filename in filename_pairs:
        filename_mapping[base_filename].append(filename)

    deduped_files = []
    for base_filename, raw_filenames in filename_mapping.items():
        deduped_files.append(raw_filenames[-1] + FILE_SUFFIX)
    print(f"Removed {len(filenames) - len(deduped_files)} duplicates.")

    return deduped_files


def copy_files():
    """
    Grabs the filenames from the raw message archive, dedupes them, copies them to a new location, and decrypts them
    """

    filenames = []
    for root, _, files in os.walk(RAW_MESSAGE_LOG_DIR):
        # Exclude .DS_Store and 'Chat with' which is multiway chats
        filenames += [os.path.join(root, f) for f in files if not f.startswith('.') and not f.startswith('Chat with')]
    print(f"Found {len(filenames)} files")

    # The old version of this code looked for .icht as well, but there don't seem to be any files like that anymore.
    if not all([f.endswith(FILE_SUFFIX) for f in filenames]):
        raise Exception(f"Unexpected files found without {FILE_SUFFIX} suffix")

    deduped_filenames = dedupe_filenames(filenames)
    return filenames, deduped_filenames


def get_filenames():
    return [f for f in os.listdir(COPIED_MESSAGE_LOG_DIR) if f.endswith('.icht')]


def index_or_none(l, item, *args):
    """
    Wrapper for list.index() that returns None instead of raising an error.
    """

    try:
        idx = l.index(item, *args)
    except ValueError:
        idx = None
    return idx


def first_regex_match(l, pat):
    """
    Returns the first regex match of `pat` in a list, or None if no matches.
    """

    matches = [i for i in l if re.search(pat, i)]
    return matches[0] if matches else None


def first_substr_match(l, substr):
    """
    Returns the first item containing `substr` in a list, or None if no matches.
    """

    matches = [i for i in l if substr in i]
    return matches[0] if matches else None


def compare_timestamps(line1, line2):
    """
    Inputs two lines, returns the number of seconds elapsed between the first line and the second line
    """

    return (datetime.strptime(line2) - datetime.strptime(line1)).seconds


def integer_to_datetime(i):
    """
    Converts an integer representing seconds since 2001-01-01 to a datetime.
    These integers are how the messages are timestamped.
    """

    return datetime.datetime(2001, 1, 1) + datetime.timedelta(seconds=i)


def strip_tags(s):
    """
    Strips the XML tag on both ends of a string, including the brackets and closing slash
    """

    tag = re.match(r'<(\w+)>', s).group(1)
    s = s.replace(f'<{tag}>', '').replace(f'</{tag}>', '')
    return s


def is_contact_info_line(line):
    return re.match(PHONE_NUMBER_PATTERN, line) or strip_tags(line) in MY_CONTACT_INFO_IDS


def datetime_from_cocoa_time(ts):
    """
    Cocoa timestamps are a number of seconds since 2001-01-01 midnight GMT
    """

    return datetime.datetime(2001, 1, 1) + datetime.timedelta(seconds=ts)


def generate_sender_id_mapping(sender_ids, conversation_started_by, other_name):
    if len(conversation_started_by) == 1:
        # Just one thread (iMessage or SMS but not both). Lower sender id is the person who started the thread
        person_order = (MY_SHORT_NAME, other_name) if conversation_started_by[0] in MY_CONTACT_INFO_IDS else (other_name, MY_SHORT_NAME)
        if len(sender_ids) == 1:  # Just one person total
            return {sender_ids[0]: person_order[0]}
        elif len(sender_ids) == 2:  # Both people
            return {sender_ids[0]: person_order[0], sender_ids[1]: person_order[1]}
        else:
            raise Exception(f"sender_ids {sender_ids} found with conversation_started_by {conversation_started_by}, expecting between 1 and 2 sender ids")
    elif len(conversation_started_by) == 2:
        # Two threads (SMS and iMessage)
        person_order = [(MY_SHORT_NAME, other_name) if c in MY_CONTACT_INFO_IDS else (other_name, MY_SHORT_NAME) for c in conversation_started_by]
        if len(sender_ids) == 1:  # One person, not sure why the second thread starts
            print(f"Warning: sender_ids {sender_ids} found with conversation_started_by {conversation_started_by}, expecting between 2 and 4 sender ids")
            return {sender_ids[0]: person_order[0][0]}
        elif len(sender_ids) == 2:  # One person per thread
            return {sender_ids[0]: person_order[0][0], sender_ids[1]: person_order[1][0]}
        elif len(sender_ids) == 3:  # Two people on one thread and one on another thread
            assert not any([15 <= s <= 29 for s in sender_ids]), f"sender_ids {sender_ids}, not sure how to handle"
            if sender_ids[1] <= 14:  # First thread is both people, second thread is just one
                return {sender_ids[0]: person_order[0][0], sender_ids[1]: person_order[0][1], sender_ids[2]: person_order[1][0]}
            else:  # First thread is one person, second thread is both people
                return {sender_ids[0]: person_order[0][0], sender_ids[1]: person_order[1][0], sender_ids[2]: person_order[1][1]}
        elif len(sender_ids) == 4:  # Two people on each thread
            return {sender_ids[0]: person_order[0][0], sender_ids[1]: person_order[0][1], sender_ids[2]: person_order[1][0], sender_ids[3]: person_order[1][1]}
        else:
            raise Exception(f"sender_ids {sender_ids} found with conversation_started_by {conversation_started_by}, expecting between 2 and 4 sender ids")
    else:
        raise Exception(f"conversation_started_by {conversation_started_by} found, expecting 1 or 2")


def unescape_xml_chars(s):
    """
    Converts the escaped &lt;, etc back to the characters they represent
    """

    escape_chars = [
        ('&lt;', '<'),
        ('&gt;', '>'),
        ('&quot;', '"'),
        ('&apos;', "'"),
        ('&at;', '@'),  # not a real XML escape, but we use this to "protect" messages that are just my email
        ('&amp;', '&'),
        ('\xa0', ''),
    ]

    s = re.sub('(\\w)\xa0(\\w)', r'\1 \2', s)
    for escaped, real in escape_chars:
        s = s.replace(escaped, real)
    s = s.replace('\xa0', '')
    return s


def parse_file(filename, other_name='Dan'):
    with open('/Users/fred/messagescorpus/' + filename, 'r') as f:
        lines = f.read().splitlines()

    # Remove lines containing blocks of 52 alphanumeric chars, this represents data e.g. attachments.
    # We don't actually have to catch all of them, this is just an initial stripdown for performance.
    lines = [l for l in lines if not re.match('\t\t\t[A-Za-z0-9/+]{52}', l)]

    # Remove tab padding
    lines = [re.sub('^\t+<', '<', l) for l in lines]

    # Remove any blank lines
    lines = [l for l in lines if l]

    # Each file should contain a E: <string> value (sometimes E:myEmail or similar)
    # Make sure it's what we want by checking the previous line for a UUID.
    # The next <string> value after this is the person who started the conversation.
    # (So either MY_EMAIL or the other person's email/phone, or sometimes blank)
    i = 1
    conversation_started_by = []
    while True:
        if i >= len(lines):
            break
        if lines[i].startswith('<string>E:') and re.match(ATTACHMENT_UUID_PATTERN, lines[i-1]):
            # Skip the SMS line if there is one, this isn't helpful
            conversation_search_idx = i+2 if lines[i+1] == '<string>SMS</string>' else i+1
            conversation_started_by.append(strip_tags(first_substr_match(lines[conversation_search_idx:], '<string>')))
        i += 1

    # If a message has a newline in it, this ends up on the next line of the file, without any XML prefix.
    # Concat them together by pasting the second line onto the first and removing the second line.
    # Repeat this until there aren't anymore "runover lines".

    while True:
        runover_lines = [idx for idx in range(1, len(lines)-1) if lines[idx].startswith('<string>') and not lines[idx].endswith('</string>')]

        if not runover_lines:
            break

        for idx in runover_lines:
            assert lines[idx+1] == '</string>' or not lines[idx+1].startswith('<')
            lines[idx] = lines[idx] + '\n' + lines[idx+1]

        lines = [l for idx, l in enumerate(lines) if idx-1 not in runover_lines]

    # Throw out irrelevant lines
    relevant_strings = ['<key>NS.time', '<key>NS.string', '<key>Sender</key>', '<string>', '<real>', '<integer>']
    lines = [l for l in lines if any([s in l for s in relevant_strings])]

    assert all([l.startswith('<') and l.endswith('>') for l in lines])

    # We usually throw out instances of <string>MY_EMAIL</string> because they are fake, but it's possible this was an actual message.
    # If this happens, there should be a <string>mailto:MY_EMAIL</string> just after it.
    i = 0
    while True:
        if i >= len(lines) - 1:
            break
        if MY_EMAIL in lines[i] and first_substr_match(lines[(i+1):], MY_EMAIL) == f'<string>mailto:{MY_EMAIL}</string>':
            print("Found my email but it appears to be an actual message. Preserving.")
            lines[i] = lines[i].replace('@', '&at;')
        i += 1

    # The key pieces of information are message, timestamp, and sender. These are represented by (for example, for timestamps) a <key>NS.time</key> followed by a <real>123456789</real>.
    # So keep <string>.* and <real>.* and <integer>.* lines only if they follow NS.string, NS.time, or Sender lines respectively
    i = 0
    while True:
        if i >= len(lines):
            break
        if i == 0 and lines[i].startswith('<string>'):  # the first line remaining can't be useful if it's a <string>, since <key> doesn't precede it
            lines.pop(i)
            continue
        if (lines[i].startswith('<string>') and not lines[i-1].startswith('<key>NS.string')) or (lines[i].startswith('<real>') and not lines[i-1].startswith('<key>NS.time')) or (lines[i].startswith('<integer>') and not lines[i-1].startswith('<key>Sender')):
            lines.pop(i)
            continue
        if lines[i] == '<string></string>':  # blank string
            lines.pop(i)
            continue
        i += 1

    # Now that we know which <string>, <real>, and <integer> lines to keep, we can get rid of the <key> lines
    lines = [l for l in lines if not l.startswith('<key>')]

    # Attachments are represented by a UUID and/or an object-replacement character, depending on the version of Messages.
    # Replace them with (MEDIA)
    lines = [re.sub(ATTACHMENT_UUID_PATTERN, '<string>(MEDIA)</string>', l) for l in lines]
    lines = [re.sub(chr(65532), '(MEDIA)', l) for l in lines]

    # If a (MEDIA) message immediately follows a regular message (they were sent together), concatenate them
    i = 0
    while True:
        if i >= len(lines) - 1:
            break
        if lines[i].startswith('<string>') and lines[i+1] == '<string>(MEDIA)</string>':
            lines[i] = lines[i].replace('</string>', ' (MEDIA)</string>')
            lines.pop(i+1)
            continue
        i += 1

    # A sender code of 0 usually indicates a "iMessage with x" message that isn't a real message; delete these
    i = 0
    while True:
        if i >= len(lines) - 2:
            break
        if lines[i] == '<integer>0</integer>' and lines[i+1].startswith('<real>') and 'iMessage with' in lines[i+2]:
            latest_timestamp = lines[i+1]  # used in a later step; sometimes we need this initial timestamp as the first "real" message doesn't have one
            lines.pop(i)
            lines.pop(i)
            lines.pop(i)
            continue
        i += 1

    # The first message may have one or more extra <string> lines containing someone's email or phone number; remove them, repeatedly
    i = 1
    while True:
        if i >= len(lines):
            break
        if is_contact_info_line(lines[1]):
            lines.pop(1)
        else:
            break

    # If there are timestamps exactly two lines apart, it's because a blank message got deleted, so delete the extra timestamp
    i = 0
    while True:
        if i >= len(lines) - 1:
            break
        if lines[i].startswith('<integer>') and lines[i+1].startswith('<integer>'):
            lines.pop(i)
            continue
        if i < len(lines) - 3 and lines[i+1].startswith('<real>') and lines[i+2].startswith('<integer>') and lines[i+3].startswith('<real>'):
            lines.pop(i)
            lines.pop(i)
            continue
        i += 1

    # When a message doesn't have a timestamp, give it one equal to the previous timestamp before that, with an asterisk to indicate
    # that it was an inferred timestamp. But first check if <string> is a contact info string, then just delete it
    i = 0
    while True:
        if i >= len(lines) - 2:
            break
        if lines[i].startswith('<real>'):
            latest_timestamp = lines[i]
        if lines[i].startswith('<integer>') and not lines[i+1].startswith('<real>'):
            if is_contact_info_line(lines[i+1]):
                lines.pop(i+1)
                continue
            else:
                lines = lines[:(i+1)] + [re.sub(r'\*?<\/real>', '*</real>', latest_timestamp)] + lines[(i+1):]
        i += 1

    # Remove the last line, if it's the other person's phone/email (it often is), repeatedly
    while not lines[-3].startswith('<integer>') and not lines[-2].startswith('<integer>') and lines[-1].startswith('<string>') and is_contact_info_line(lines[-1]):
        lines.pop(-1)

    # If the last line is now a timestamp, which it sometimes is, remove it, as it is meaningless, repeatedly
    while lines[-1].startswith('<real>'):
        lines.pop(-1)

    i = 0
    while True:
        if i >= len(lines) - 3:
            break
        if lines[i].startswith('<integer>') and lines[i+1].startswith('<string>'):
            remaining_lines = [l for l in lines[i+2:] if not l.startswith('<string>')]
            if remaining_lines and remaining_lines[0].startswith('<real>'):
                if is_contact_info_line(lines[i+1]):
                    lines.pop(i+1)
                    continue
                else:
                    raise Exception(f"""Line {i+1}, "{lines[i+1]}", is between an <integer> and a <real> but isn't recognized as contact info""")
        i += 1

    # Some phone number or E: strings still remain, lumped in with legitimate messages; remove them
    i = 0
    while True:
        if i >= len(lines) - 1:
            break
        if lines[i].startswith('<string>') and lines[i+1].startswith('<string>'):
            # Not sure if we should include the MY_NAME check in is_contact_info_line, I've only observed it once, at the end of a file
            if is_contact_info_line(lines[i]) or lines[i] == f'<string>{MY_NAME}</string>':
                lines.pop(i)
            elif is_contact_info_line(lines[i+1]) or lines[i+1] == f'<string>{MY_NAME}</string>':
                lines.pop(i+1)
            else:
                raise Exception(f"""Lines {i} and {i+1}, "{lines[i]}" and "{lines[i+1]}", both start with <string> but aren't recognized as contact info""")
            continue
        i += 1

    sender_ids = sorted(list(set([int(strip_tags(l)) for l in lines if l.startswith('<integer>')])))

    # Most files have 2 distinct sender ids. The lower id is the person who started the conversation.
    # However, some files have multiple conversation threads, due to a switch between SMS and iMessage.
    # Then each thread has up to 2 distinct sender ids, for a total of 4.
    if lines:
        sender_id_mapping = generate_sender_id_mapping(sender_ids, conversation_started_by, other_name)

    assert len(lines) % 3 == 0, f"Result has {len(lines)} line(s), not a multiple of 3"

    # At this point, the lines in the file should go: sender id, timestamp, message, sender id, timestamp, message, etc
    # Take every three lines and concatenate them into one vector element
    messages = []
    meta = {'sender_id_mapping': sender_id_mapping, 'filename': filename}
    for i in range(int(len(lines)/3)):
        sender_idx = i * 3
        timestamp_idx = i * 3 + 1
        message_idx = i * 3 + 2
        assert lines[sender_idx].startswith('<integer>'), f"""Line {sender_idx} is "{lines[sender_idx]}", was expecting an <integer> line"""
        assert lines[timestamp_idx].startswith('<real>'), f"""Line {timestamp_idx} is "{lines[timestamp_idx]}", was expecting a <real> line"""
        assert lines[message_idx].startswith('<string>'), f"""Line {message_idx} is "{lines[message_idx]}", was expecting a <string> line"""
        sender_id = int(strip_tags(lines[sender_idx]))
        sender = sender_id_mapping[sender_id]
        timestamp_str = strip_tags(lines[timestamp_idx])
        timestamp = datetime_from_cocoa_time(float(timestamp_str.replace('*', '')))
        message = unescape_xml_chars(strip_tags(lines[message_idx]))
        messages.append({
            'sender_id': sender_id,
            'sender': sender,
            'timestamp': timestamp,
            'is_timestamp_inferred': '*' in timestamp_str,
            'message': message,
            'meta': meta,
            })
    assert len(messages) == len(lines) / 3, f"{len(lines)} lines became {len(messages)} messages"

    return sorted(messages, key=lambda k: k['timestamp'])


def parse_all_files(other_name='Dan', quiet=True):
    filenames = get_filenames()
    messages = []
    for f in filenames:
        if not quiet:
            print(f"Parsing {f}")
        messages += parse_file(f, other_name)
    return sorted(messages, key=lambda k: k['timestamp'])


"""
notes:
conversation_started_by:
treats SMS and iMessage as separate threads
emails/phone numbers are iMessage, "SMS" is SMS by either person
for SMS, look at the next string (usually after an NS.string but once I saw it as e: immediately next line) after the SMS string

sender ids:
within each thread (iMessage vs SMS), lower sender id is the thread starter
if I start a message with iMessage (eg sender id 6) and then DB replies with SMS, his sender id is higher (eg 57) because that's a different thread, if he later has an iMessage then the iMessage is a lower sender id (eg 11)
whichever medium is first (iMessage or SMS) has lower sender ids, eg I start with SMS, my sender id is 6, DB replies with iMessage, his sender id is 46, then I reply with iMessage, mine is 50
"""