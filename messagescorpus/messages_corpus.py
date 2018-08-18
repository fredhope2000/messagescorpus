import datetime
import json
import re
import os
import pandas as pd
import subprocess
import tabulate
import time
import tqdm
from concurrent.futures import ProcessPoolExecutor
from termcolor import colored


"""
Message Corpus Creator!

Copyright 2014 by Fred Hope, in collaboration with Mark Myslin.
Ported to Python from R in 2018
"""


# Default year to start looking for messages in the copy functions, if no year is specified
START_YEAR = 2012

BASE_REPO_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Where the copied/decrypted files should go. The default, "data" in the repo's base folder, is git-ignored
COPIED_MESSAGE_LOG_DIR = os.path.join(BASE_REPO_DIR, 'data')

# Modify these based on your iCloud/iMessage information
MY_EMAIL = 'fredhope2000@gmail.com'
MY_NAME = 'Fred Hope'

MY_CONTACT_INFO_IDS = ['e:', f'e:{MY_EMAIL}', MY_EMAIL]

# How your messages will appear in the parsed logs
MY_DISPLAY_NAME = 'Fred'

# Include any non-standard phone numbers here that show up in your logs
OTHER_FORMATTED_NUMBERS = ['+90 (008) 000 40 07', '+1 900080005330']
OTHER_RAW_NUMBERS = ['900080004007', '1900080005330']

# Debug mode will include more info with each message, like the filename and sender id logic
DEBUG_MODE = False

# Shouldn't need to modify any of these, unless you have stuff in your logs I haven't accounted for

FILE_SUFFIX = '.ichat'
CURRENT_YEAR = datetime.datetime.utcnow().year
RAW_MESSAGE_LOG_DIR = os.path.join(os.environ['HOME'], 'Library', 'Messages', 'Archive')
DUPLICATE_FILE_PATTERN = re.compile(r'\-[0-9]+$')
OTHER_NAME_PATTERN = re.compile('[0-9]{4}-[0-9]{2}-[0-9]{2}_\u202a?([^\u202c]+)\u202c? on [0-9]{4}-[0-9]{2}-[0-9]{2} at ')
TAB_PADDING_PATTERN = re.compile('^\t+<')
ATTACHMENT_UUID_PATTERN = re.compile('<string>[A-Z0-9]{8}-[A-Z0-9]{4}-[A-Z0-9]{4}-[A-Z0-9]{4}-[A-Z0-9]{12}</string>')
DATA_PATTERN = re.compile('\t\t\t[A-Za-z0-9/+]{52}')
NBSP_PATTERN = re.compile('(\\w)\xa0(\\w)')
AUTOMATED_SENDER_PATTERN = re.compile(r'^\d{5,6}$|^\d{12}$')
PHONE_NUMBER_PATTERN = re.compile(r'<string>(\+?[0-9]{11})</string>')
BARE_PHONE_NUMBER_PATTERN = re.compile(r'<string>[0-9]{10}</string>')
EMAIL_PATTERN = re.compile(r'^[^ @]+@[^ @.]+\.[^ @]+$')
INFERRED_TIMESTAMP_PATTERN = re.compile(r'\*?<\/real>')
TAG_PATTERN = re.compile(r'<(\w+)>')

# Suppress pandas warnings when modifying the dataframes a certain way (it seems to complain even when we use df.loc)
pd.options.mode.chained_assignment = None


"""Functions for copying and decrypting the files"""


def decrypt_file(filename):
    """
    Decrypts the given file and places it in COPIED_MESSAGE_LOG_DIR with a unique filename
    """

    dirname = os.path.basename(os.path.dirname(filename))  # eg 2018-01-04
    base_output_filename = dirname + '_' + os.path.basename(filename)
    output_filename = os.path.join(COPIED_MESSAGE_LOG_DIR, base_output_filename)
    if not os.path.isdir(COPIED_MESSAGE_LOG_DIR):
        os.mkdir(COPIED_MESSAGE_LOG_DIR)
    subprocess.check_call(['plutil', '-convert', 'xml1', filename, '-o', output_filename])
    return base_output_filename


def dedupe_filenames(filenames):
    """
    Duplicate files look like foo.ichat, foo-1.ichat, foo-2.ichat, etc. Prior to 7/25/2014, the highest number is the most recent/complete file,
    so return just the highest-numbered file in each fileset. After 7/25/2014, the files are distinct (and just happen to have the same name),
    so return all the files in the fileset.
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
        if re.search(' on ([0-9]{4}-[0-9]{2}-[0-9]{2}) at ', base_filename).group(1) >= '2014-07-25':
            for f in raw_filenames:
                deduped_files.append(f + FILE_SUFFIX)
        else:
            deduped_files.append(raw_filenames[-1] + FILE_SUFFIX)
    print(f"Removed {len(filenames) - len(deduped_files)} duplicates.")

    return deduped_files


def copy_files(years=None, return_filenames=False):
    """
    Grabs the filenames from the raw message archive, dedupes them, copies them to a new location, and decrypts them.
    The decrypting is kind of slow, so we can just do files for a certain year and keep the rest.
    """

    now = time.time()
    if years is None:
        years = [str(year) for year in range(START_YEAR, CURRENT_YEAR + 1)]
    print(f"Copying files for year(s) {years}")
    year_strs = [f' on {year}-' for year in years]  # eg "Fred Hope on 2018-01-01 at 16.17.18"
    filenames = []
    for root, _, files in os.walk(RAW_MESSAGE_LOG_DIR):
        # Exclude .DS_Store and 'Chat with' which is multiway chats
        filenames += [os.path.join(root, f) for f in files if not f.startswith('.') and not f.startswith('Chat with') and any([year_str in f for year_str in year_strs])]
    print(f"Found {len(filenames)} files")

    # The old version of this code looked for .icht as well, but there don't seem to be any files like that anymore.
    if not all([f.endswith(FILE_SUFFIX) for f in filenames]):
        raise Exception(f"Unexpected files found without {FILE_SUFFIX} suffix")

    deduped_filenames = dedupe_filenames(filenames)

    output_files = []
    print("Decrypting...")
    with ProcessPoolExecutor() as executor:
        for output_file in list(tqdm.tqdm(executor.map(decrypt_file, deduped_filenames, chunksize=3), total=len(deduped_filenames))):
            output_files.append(output_file)
    print("\nDecrypted {files} files in {seconds:.02f} seconds".format(files=len(output_files), seconds=time.time() - now))

    if return_filenames:
        return output_files
    return


"""Functions for parsing the files"""


def get_filenames(years=None):
    if years:
        year_strs = [f' on {year}-' for year in years]  # eg "Fred Hope on 2018-01-01 at 16.17.18"
        return [f for f in os.listdir(COPIED_MESSAGE_LOG_DIR) if f.endswith(FILE_SUFFIX) and any([year_str in f for year_str in year_strs])]
    else:
        return [f for f in os.listdir(COPIED_MESSAGE_LOG_DIR) if f.endswith(FILE_SUFFIX)]


def get_name_groups():
    """
    Name groups are mappings between a person's name (or however you want them to be identified) and other names, phone numbers, or emails
    they might be identified as in the various files.
    name_groups.json is a separate file (not included in this repo) because it contains PII.
    You can use your own, with format:
        {
            "Name": ["Alt Name", "Another Alt Name", "alt@email.com"],
            ...
        }
    """

    with open(os.path.join(BASE_REPO_DIR, 'name_groups.json'), 'r') as ng:
        name_groups = json.load(ng)
    name_groups = {k: set(v) for k, v in name_groups.items()}
    assert sum([len(s) for s in name_groups.values()]) == len(frozenset().union(*name_groups.values())), "Name groups must be pairwise disjoint"
    return name_groups


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
    `pat` can be a compiled regex pattern or a string.
    """

    if isinstance(pat, str):
        pat = re.compile(pat)
    matches = [i for i in l if pat.search(i)]
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

    tag = TAG_PATTERN.match(s).group(1)
    s = s.replace(f'<{tag}>', '').replace(f'</{tag}>', '')
    return s


def datetime_from_cocoa_time(ts):
    """
    Cocoa timestamps are a number of seconds since 2001-01-01 midnight GMT
    """

    return datetime.datetime(2001, 1, 1) + datetime.timedelta(seconds=ts)


def generate_sender_id_mapping(sender_ids, conversation_started_by, other_name):
    if len(conversation_started_by) == 1:
        # Just one thread (iMessage or SMS but not both). Lower sender id is the person who started the thread
        person_order = (MY_DISPLAY_NAME, other_name) if conversation_started_by[0] in MY_CONTACT_INFO_IDS else (other_name, MY_DISPLAY_NAME)
        if len(sender_ids) == 1:  # Just one person total
            return {sender_ids[0]: person_order[0]}
        elif len(sender_ids) == 2:  # Both people
            return {sender_ids[0]: person_order[0], sender_ids[1]: person_order[1]}
        else:
            raise Exception(f"sender_ids {sender_ids} found with conversation_started_by {conversation_started_by}, expecting between 1 and 2 sender ids")
    elif len(conversation_started_by) == 2:
        # Two threads (SMS and iMessage)
        person_order = [(MY_DISPLAY_NAME, other_name) if c in MY_CONTACT_INFO_IDS else (other_name, MY_DISPLAY_NAME) for c in conversation_started_by]
        if len(sender_ids) == 1:  # One person, not sure why the second thread starts
            # print(f"Warning: sender_ids {sender_ids} found with conversation_started_by {conversation_started_by}, expecting between 2 and 4 sender ids")
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
        ('&at;', '@'),  # not a real XML escape, but we use this to "protect" messages that are just an email
        ('&tel;', ''),  # not a real XML escape, but we use this to "protect" messages that are just a phone number
        ('&amp;', '&'),
        ('\xa0', ''),
    ]

    s = NBSP_PATTERN.sub(r'\1 \2', s)
    for escaped, real in escape_chars:
        s = s.replace(escaped, real)
    s = s.replace('\xa0', '')
    return s


def get_primary_other_name(name, name_groups):
    """
    Checks the name groups to find the primary name associated with this name.
    """

    if name in name_groups:
        return name
    for primary_name, alt_names in name_groups.items():
        if name in alt_names:
            return primary_name
    return name


def get_all_other_names(name, name_groups):
    primary_other_name = get_primary_other_name(name, name_groups)
    return name_groups.get(primary_other_name, {primary_other_name})


def get_all_other_name_emails(name, name_groups):
    all_other_names = get_all_other_names(name, name_groups)
    return set([name for name in all_other_names if EMAIL_PATTERN.match(name)])


def other_name_from_filename(filename):
    return OTHER_NAME_PATTERN.search(filename).group(1)


def parse_file(filename, debug_mode=DEBUG_MODE, name_groups=None):
    try:
        name_groups = name_groups or get_name_groups()
        other_name = other_name_from_filename(filename)
        all_other_name_emails = get_all_other_name_emails(other_name, name_groups)
        primary_other_name = get_primary_other_name(other_name, name_groups)

        with open(os.path.join(COPIED_MESSAGE_LOG_DIR, filename), 'r') as f:
            lines = f.readlines()
        lines = [l.rstrip('\n') for l in lines]

        # Remove lines containing blocks of 52 alphanumeric chars, this represents data e.g. attachments.
        # We don't actually have to catch all of them, this is just an initial stripdown for performance.
        # Doing it without regex as the regex is way slower
        # lines = [l for l in lines if not DATA_PATTERN.match(l)]
        lines = [l for l in lines if not (l.startswith('\t\t\t') and len(l) == 55 and ' ' not in l and '<' not in l)]

        # Remove tab padding
        lines = [TAB_PADDING_PATTERN.sub('<', l) for l in lines]

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
            if lines[i].startswith('<string>E:') and ATTACHMENT_UUID_PATTERN.match(lines[i-1]):
                # Skip the SMS line if there is one, this isn't helpful
                conversation_search_idx = i+2 if lines[i+1] == '<string>SMS</string>' else i+1
                conversation_started_by.append(strip_tags(first_substr_match(lines[conversation_search_idx:], '<string>')))
            i += 1

        def is_contact_info_line(line):
            return bool(PHONE_NUMBER_PATTERN.match(line)
                or (BARE_PHONE_NUMBER_PATTERN.match(line) and strip_tags(line) == other_name)
                or strip_tags(line) in MY_CONTACT_INFO_IDS
                or strip_tags(line) in conversation_started_by
                or (AUTOMATED_SENDER_PATTERN.match(other_name) and strip_tags(line) == other_name)
                or (other_name in OTHER_FORMATTED_NUMBERS and strip_tags(line) in OTHER_RAW_NUMBERS)
                or any((EMAIL_PATTERN.match(name) and strip_tags(line) == name.lower()) for name in all_other_name_emails))

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

        # We usually throw out instances of <string>(email)</string> because they are fake, but it's possible this was an actual message.
        # If this happens, there should be a <string>mailto:(email)</string> just after it.
        # Same thing for <string>(phonenumber)</string> with <string>tel:(phonenumber)</string> after it.
        i = 0
        while True:
            if i >= len(lines) - 1:
                break
            if MY_EMAIL in lines[i] and first_substr_match(lines[(i+1):], MY_EMAIL) == f'<string>mailto:{MY_EMAIL}</string>':
                lines[i] = lines[i].replace('@', '&at;')
            elif any(name in lines[i] and first_substr_match(lines[(i+1):], name) == f'<string>mailto:{name}</string>' for name in all_other_name_emails):
                lines[i] = lines[i].replace('@', '&at;')
            else:
                phone_number_match = PHONE_NUMBER_PATTERN.match(lines[i])
                if phone_number_match and first_substr_match(lines[(i+1):], phone_number_match.group(1)) == f'<string>tel:{phone_number_match.group(1)}</string>':
                    lines[i] = lines[i].replace('<string>', '<string>&tel;')

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
        lines = [ATTACHMENT_UUID_PATTERN.sub('<string>(MEDIA)</string>', l) for l in lines]
        lines = [l.replace(chr(65532), '(MEDIA)') for l in lines]

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

        # A sender code of 0 usually indicates a "iMessage with x" message that isn't a real message; delete these, but take note of them
        imessage_with_lines = set()
        i = 0
        while True:
            if i >= len(lines) - 2:
                break
            if lines[i] == '<integer>0</integer>' and lines[i+1].startswith('<real>') and 'iMessage with' in lines[i+2]:
                imessage_with_lines.add(strip_tags(lines[i+2]))
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
            if i >= len(lines) - 1:
                break
            if lines[i].startswith('<real>'):
                latest_timestamp = lines[i]
            if lines[i].startswith('<integer>') and not lines[i+1].startswith('<real>'):
                if is_contact_info_line(lines[i+1]):
                    lines.pop(i+1)
                    continue
                else:
                    lines = lines[:(i+1)] + [INFERRED_TIMESTAMP_PATTERN.sub('*</real>', latest_timestamp)] + lines[(i+1):]
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
        # Another way to have multiple threads is if there are different numbers for the same contact. Throw these out for now.
        if len(sender_ids) > 2 and len(conversation_started_by) == 1 and (imessage_with_lines or primary_other_name == MY_NAME):
            # print(f'WARNING: in file {filename}, sender_ids {sender_ids} but only conversation_started_by {conversation_started_by}, likely due to "iMessage with" lines {imessage_with_lines}.')
            sender_id_mapping = {sender_id: 'Unknown' for sender_id in sender_ids}
        else:
            sender_id_mapping = generate_sender_id_mapping(sender_ids, conversation_started_by, primary_other_name)

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
            message_dict = {'sender': sender, 'timestamp': timestamp, 'message': message}
            if debug_mode:
                message_dict.update({'is_timestamp_inferred': '*' in timestamp_str, 'sender_id': sender_id, 'meta': meta})
            messages.append(message_dict)
        assert len(messages) == len(lines) / 3, f"{len(lines)} lines became {len(messages)} messages"
    except Exception:
        print(filename)
        raise

    return sorted(messages, key=lambda k: k['timestamp']), primary_other_name


def parse_file_with_kwargs(kwargs):
    """
    executor.map can only pass a single arg (and we can't use a lambda because lambdas can't be pickled),
    so this helper function takes kwargs as a single arg and extracts it before calling parse_file.
    https://stackoverflow.com/questions/42056738/how-to-pass-a-function-with-more-than-one-argument-to-python-concurrent-futures
    """

    return parse_file(**kwargs)


def dedupe_messages(messages):
    """
    At least in my data, there are legitimately duplicated message files, with separate dates, where messages appear again.
    This is how the data is stored by Messages and nothing to do with this tool.

    We can dedupe on (sender, timestamp, message) for a given person. Unfortunately, this means if the same message is sent multiple times in the same second
    (aka "spammed"), we will legitimately lose that piece of information (it will appear only once after deduping).
    """

    unique_messages = []
    processed_messages = set()
    for message in messages:
        unique_message = (message['sender'], message['timestamp'], message['message'])
        if unique_message not in processed_messages:
            unique_messages.append(message)
            processed_messages.add(unique_message)
    return unique_messages


def parse_files(filenames=None, years=None):
    """
    Parses a list of files or all the files for the specified years
    """

    if filenames and years:
        raise ValueError("Cannot filter by both filenames and years")
    if filenames is None:
        filenames = get_filenames(years=years)
    messages = {}
    print("Parsing...")
    now = time.time()
    name_groups = get_name_groups()
    kwargs = [{'filename': f, 'name_groups': name_groups} for f in filenames]
    with ProcessPoolExecutor() as executor:
        for new_messages, other_name in list(tqdm.tqdm(executor.map(parse_file_with_kwargs, kwargs, chunksize=3), total=len(filenames))):
            messages[other_name] = messages.get(other_name, []) + new_messages
    print("\nParsed {files} files in {seconds:.02f} seconds".format(files=len(filenames), seconds=time.time() - now))

    for other_name in messages:
        messages[other_name] = dedupe_messages(sorted(messages[other_name], key=lambda k: k['timestamp']))

    return messages


def copy_and_parse_files(years=None, parse_copied_files_only=True):
    """
    Copies/decrypts files for the specified years, and parses them to get the messages.
    Basically a combination of copy_files() and parse_files()
    If parse_copied_files_only, parse only the files that were copied, even if others exist in the directory.
    """

    filenames = copy_files(years=years, return_filenames=parse_copied_files_only)
    messages = parse_files(filenames)
    return messages


"""Functions for searching and printing the parsed data"""


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


def search_corpus(message_list, query, ignore_case=True, regex=False, regex_group=None, context=0):
    """
    Searches a list of messages for a substring or regex patter and prints the results as a tabulated DataFrame.

    :param message_list: list of message objects. E.g. if `messages` was returned by parse_files(), this can be messages['Dan']
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

    matches = []
    for idx, m in enumerate(message_list):
        match = _search(query, m['message'])
        if match:
            matches.append((idx, match))
    if not matches:
        return
    df = pd.DataFrame(message_list)
    for message_idx, substr_range in matches:
        sub_df = df.loc[(message_idx-context):(message_idx+context), :]
        print(tabulate_df(sub_df, substr_highlights={message_idx: substr_range}))



