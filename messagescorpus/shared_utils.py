import json
import os

BASE_REPO_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# How your messages will appear in the parsed logs
MY_DISPLAY_NAME = 'Fred'


def get_primary_other_name(name, name_groups):
    """
    Checks the name groups to find the primary name associated with this name.
    """

    if name in name_groups:
        return name
    found_primary_name = None
    for primary_name, alt_names in name_groups.items():
        if name in alt_names:
            if found_primary_name:
                raise KeyError(f'Other name "{name}" listed under multiple primary names ("{found_primary_name}" and "{primary_name}")')
            found_primary_name = primary_name
    return found_primary_name if found_primary_name is not None else name


def get_name_groups():
    """
    Name groups are mappings between a person's name (or however you want them to be identified) and other names, phone numbers, or emails
    they might be identified as in the various files.
    Create name_groups.json if it doesn't exist, in the base repo directory (it will be gitignored).
    You can use your own, with format:
        {
            "Name": ["Alt Name", "Another Alt Name", "alt@email.com"],
            ...
        }
    For recent versions of OS X, be sure to include the person's phone number in "+11234567890" format
    as that's how the threads are formatted, rather than using their name.
    """

    with open(os.path.join(BASE_REPO_DIR, 'name_groups.json'), 'r') as ng:
        name_groups = json.load(ng)
    name_groups = {k: set(v) for k, v in name_groups.items()}
    assert sum([len(s) for s in name_groups.values()]) == len(frozenset().union(*name_groups.values())), "Name groups must be pairwise disjoint"
    return name_groups
