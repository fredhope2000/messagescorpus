import re

from flask import Flask, render_template, request

from messagescorpus.corpus import message_names_from_sqlite, messages_from_sqlite, search_corpus


app = Flask(__name__)
MESSAGE_CACHE = {}
MESSAGE_NAMES_CACHE = None
DEFAULT_THREAD_MESSAGE_LIMIT = 20
THREAD_MESSAGE_LIMIT_INCREMENT = 10
SEARCH_CONTEXT_INCREMENT = 5


def parse_int_arg(name, default, minimum=None):
    value = request.args.get(name, "")
    try:
        parsed = int(value) if value != "" else default
    except ValueError:
        return default
    if minimum is not None:
        return max(minimum, parsed)
    return parsed


def parse_checkbox_arg(name, default=False):
    if name not in request.args:
        return default
    return request.args.get(name) == "on"


def get_cached_messages(name):
    was_cached = name in MESSAGE_CACHE
    if not was_cached:
        MESSAGE_CACHE[name] = messages_from_sqlite(other_name_filter=name)
    return MESSAGE_CACHE[name], was_cached


def refresh_cached_messages(name):
    MESSAGE_CACHE.pop(name, None)
    MESSAGE_CACHE[name] = messages_from_sqlite(other_name_filter=name)
    return MESSAGE_CACHE[name]


def get_cached_message_names():
    global MESSAGE_NAMES_CACHE
    if MESSAGE_NAMES_CACHE is None:
        MESSAGE_NAMES_CACHE = message_names_from_sqlite()
    return MESSAGE_NAMES_CACHE


def refresh_cached_message_names():
    global MESSAGE_NAMES_CACHE
    MESSAGE_NAMES_CACHE = message_names_from_sqlite()
    return MESSAGE_NAMES_CACHE


def highlight_message(message, match_span):
    start, end = match_span
    return {
        "before": message[:start],
        "match": message[start:end],
        "after": message[end:],
    }


def build_thread_rows(messages, limit=DEFAULT_THREAD_MESSAGE_LIMIT):
    recent_messages = messages[-limit:]
    return [
        {
            "timestamp": message["timestamp"],
            "sender": message["sender"],
            "is_match": False,
            "message_parts": None,
            "message": message["message"],
        }
        for message in recent_messages
    ]


def build_result_blocks(search_results, context, most_recent, expanded_match_index=None, extra_before=0, extra_after=0):
    if search_results is None:
        return []

    df = search_results["dfs"][None]
    matches = search_results["matches"][None]
    total_rows = len(df)
    if most_recent:
        df = df.iloc[::-1]

    result_blocks = []
    for message_idx, match_span in matches:
        block_extra_before = extra_before if expanded_match_index == message_idx else 0
        block_extra_after = extra_after if expanded_match_index == message_idx else 0
        if most_recent:
            start_idx = message_idx + context + block_extra_before
            end_idx = message_idx - context - block_extra_after
            sub_df = df.loc[start_idx:end_idx, :]
            can_expand_before = start_idx < (total_rows - 1)
            can_expand_after = end_idx > 0
        else:
            start_idx = message_idx - (context + block_extra_before)
            end_idx = message_idx + (context + block_extra_after)
            sub_df = df.loc[start_idx:end_idx, :]
            can_expand_before = start_idx > 0
            can_expand_after = end_idx < (total_rows - 1)
        rows = []
        for row_idx, row in sub_df.iterrows():
            rows.append({
                "timestamp": row["timestamp"],
                "sender": row["sender"],
                "is_match": row_idx == message_idx,
                "message_parts": highlight_message(row["message"], match_span) if row_idx == message_idx else None,
                "message": row["message"],
            })
        result_blocks.append({
            "match_index": message_idx,
            "rows": rows,
            "is_expanded": expanded_match_index == message_idx,
            "can_expand_before": can_expand_before,
            "can_expand_after": can_expand_after,
            "next_extra_before": block_extra_before + SEARCH_CONTEXT_INCREMENT,
            "next_extra_after": block_extra_after + SEARCH_CONTEXT_INCREMENT,
        })
    return result_blocks


@app.route("/")
def index():
    suggested_names = get_cached_message_names()
    search_form_submitted = request.args.get("search_form") == "1"
    form_data = {
        "name": request.args.get("name", ""),
        "query": request.args.get("query", ""),
        "thread_limit": parse_int_arg("thread_limit", DEFAULT_THREAD_MESSAGE_LIMIT, minimum=1),
        "expanded_match": request.args.get("expanded_match", ""),
        "extra_before": parse_int_arg("extra_before", 0, minimum=0),
        "extra_after": parse_int_arg("extra_after", 0, minimum=0),
        "context": parse_int_arg("context", 3, minimum=0),
        "max_results": parse_int_arg("max_results", 20, minimum=1),
        "regex_group": request.args.get("regex_group", ""),
        "ignore_case": parse_checkbox_arg("ignore_case", default=not search_form_submitted),
        "regex": parse_checkbox_arg("regex", default=False),
        "most_recent": parse_checkbox_arg("most_recent", default=not search_form_submitted),
    }
    has_submission = bool(form_data["name"] or form_data["query"])
    refresh_requested = request.args.get("refresh_cache", "") == "1"
    error_message = None
    info_message = None
    cache_status = None
    result_blocks = []
    result_count = 0
    thread_rows = []
    total_thread_messages = 0
    selected_name = form_data["name"]

    if refresh_requested and selected_name:
        try:
            messages = refresh_cached_messages(selected_name)
            refresh_cached_message_names()
            suggested_names = get_cached_message_names()
            cache_status = "refreshed from SQLite"
            info_message = f'Refreshed cache for "{selected_name}" ({len(messages)} messages loaded).'
            thread_rows = build_thread_rows(messages)
        except IndexError:
            error_message = f'No conversation found for "{selected_name}".'
        except ValueError as exc:
            error_message = str(exc)

    if not error_message and selected_name:
        try:
            if refresh_requested and cache_status == "refreshed from SQLite":
                messages = MESSAGE_CACHE[selected_name]
            else:
                messages, was_cached = get_cached_messages(selected_name)
                cache_status = "cache hit" if was_cached else "loaded from SQLite"
            total_thread_messages = len(messages)
            if form_data["query"]:
                regex_group = None
                expanded_match_index = None
                if form_data["expanded_match"] != "":
                    expanded_match_index = int(form_data["expanded_match"])
                if form_data["regex_group"] != "":
                    regex_group = int(form_data["regex_group"])
                search_results = search_corpus(
                    messages,
                    form_data["query"],
                    ignore_case=form_data["ignore_case"],
                    regex=form_data["regex"],
                    regex_group=regex_group,
                    context=form_data["context"],
                    max_results=form_data["max_results"],
                    most_recent=form_data["most_recent"],
                )
                result_blocks = build_result_blocks(
                    search_results,
                    context=form_data["context"],
                    most_recent=form_data["most_recent"],
                    expanded_match_index=expanded_match_index,
                    extra_before=form_data["extra_before"],
                    extra_after=form_data["extra_after"],
                )
                result_count = 0 if search_results is None else search_results["num_matches"]
            else:
                thread_rows = build_thread_rows(messages, limit=form_data["thread_limit"])
        except ValueError as exc:
            error_message = str(exc)
        except IndexError:
            error_message = f'No conversation found for "{selected_name}".'
        except re.error as exc:
            error_message = f"Invalid regex: {exc}"

    return render_template(
        "index.html",
        form_data=form_data,
        has_submission=has_submission,
        suggested_names=suggested_names,
        result_blocks=result_blocks,
        result_count=result_count,
        thread_rows=thread_rows,
        total_thread_messages=total_thread_messages,
        thread_limit_increment=THREAD_MESSAGE_LIMIT_INCREMENT,
        search_context_increment=SEARCH_CONTEXT_INCREMENT,
        error_message=error_message,
        info_message=info_message,
        cache_status=cache_status,
        selected_name=selected_name,
    )


if __name__ == "__main__":
    app.run(debug=True)
