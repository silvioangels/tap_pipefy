import json
import os.path
import sys

import pendulum
import requests

import singer
from singer import Schema
from singer import Transformer
from singer import utils
from singer.catalog import Catalog
from singer.catalog import CatalogEntry


REQUIRED_CONFIG_KEYS = ("organization_id personal_access_token").split()
SENSITIVE_CONFIG_KEYS = "personal_access_token".split()  # Won't be logged

BASE_URL = "https://app.pipefy.com/queries"

# Brackets in queries need to be escaped
# Field names are still enclosed in single brackets {}
# For other brackets use double brackets {{ }}

QUERIES = {
    "me": """
          {{
             me {{
                id
                name
                username
                email
                avatarUrl
                created_at
                locale
                timeZone
              }}
            }}
            """,
    "cards": """
             {{
              cards(first: {page_size},
                           {after}
                           pipe_id: {pipe_id}) {{
                edges {{
                  node {{
                    id
                    title
                    created_by {{
                      id
                    }}
                    assignees {{
                      id
                    }}
                    comments {{
                    id
                    author {{
                        id
                        }}
                    text
                    created_at
                    }}
                    comments_count
                    current_phase {{
                      name
                    }}
                    done
                    due_date
                    created_at
                    updated_at
                    finished_at
                    fields {{
                      name
                      value
                      updated_at
                    }}
                    labels {{
                      name
                    }}
                    phases_history {{
                      phase {{
                        name
                      }}
                      firstTimeIn
                      lastTimeOut
                    }}
                    url
                  }}
                }}
                pageInfo {{
                      endCursor
                      hasNextPage
                      hasPreviousPage
                      startCursor
                    }}
              }}
             }}
             """,
    "organization": """
                    {{
                      organization(id: {organization_id} ) {{
                        name
                        created_at
                        members {{
                          user {{
                            id
                            name
                            email
                            created_at
                            avatarUrl
                            username
                            timeZone
                            locale
                          }}
                          role_name
                        }}
                        pipes(include_publics: true) {{
                          id
                          name
                          description
                          icon
                          created_at
                          start_form_fields {{
                              id
                              label
                              type
                              required
                          }}
                          phases {{
                            id
                            name
                            cards_count
                            fields {{
                                id
                                label
                                type
                                required
                            }}
                          }}
                        }}
                        tables {{
                          edges {{
                            node {{
                              id
                              name
                              description
                              icon
                              authorization
                              public
                              public_form
                              table_records_count
                              url
                              table_fields {{
                                id
                                label
                                type
                                description
                                is_multiple
                                unique
                                required
                                options
                                }}
                            }}
                          }}
                        }}
                      }}
                    }}
                    """,
    "table_records": """
                {{
                  table_records(first: {page_size},
                                {after}
                                table_id: "{table_id}") {{
                    edges {{
                      cursor
                      node {{
                        id
                        title
                        url
                        created_at
                        updated_at
                        finished_at
                        due_date
                        created_by {{
                            id
                        }}
                        record_fields {{
                            filled_at
                            updated_at
                            required
                            name
                            value
                            field {{
                                id
                                type
                            }}
                        }}
                      }}
                    }}
                    pageInfo {{
                      endCursor
                      hasNextPage
                      hasPreviousPage
                      startCursor
                    }}
                  }}
                }}
                 """
}

MAX_PAGE_SIZE = 50

NUMERIC_TYPES = set("currency number".split())
INTEGER_TYPES = set("id".split())
DATE_TYPES = set("date datetime due_date".split())
STRING_TYPES = set(("cnpj cpf email phone radio_horizontal radio_vertical "
                    "select short_text long_text label_select statement "
                    " time").split())


CONFIG = {
    "page_size": 5
}

STATE = {}

LOGGER = singer.get_logger()

SESSION = requests.session()


def save_json_to_file(data, file_name):
    with open(file_name, "w") as f:
        json.dump(data, f)


def get_query(key, params=None):
    params = params or {}
    return QUERIES[key].format(**params)


@utils.ratelimit(120, 60)
def request(url, query):
    """ Issue http POST request to url, return json response and handle errors
    """
    resp_json = {}
    headers = {
        "Authorization": "Bearer {}".format(CONFIG['personal_access_token']),
        "Accept": "application/json"
    }

    if 'user_agent' in CONFIG:
        headers['User-Agent'] = CONFIG['user_agent']

    data = {"query": query}

    req = requests.Request('POST', url, headers=headers, data=data).prepare()
    LOGGER.info("%s %s \n     QUERY: %s", req.method, req.url, query)

    try:
        resp = SESSION.send(req, timeout=10)

        if resp:
            resp_json = resp.json()
            if "errors" in resp_json:
                LOGGER.error(resp_json)
            return resp_json

        if resp.status_code >= 400:
            resp.raise_for_status()

        if resp is None:
            LOGGER.error("Blank response from %s", url)

    except (requests.exceptions.ConnectionError,
            requests.exceptions.Timeout) as exc:
        LOGGER.error("Request: %s : e: %s", req.url, exc)

    return resp_json


def log_config_keys():
    """ Prints config keys to the log masking the sensitive ones
    """
    for key, value in CONFIG.items():
        if key in SENSITIVE_CONFIG_KEYS:
            value = "*" * len(value)

        LOGGER.info("CONFIG: %s = %s", key, value)


def get_abs_path(path):
    """ Get absolute path for the current file
    """
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schema(stream):
    """ Load schema for the stream
    """
    return utils.load_json(get_abs_path("schemas/{}.json".format(stream)))


def format_date(date):
    """ Convert date string to UTC and RFC3339 format
    """
    if date:
        return singer.utils.strftime(pendulum.parse(date).in_timezone("UTC"))


def transform_datetimes_hook(data, typ, schema):
    """ Transform datetime fields to UTC and RFC3339 format
    """
    if typ in ["string"] and schema.get("format", "") == "date-time":
        data = format_date(data)
    return data


def get_organization(organization_id):
    """ Query API and get info for the organization_id
        Response includes pipes, phases, tables, members
    """
    params = {"organization_id": organization_id}
    query = get_query("organization", params)
    resp = request(BASE_URL, query)
    data = resp.get("data", None)
    return data.get("organization", {})


def get_after(end_cursor):
    """ Get the "after" portion of the pagination query
    """
    return 'after: "{}", '.format(end_cursor) if end_cursor else ""


def get_nodes(data):
    nodes = [item["node"] for item in data.get("edges", [])]
    for node in nodes:
        yield node


def get_cards(pipe_id, end_cursor=None):
    """ Query API and get cards for the pipe_id
    """
    has_next_page = True

    while has_next_page:
        params = {
            "pipe_id": pipe_id,
            "page_size": max(1, min(CONFIG["page_size"], MAX_PAGE_SIZE)),
            "after": get_after(end_cursor)
        }

        query = get_query("cards", params)
        resp = request(BASE_URL, query)
        data = resp.get("data", {})
        cards = data.get("cards", {})

        page_info = cards.get("pageInfo", {})
        has_next_page = page_info.get("hasNextPage", False)
        end_cursor = page_info.get("endCursor", "")

        for card in get_nodes(cards):
            yield card


def process_table_record(record):
    record_fields = record.pop("record_fields", [])
    output_fields = {
        field["field"]["id"]: field["value"]
        for field in record_fields
    }
    output_fields.update({"__id": record["id"]})
    return output_fields


def get_table_records(table_id, end_cursor=None):
    """ Query API and get table_records for the table_id
    """
    has_next_page = True

    while has_next_page:
        params = {
            "table_id": table_id,
            "page_size": max(1, min(CONFIG["page_size"], MAX_PAGE_SIZE)),
            "after": get_after(end_cursor)
        }

        query = get_query("table_records", params)
        resp = request(BASE_URL, query)
        data = resp.get("data", {})
        table_records = data.get("table_records", {})

        page_info = table_records.get("pageInfo", {})
        has_next_page = page_info.get("hasNextPage", False)
        end_cursor = page_info.get("endCursor", "")

        for record in table_records.get("edges", []):
            yield process_table_record(record["node"])


def test_api_connection():
    """ Send 'organization' query to the API to test connection
    """
    LOGGER.info("Testing API connection. Issuing 'organization' query")

    org = get_organization(CONFIG["organization_id"])

    if org:
        LOGGER.info("API connection successful")
        LOGGER.info("organization_id: %s, name: %s",
                    CONFIG["organization_id"], org["name"])
    else:
        LOGGER.critical(
            "API connection failed. Unable to find data for organization: %s",
            CONFIG["organization_id"]
        )
        sys.exit(-1)


def load_static_schema(stream):
    """ Append 'inclusion': 'automatic' property to all fields in the schema
    """
    schema = load_schema(stream)
    for key in schema['properties']:
        schema['properties'][key]['inclusion'] = 'automatic'
    return schema


def load_static_schemas(streams):
    """ Load default schemas for all streams
    """
    for stream in streams:
        LOGGER.info('Loading schema for %s', stream.tap_stream_id)
        stream.discovered_schema.update(load_static_schema(stream))


STREAMS = [
    {"stream": "members", "static": True},
    {"stream": "pipes", "static": True},
    {"stream": "cards", "static": False},
    {"stream": "tables", "static": True}
]

catalog_entries = [
    CatalogEntry(
        tap_stream_id=stream["stream"],
        stream=stream["stream"],
        key_properties=["id"],
        schema=Schema.from_dict(load_static_schema(stream["stream"]))
    ) for stream in STREAMS if stream["static"]
]

CATALOG = Catalog(catalog_entries)

LOGGER.info("Catalog is: %s", json.dumps(CATALOG.to_dict(), indent=2))


LOGGER.info("There are %s static streams", len(CATALOG.streams))
LOGGER.info("STREAMS: %s",
            [stream.stream for stream in CATALOG.streams])


def append_property_schema(schema, field):
    property_schema = {"inclusion": "automatic"}
    property_schema['type'] = []

    if field["required"]:
        schema["required"].append(field["id"])
    else:
        property_schema['type'].append("null")

    if field["type"] in (STRING_TYPES | DATE_TYPES) \
            or field["is_multiple"]:
        property_schema['type'].append("string")

    if field["type"] in DATE_TYPES:
        property_schema["format"] = "date-time"

    if field["type"] in NUMERIC_TYPES:
        property_schema["type"].append("number")

    if field["type"] in INTEGER_TYPES or field["type"] == "integer":
        property_schema["type"].append("integer")

    schema["properties"][field["id"]] = property_schema
    return schema


def get_schema_for_table(table):
    """ Given a table object, output its schema
    """
    schema = {"type": "object", "properties": {}, "required": []}
    table_fields = table.get("table_fields", [])

    # Add __id (record id) to every table

    record_id_field = {
        "id": "__id",
        "type": "integer",
        "required": True,
        "is_multiple": False
    }

    table_fields.append(record_id_field)

    for field in table_fields:
        schema = append_property_schema(schema, field)

    return Schema.from_dict(schema)


def get_schema_for_fields(fields):
    schema = {"type": "object", "properties": {}, "required": []}
    for field in fields.values():
        append_property_schema(schema, field)
    return schema


def combine_schemas(schema1, schema2):
    properties1 = schema1["properties"]
    schema2["properties"].update(properties1)
    return schema2


def get_dynamic_streams():
    """ Get dynamic table schemas
        Append dynamic fields to cards schema
    """
    entries = []
    org = get_organization(CONFIG["organization_id"])
    pipes = org.pop("pipes", [])
    tables = org.pop("tables", [])
    tables = list(get_nodes(tables))

    for pipe in pipes:
        all_fields = pipe.pop("start_form_fields", [])

        phases = pipe.get("phases", [])
        for phase in phases:
            fields = phase.pop("fields", [])
            for field in fields:
                all_fields.append(field)

    unique_fields = {field["id"]: field for field in all_fields}

    static_cards_schema = load_static_schema("cards")
    dynamic_cards_schema = get_schema_for_fields(unique_fields)
    cards_schema = combine_schemas(static_cards_schema, dynamic_cards_schema)

    cards_entry = CatalogEntry(
        tap_stream_id="cards",
        stream="cards",
        key_properties=["id"],
        schema=Schema.from_dict(cards_schema)
    )

    entries.append(cards_entry)

    for table in tables:
        stream = "table_{}".format(table["id"])
        entry = CatalogEntry(
            tap_stream_id=stream,
            stream=stream,
            key_properties=["__id"],
            schema=get_schema_for_table(table)
        )
        entries.append(entry)

    LOGGER.info("There are %s tables (dynamic schemas)", len(tables))
    return entries


def discover_schemas():
    """ Generate a list of streams supported by the tap
    """
    LOGGER.info("Discovering schemas ...")

    CATALOG.streams.extend(get_dynamic_streams())
    schemas = []
    for stream in CATALOG.streams:
        schema = {
            'tap_stream_id': stream.tap_stream_id,
            'stream': stream.stream,
            'schema': stream.schema.to_dict(),
            'key_properties': stream.key_properties
        }
        schemas.append(schema)

    return {'streams': schemas}


def write_catalog_schema(stream):
    """ Output SCHEMA message for the stream
    """
    if stream:
        singer.write_schema(
            stream.tap_stream_id,
            stream.schema.to_dict(),
            stream.key_properties
        )


def write_members(members):
    """ Process members array and output SCHEMA and RECORD messages
    """
    members_stream = CATALOG.get_stream("members")
    write_catalog_schema(members_stream)

    for member in members:
        user = member.pop("user")
        member.update(user)

        with Transformer(pre_hook=transform_datetimes_hook) as xform:
            member = xform.transform(member, members_stream.schema.to_dict())
            singer.write_record("members", member)


def get_id_from_object(obj, id_name):
    return obj.pop(id_name, {}).pop("id", {})


def write_pipes_and_cards(pipes):
    """ Process pipes array and output SCHEMA and RECORD messages
    """
    pipes_stream = CATALOG.get_stream("pipes")
    cards_stream = CATALOG.get_stream("cards")

    write_catalog_schema(pipes_stream)
    write_catalog_schema(cards_stream)

    save_json_to_file(pipes, "pipes_data.json")

    for pipe in pipes:
        with Transformer(pre_hook=transform_datetimes_hook) as xform:
            pipe = xform.transform(pipe, pipes_stream.schema.to_dict())
            singer.write_record("pipes", pipe)

            cards = list(get_cards(pipe["id"]))
            save_json_to_file(cards, "cards_data.json")

            for card in get_cards(pipe["id"]):
                card["pipe_id"] = pipe["id"]
                card["created_by"] = get_id_from_object(card, "created_by")
                comments = card.pop("comments", [])
                for comment in comments:
                    comment["author_id"] = get_id_from_object(
                        comment, "author")
                card["comments"] = comments
                card = xform.transform(card, cards_stream.schema.to_dict())
                singer.write_record("cards", card)


def write_tables_and_records(tables):
    tables = [tab["node"] for tab in tables.get("edges", [])]

    tables_stream = CATALOG.get_stream("tables")
    write_catalog_schema(tables_stream)

    with Transformer(pre_hook=transform_datetimes_hook) as xform:
        for table in tables:
            table = xform.transform(table, tables_stream.schema.to_dict())
            singer.write_record("tables", table)

            table_stream = CATALOG.get_stream("table_{}".format(table["id"]))
            write_catalog_schema(table_stream)

            for table_record in get_table_records(table["id"]):
                table_record = xform.transform(
                    table_record, table_stream.schema.to_dict())
                singer.write_record(table_stream.stream, table_record)


def sync_organization(organization_id):
    """ Sync data for an organization.
        Data includes pipes + phases and tables
    """
    org = get_organization(organization_id)
    members = org.pop("members", [])
    pipes = org.pop("pipes", [])
    tables = org.pop("tables", [])

    write_members(members)
    write_pipes_and_cards(pipes)
    write_tables_and_records(tables)


# def do_sync(state, catalog):
#     """ Sync all selected streams
#     """
#     selected_streams = get_selected_streams(STREAMS, catalog)
#     LOGGER.info("Starting Sync for %s",
#                 [s.tap_stream_id for s in selected_streams])

#     for stream in selected_streams:
#         LOGGER.info("Syncing %s", stream.tap_stream_id)

#         state = stream.sync(state, stream.tap_stream_id, catalog)

#     singer.write_state(state)


def do_discover():
    """ Output streams supported by the tap
    """
    LOGGER.info('Loading schemas')
    json.dump(discover_schemas(), sys.stdout, indent=4)


def main_impl():
    """ Main entry point
    """
    global CATALOG
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    if args.state:
        STATE.update(args.state)
        LOGGER.info("STATE: %s", STATE)

    CONFIG.update(args.config)
    log_config_keys()

    if args.discover:
        do_discover()
    elif args.catalog:
        CATALOG = args.catalog

        # do_sync(STATE, args.catalog)
        sync_organization(CONFIG["organization_id"])
    else:
        LOGGER.info("No catalog was provided")


def main():
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical(exc)
        raise


if __name__ == '__main__':
    main()
