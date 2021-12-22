# Copyright 2020 Google, LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import base64
import json

import main
import shared

import mock
import pytest


@pytest.fixture
def client():
    main.app.testing = True
    return main.app.test_client()


def test_not_json(client):
    with pytest.raises(Exception) as e:
        client.post("/", data="foo")

    assert "Expecting JSON payload" in str(e.value)


def test_not_pubsub_message(client):
    with pytest.raises(Exception) as e:
        client.post(
            "/",
            data=json.dumps({"foo": "bar"}),
            headers={"Content-Type": "application/json"},
        )

    assert "Not a valid Pub/Sub Message" in str(e.value)


def test_missing_msg_attributes(client):
    with pytest.raises(Exception) as e:
        client.post(
            "/",
            data=json.dumps({"message": "bar"}),
            headers={"Content-Type": "application/json"},
        )

    assert "Missing pubsub attributes" in str(e.value)


def test_new_source_event_processed(client):
    data = json.dumps({
        "object": "page",
        "id": "591f1b41-d009-4a19-b29a-596b1e80ffb6",
        "created_time": "2021-12-15T18:30:00.000Z",
        "last_edited_time": "2021-12-22T13:01:00.000Z",
        "cover": None,
        "icon": None,
        "parent": {
            "type": "database_id",
            "database_id": "04b356ab-6995-43a7-824f-ef7294344e5b"
        },
        "archived": False,
        "properties": {
            "#BugT - Started by": {
                "id": "CcJT",
                "type": "rich_text",
                "rich_text": [
                    {
                        "type": "text",
                        "text": {
                            "content": "U02LR1Y1X0C",
                            "link": None
                        },
                        "annotations": {
                            "bold": False,
                            "italic": False,
                            "strikethrough": False,
                            "underline": False,
                            "code": False,
                            "color": "default"
                        },
                        "plain_text": "U02LR1Y1X0C",
                        "href": None
                    }
                ]
            },
            "#BugT - translation": {
                "id": "HbGj",
                "type": "checkbox",
                "checkbox": False
            },
            "#BugT - number of downtimes": {
                "id": "IOMC",
                "type": "number",
                "number": None
            },
            "#BugT - typo": {
                "id": "LDlj",
                "type": "checkbox",
                "checkbox": False
            },
            "#BugT - Slack ts": {
                "id": "M%3ApL",
                "type": "rich_text",
                "rich_text": [
                    {
                        "type": "text",
                        "text": {
                            "content": "1639593023.213500",
                            "link": None
                        },
                        "annotations": {
                            "bold": False,
                            "italic": False,
                            "strikethrough": False,
                            "underline": False,
                            "code": False,
                            "color": "default"
                        },
                        "plain_text": "1639593023.213500",
                        "href": None
                    }
                ]
            },
            "D. Difficulty": {
                "id": "M%60VG",
                "type": "number",
                "number": 2
            },
            "Column": {
                "id": "MnoW",
                "type": "rich_text",
                "rich_text": []
            },
            "#BugT - monitor down": {
                "id": "Pcs%7C",
                "type": "select",
                "select": None
            },
            "P. Impact": {
                "id": "S%5D%3Eq",
                "type": "number",
                "number": None
            },
            "Epic": {
                "id": "U%3C%7B_",
                "type": "checkbox",
                "checkbox": False
            },
            "#BugT - downtime": {
                "id": "Vu%3FY",
                "type": "checkbox",
                "checkbox": False
            },
            "Estimated number of days": {
                "id": "Vw%60L",
                "type": "formula",
                "formula": {
                    "type": "number",
                    "number": 0
                }
            },
            "Last edited time": {
                "id": "XbD%3F",
                "type": "last_edited_time",
                "last_edited_time": "2021-12-22T13:01:00.000Z"
            },
            "Created by": {
                "id": "YjCa",
                "type": "created_by",
                "created_by": {
                    "object": "user",
                    "id": "377e172a-c1f4-4f3c-bdc3-5ccc470a5b5c",
                    "name": "bugs-channel-integration",
                    "avatar_url": None,
                    "type": "bot",
                    "bot": {}
                }
            },
            "Card type representation": {
                "id": "%5B%3ACZ",
                "type": "formula",
                "formula": {
                    "type": "string",
                    "string": "\ud83d\udc1b"
                }
            },
            "#BugT - Already reported link": {
                "id": "%5B%3FWr",
                "type": "number",
                "number": None
            },
            "Created at": {
                "id": "%5CV%7CD",
                "type": "created_time",
                "created_time": "2021-12-15T18:30:00.000Z"
            },
            "Priority": {
                "id": "%5C%7CuW",
                "type": "select",
                "select": {
                    "id": "649103ef-d674-4b70-9bcc-256d147715e4",
                    "name": "High",
                    "color": "orange"
                }
            },
            "Prioridad - Chores": {
                "id": "%5DSJc",
                "type": "formula",
                "formula": {
                    "type": "number",
                    "number": 0
                }
            },
            "C. Chances": {
                "id": "_%60_z",
                "type": "number",
                "number": None
            },
            "Experimento card": {
                "id": "%60A%5BM",
                "type": "relation",
                "relation": []
            },
            "#BugT - closed time": {
                "id": "aSPk",
                "type": "date",
                "date": {
                    "start": "2021-12-15T20:30:00.000-05:00",
                    "end": None,
                    "time_zone": None
                }
            },
            "#BugT - closed by": {
                "id": "d%3A%5Bd",
                "type": "rich_text",
                "rich_text": [
                    {
                        "type": "text",
                        "text": {
                            "content": "U02LR1Y1X0C",
                            "link": None
                        },
                        "annotations": {
                            "bold": False,
                            "italic": False,
                            "strikethrough": False,
                            "underline": False,
                            "code": False,
                            "color": "default"
                        },
                        "plain_text": "U02LR1Y1X0C",
                        "href": None
                    }
                ]
            },
            "On hold": {
                "id": "jY%5Dn",
                "type": "formula",
                "formula": {
                    "type": "boolean",
                    "boolean": False
                }
            },
            "PR links": {
                "id": "jfyR",
                "type": "files",
                "files": [
                    {
                        "name": "https://github.com/torre-labs/vader/pull/934",
                        "type": "external",
                        "external": {
                            "url": "https://github.com/torre-labs/vader/pull/934"
                        }
                    }
                ]
            },
            "Engineers proficiency": {
                "id": "moz%3A",
                "type": "multi_select",
                "multi_select": []
            },
            "Engineering parent": {
                "id": "p_od",
                "type": "relation",
                "relation": []
            },
            "#BugT - first approach": {
                "id": "q%40%3FD",
                "type": "date",
                "date": {
                    "start": "2021-12-15T20:15:00.000-05:00",
                    "end": None,
                    "time_zone": None
                }
            },
            "Sprint": {
                "id": "tWI~",
                "type": "multi_select",
                "multi_select": [
                    {
                        "id": "057586b3-14c5-48e8-8156-e35d9eca7f31",
                        "name": "Sprint 14",
                        "color": "purple"
                    }
                ]
            },
            "Squad": {
                "id": "yY%40t",
                "type": "multi_select",
                "multi_select": [
                    {
                        "id": "084ea5f2-45e8-4089-abf5-25213c5b4cc6",
                        "name": "Talent squad",
                        "color": "pink"
                    }
                ]
            },
            "dialog": {
                "id": "title",
                "type": "title",
                "title": [
                    {
                        "type": "text",
                        "text": {
                            "content": "On job post onboarding, when the admin tries to select a country for a remote job for certain countries, the dropdown is not being displayed correctly for screens with heigth less than 678px #vers",
                            "link": None
                        },
                        "annotations": {
                            "bold": False,
                            "italic": False,
                            "strikethrough": False,
                            "underline": False,
                            "code": False,
                            "color": "default"
                        },
                        "plain_text": "On job post onboarding, when the admin tries to select a country for a remote job for certain countries, the dropdown is not being displayed correctly for screens with heigth less than 678px #vers",
                        "href": None
                    }
                ]
            },
            "Current ETA": {
                "id": "trello_due",
                "type": "date",
                "date": None
            },
            "Other labels": {
                "id": "trello_label",
                "type": "multi_select",
                "multi_select": [
                    {
                        "id": "[s;]",
                        "name": "Bug",
                        "color": "purple"
                    }
                ]
            },
            "Assignees": {
                "id": "trello_assign",
                "type": "people",
                "people": [
                    {
                        "object": "user",
                        "id": "e83c2e54-0025-4313-8e11-669a69d34765",
                        "name": "Kalib Hackin",
                        "avatar_url": None,
                        "type": "person",
                        "person": {
                            "email": "kalib@torre.co"
                        }
                    }
                ]
            },
            "Stage": {
                "id": "trello_status",
                "type": "select",
                "select": {
                    "id": "vj?t",
                    "name": "\u2705   Done",
                    "color": "default"
                }
            },
            "Attachments": {
                "id": "trello_attachment",
                "type": "files",
                "files": []
            }
        },
        "url": "https://www.notion.so/On-job-post-onboarding-when-the-admin-tries-to-select-a-country-for-a-remote-job-for-certain-countr-591f1b41d0094a19b29a596b1e80ffb6"
    }).encode("utf-8")

    pubsub_msg = {
        "message": {
            "data": base64.b64encode(data).decode("utf-8"),
            "attributes": {"foo": "bar"},
            "message_id": "foobar",
        },
    }

    event = {
        "event_type": "incident",
        "id": "notion-591f1b41-d009-4a19-b29a-596b1e80ffb6",
        "metadata": data.decode(),
        "time_created": "2021-12-15T18:30:00.000Z",
        "signature": "591f1b41-d009-4a19-b29a-596b1e80ffb6",
        "msg_id": "foobar",
        "source": "incident-reporter",
    }

    shared.insert_row_into_bigquery = mock.MagicMock()

    r = client.post(
        "/",
        data=json.dumps(pubsub_msg),
        headers={"Content-Type": "application/json"},
    )

    shared.insert_row_into_bigquery.assert_called_with(event)
    assert r.status_code == 204
