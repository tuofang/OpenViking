# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0

import json

from starlette.responses import Response

from openviking.server.responses import success_json_response


def test_success_json_response_preserves_standard_envelope():
    result = {
        "matches": ["viking://resources/a.txt", "viking://resources/sub/b.txt"],
        "count": 2,
    }

    response = success_json_response(result)

    assert type(response) is Response
    assert response.media_type == "application/json"
    assert json.loads(response.body) == {
        "status": "ok",
        "result": result,
        "error": None,
        "telemetry": None,
        "profile": None,
    }
