import orjson
import pytest
import respx
from httpx import Response

from sync.asyncapi import predictions
from sync.models import Platform

# auth route will only be called in the first test
mock_router = respx.mock(base_url="https://*.synccomputing.com", assert_all_called=False)
mock_router.post("/v1/auth/token").mock(
    return_value=Response(
        200,
        json={
            "result": {"access_token": "notarealtoken", "expires_at_utc": "2022-09-01T20:54:48Z"}
        },
    )
)


@pytest.mark.asyncio
@mock_router
async def test_generate_prediction():
    prediction_id = "e26c36fa-3b50-4d42-a412-19db210591a4"
    mock_router.post("/v1/autotuner/predictions").mock(
        return_value=Response(202, json={"result": {"prediction_id": prediction_id}})
    )
    mock_router.get(f"/v1/autotuner/predictions/{prediction_id}/status").mock(
        return_value=Response(200, json={"result": {"status": "SUCCESS"}})
    )

    with open("tests/data/predictions_response.json") as predictions_fobj:
        prediction = [
            p
            for p in orjson.loads(predictions_fobj.read())["result"]
            if p["prediction_id"] == prediction_id
        ][0]
    mock_router.get(f"/v1/autotuner/predictions/{prediction_id}").mock(
        side_effect=lambda r: Response(200, json={"result": prediction})
        if prediction_id in r.url.path
        else Response(404)
    )

    response = await predictions.generate_prediction(
        Platform.AWS_EMR, {}, "https://hello.s3.awsamazon.com/world"
    )

    assert response.result["prediction_id"] == prediction_id
