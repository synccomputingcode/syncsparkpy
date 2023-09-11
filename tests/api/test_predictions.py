import orjson
import respx
from httpx import Response

from sync.api import predictions
from sync.config import CONFIG
from sync.models import Platform

# auth route will only be called in the first test
mock_router = respx.mock(base_url=CONFIG.api_url, assert_all_called=False)
mock_router.post("/v1/auth/token").mock(
    return_value=Response(
        200,
        json={
            "result": {"access_token": "notarealtoken", "expires_at_utc": "2022-09-01T20:54:48Z"}
        },
    )
)


@mock_router
def test_create_prediction():
    prediction_id = "2c33df4a-c491-4602-9a8a-1353ddec4376"
    mock_router.post("/v1/autotuner/predictions").mock(
        return_value=Response(202, json={"result": {"prediction_id": prediction_id}})
    )

    response = predictions.create_prediction(
        Platform.AWS_EMR, {}, "https://hello.s3.awsamazon.com/world"
    )

    assert response.result == prediction_id


@mock_router
def test_get_prediction_status():
    prediction_id = "2c33df4a-c491-4602-9a8a-1353ddec4376"
    mock_router.get(f"/v1/autotuner/predictions/{prediction_id}/status").mock(
        side_effect=lambda r: Response(200, json={"result": {"status": "SUCCESS"}})
        if prediction_id in r.url.path
        else Response(404)
    )

    response = predictions.get_status(prediction_id)

    assert response.result == "SUCCESS"


@mock_router
def test_get_prediction():
    prediction_id = "e26c36fa-3b50-4d42-a412-19db210591a4"
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

    response = predictions.get_prediction(prediction_id)

    assert response.result["prediction_id"] == prediction_id
