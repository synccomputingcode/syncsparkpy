from sync.clients.sync import get_default_client
from sync.models import AccessReport, AccessReportLine, AccessStatusCode, Response
import boto3 as boto
from urllib.parse import urlparse


def get_access_report() -> AccessReport:
    """Reports access to the Sync API

    :return: access report
    :rtype: AccessReport
    """
    response = get_default_client().get_products()

    error = response.get("error")
    if error:
        return AccessReport(
            [
                AccessReportLine(
                    name="Sync Authentication",
                    status=AccessStatusCode.RED,
                    message=f"{error['code']}: {error['message']}",
                )
            ]
        )

    return AccessReport(
        [
            AccessReportLine(
                name="Sync Authentication",
                status=AccessStatusCode.GREEN,
                message="Sync credentials are valid",
            )
        ]
    )

def generate_presigned_url(s3_url: str, expires_in_secs: int = 3600) -> Response[str]:
    """Generates presigned HTTP URL for S3 URL

    :param s3_url: URL of object in S3
    :type s3_url: str
    :param expires_in_secs: number of seconds after which presigned URL expires, defaults to 3600
    :type expires_in_secs: int, optional
    :return: presigned URL
    :rtype: Response[str]
    """
    parsed_s3_url = urlparse(s3_url)

    s3 = boto.client("s3")
    return Response(
        result=s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": parsed_s3_url.netloc, "Key": parsed_s3_url.path.lstrip("/")},
            ExpiresIn=expires_in_secs,
        )
    )
