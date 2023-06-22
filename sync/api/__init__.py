from sync.clients.sync import get_default_client
from sync.models import AccessReport, AccessReportLine, AccessStatusCode


def get_access_report() -> AccessReport:
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
                message="Can Authenticate Sync credentials",
            )
        ]
    )
