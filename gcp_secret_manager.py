from google.cloud import secretmanager
from google.oauth2 import service_account

smcreds = service_account.Credentials.from_service_account_file(
    "service_account_creds.json",
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

secret_manager_client = secretmanager.SecretManagerServiceClient(
    credentials=smcreds,
)

PROJECTID = smcreds.project_id


def access_secret_version(
    secret_id, project_id=PROJECTID, client=secret_manager_client
):
    # Build the resource name of the secret version.
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"

    # Access the secret version.
    response = client.access_secret_version(request={"name": name})
    payload = response.payload.data.decode("UTF-8")
    return payload


def destroy_secret_version(
    secret_id, project_id=PROJECTID, client=secret_manager_client
):

    version = get_secret_version(secret_id)

    # Build the resource name of the secret version
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version}"

    # Destroy the secret version.
    response = client.destroy_secret_version(request={"name": name})

    # print(f"Destroyed secret version: {response.name}")


def add_secret_version(
    secret_id, payload, project_id=PROJECTID, client=secret_manager_client
):

    parent = client.secret_path(project_id, secret_id)

    # convert payload from string to bytes
    payload_bytes = payload.encode("UTF-8")

    # Add the secret version.
    response = client.add_secret_version(
        request={
            "parent": parent,
            "payload": {"data": payload_bytes},
        }
    )

    # print(f"Added secret version: {response.name}")


def get_secret_version(secret_id, project_id=PROJECTID, client=secret_manager_client):

    # Build the resource name of the secret version.
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"

    # Get the secret version.
    response = client.get_secret_version(request={"name": name})

    # Print information about the secret version.
    state = response.state.name

    version = (response.name).split("/")[-1]
    # print(f"Got secret version {response.name} with state {state}")
    return version
