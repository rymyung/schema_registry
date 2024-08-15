import requests
import json
from typing import Optional
from core.constants import BASE_SCHEMA_REGISTRY_URL

def get_proto_schema(file_path: str) -> str:
    with open(file_path, "r") as proto_file:
        proto_schema = proto_file.read()
    
    return proto_schema


def register_schema(schema: str, subject_name: str) -> int:
    headers = {
        "Content-Type": "application/vnd.schemaregistry.v1+json"
    }
    payload = {
        "schemaType": "PROTOBUF",
        "schema": schema
    }
    url = f"{BASE_SCHEMA_REGISTRY_URL}/subjects/{subject_name}/versions"
    response = requests.post(url, headers=headers, data=json.dumps(payload))

    if response.status_code == 200:
        print("Success register schema!")
        print(f"Response: {response.json()}")

    else:
        print(f"Fail to register schema. {response.status_code}")
        print(f"Response: {response.text}")
    
    return response.status_code


def get_registered_schema(subject_name: Optional[str]=None):
    if subject_name:
        url = f"{BASE_SCHEMA_REGISTRY_URL}/subjects/{subject_name}/versions"
        response = requests.get(url)
        if response.status_code == 200:
            versions = response.json()
            print(f"{subject_name}'s all versions:")
            for version in versions:
                print(version)
        else:
            print(f"Fail to get schema version. {response.status_code}")
            print(f"Response: {response.text}")

    else:
        response = requests.get(f"{BASE_SCHEMA_REGISTRY_URL}/subjects")

        if response.status_code == 200:
            subjects = response.json()
            print("regstered schema subjects:")
            for subject in subjects:
                print(subject)
        else:
            print(f"Fail to get registered schema. {response.status_code}")
            print(f"Response: {response.text}")


if __name__ == "__main__":
    proto_schema = get_proto_schema("schema_registry/schemas/person.proto")

    subject_name = "person"
    result = register_schema(schema=proto_schema, subject_name=subject_name)
    if result == 200:
        print("="*100)
        get_registered_schema()
        print("="*100)
        get_registered_schema(subject_name=subject_name)
