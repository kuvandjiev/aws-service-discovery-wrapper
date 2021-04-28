"""
MIT License

Copyright (c) 2021 Vladimir Kuvandjiev

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

import boto3
import argparse
import json
import datetime
import time

VERSION = "1.0"

""" List of required keys in the input JSON file with custom attributes
    to be associated with the service and the instance
"""


def register_service(namespace_id: str, service_name: str, service_description: str, client) -> str:
    """ Register a new service in AWS Cloud map"""
    response = client.create_service(
        Name=service_name,
        NamespaceId=namespace_id,
        Description=service_description,
        HealthCheckCustomConfig={
            'FailureThreshold': 1
        },
        Type='HTTP'
    )
    return response.get("Service").get('Id')


def register_instance(service_id: str, custom_attributes: dict, client) -> str:
    instance_id = custom_attributes.get('instance_name')
    """ Registers a new service instance in AWS Cloud map"""
    response = client.register_instance(
        ServiceId=service_id,
        InstanceId=instance_id,
        Attributes=custom_attributes
    )
    return response.get('OperationId')


def deregister_instance(service_id: str, instance_id: str, client):
    """ Deregisters instance """
    return client.deregister_instance(ServiceId=service_id, InstanceId=instance_id).get('OperationId')


def await_operation_result(operation_id: str,
                           client,
                           SUCCESS_STATUS: str = "SUCCESS",
                           FAIL_STATUS: str = "FAIL",
                           RETRY_AFTER: int = 5,
                           OPERATION_TIMEOUT: int = 3600):
    """
    Awaits operation result for some time.
    Will raise RuntimeError if the status is not SUCCESS_STATUS.
    Will timeout after OPERATION_TIMEOUT.
    """
    t1 = datetime.datetime.now()
    while True:
        print("Checking...")
        get_operation_response = client.get_operation(OperationId=operation_id)
        operation_status = get_operation_response.get('Operation', {}).get('Status')
        if operation_status == SUCCESS_STATUS:
            print("Success.")
            break
        if operation_status == FAIL_STATUS:
            print("Operation failed.")
            raise RuntimeError(get_operation_response)
        t2 = datetime.datetime.now()
        total_seconds = (t2 - t1).total_seconds()
        if total_seconds > OPERATION_TIMEOUT:
            raise RuntimeError(f"Operation timed out after {total_seconds}. Last get operation result {get_operation_response}")
        print("Waiting ...")
        time.sleep(RETRY_AFTER)


def delete_service(service_id: str, client):
    """ Deletes a service. Expects to not have registered instance. """
    client.delete_service(Id=service_id)


def get_instances_for_service(service_id: str, client) -> list:
    """ Returns list of registered instance for service with id <service_id>"""
    return client.list_instances(ServiceId=service_id).get('Instances', [])


def get_namespace_by_name(namespace_name: str, client) -> str:
    """ Returns namespace id of a namespace with name <namespace_name> """
    namespaces = client.list_namespaces()
    for n in namespaces.get("Namespaces"):
        if n.get("Name") == namespace_name:
            return n.get("Id")
    raise RuntimeError(f"Namespace {namespace_name} not found")


def get_service_by_name(namespace_id: str, service_name: str, client) -> str:
    """ Returns service id of service with name <service_name> """
    service_id = None
    next_token = None
    max_results = 100
    while True:
        token_args = {"NextToken": next_token} if next_token else {}
        services_response = client.list_services(
            MaxResults=max_results,
            Filters=[
                {
                    'Name': 'NAMESPACE_ID',
                    'Values': [namespace_id, ],
                    'Condition': 'EQ'
                },
            ],
            **token_args
        )
        next_token = services_response.get('NextToken', None)
        services = services_response.get('Services')
        for service in services:
            if service.get("Name") == service_name:
                service_id = service.get('Id', None)
                break
        if next_token is None:
            break
    return service_id


def check_required_keys(data: dict, required_keys: list):
    """ Raises runtime error if a required key from <required_keys> is not found in <data> """
    for required_key in required_keys:
        if not data.get(required_key, None):
            raise RuntimeError(f"'{required_key}' is a required key.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("operation", help="Operation - register_instance, update_instance, deregister_instance, delete_service")
    parser.add_argument("service_discovery_data_json", help="Path to JSON file with service information")
    parser.add_argument('--extra', nargs=2, action='append')
    args = parser.parse_args()

    extra_attributes = {k: v for k, v in args.extra} if args.extra else {}
    if extra_attributes:
        print(f"Extra attributes provided: {extra_attributes}")

    operation = args.operation
    data_file = open(args.service_discovery_data_json, "r")
    service_data = json.loads(data_file.read())

    valid_operations = ['register_instance', 'deregister_instance', 'update_instance', 'delete_service', 'get_instances']
    if operation not in valid_operations:
        raise RuntimeError(f'{operation} is not a valid operation. Please pass one of {valid_operations}')

    check_required_keys(data=service_data, required_keys=['namespace'])
    namespace_name = service_data.get("namespace", None)
    service_name = service_data.get("service_name", None)

    client = boto3.client('servicediscovery')

    namespace_id = get_namespace_by_name(namespace_name=namespace_name, client=client)
    service_id = get_service_by_name(namespace_id=namespace_id, service_name=service_name, client=client)

    if service_id is None and operation in ['register_instance', 'delete_service']:
        check_required_keys(data=service_data, required_keys=['namespace', 'service_name', 'description'])
        service_description = service_data.pop("description", None)
        service_id = register_service(namespace_id=namespace_id,
                                      service_name=service_name,
                                      service_description=service_description,
                                      client=client)

    if operation == 'register_instance':
        check_required_keys(data=service_data, required_keys=['namespace', 'service_name', 'type', 'instance_name'])
        print(f"Registering instance...")
        operation_id = register_instance(service_id=service_id, custom_attributes={**service_data, **extra_attributes}, client=client)
        print(f"Operation with id {operation_id} submitted. Checking status...")
        await_operation_result(operation_id=operation_id, client=client)
    elif operation == 'update_instance':
        check_required_keys(data=service_data, required_keys=['namespace', 'service_name', 'instance_name'])
        instance_name = service_data.get("instance_name", None)
        print(f"Updating instance with name {instance_name}...")
        for instance in get_instances_for_service(service_id=service_id, client=client):
            current_attributes = instance.get("Attributes", {})
            if current_attributes.get('instance_name') == instance_name:
                # order is important when merging the dicts here
                # namespace, service_name and instance_name must not remain unchanged
                custom_attributes = {**current_attributes,
                                     **service_data,
                                     **extra_attributes,
                                     'namespace': service_data.get('namespace'),
                                     'service_name': service_data.get('service_name'),
                                     'instance_name': service_data.get('instance_name')
                                     }
                operation_id = register_instance(service_id=service_id,
                                                 custom_attributes=custom_attributes,
                                                 client=client)
                print(f"Operation with id {operation_id} submitted. Checking status...")
                await_operation_result(operation_id=operation_id, client=client)
    elif operation == 'deregister_instance':
        check_required_keys(data=service_data, required_keys=['namespace', 'service_name', 'instance_name'])
        instance_name = service_data.get("instance_name", None)
        print(f"Deregistering instance with name {instance_name}...")
        for instance in get_instances_for_service(service_id=service_id, client=client):
            if instance.get("Attributes", {}).get('instance_name') == instance_name:
                instance_id = instance.get('Id', None)
                operation_id = deregister_instance(service_id=service_id, instance_id=instance_id, client=client)
                print(f"Operation with id {operation_id} submitted. Checking status...")
                await_operation_result(operation_id=operation_id, client=client)
    elif operation == 'delete_service':
        check_required_keys(data=service_data, required_keys=['namespace', 'service_name'])
        print(f"Deleting service...")
        delete_service(service_id=service_id, client=client)
        print("Service deleted")
    elif operation == 'get_instances':
        check_required_keys(data=service_data, required_keys=['namespace', 'service_name'])
        if service_id is None:
            raise RuntimeError(f'Service with name {service_name} not found')
        print(get_instances_for_service(service_id=service_id, client=client))


if __name__ == "__main__":
    main()
