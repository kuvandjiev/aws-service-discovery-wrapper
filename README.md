aws-service-discovery-wrapper
=============================

Wrapper around aws service_discovery and boto3 to register services in Azure Cloud Map

### For API documentation see:
https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/servicediscovery.html

### Usage:
```python ./service_discover.py <operation> <path_to_json_file>```

### Register instance:
```python ./service_discovery.py register_instance ./service_discovery.json```

The AWS Cloud Map namespace must be existing.

Required keys in the input JSON file are: `namespace`, `service_name`, `type`, `instance_name`

### List registered instances of a service:
```python ./service_discovery.py get_instances ./service_discovery.json```

### Deregister instance:
```python ./service_discovery.py deregister_instance ./service_discovery.json```

### Delete registered service:
```python ./service_discovery.py delete_service ./service_discovery.json```


### License:
[MIT](./LICENSE)
