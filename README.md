# sesam-gcp-pubsub-sink
Sesam.io sink to GCP pubsub

Example System config 
```json
{
  "_id": "gcp-pubsub-sink",
  "type": "system:microservice",
  "docker": {
    "environment": {
      "GOOGLE_APPLICATION_CREDENTIALS": "credentials.json", <- name of file where credentials will be stored, or file name if you store credentials file in image (not smart)
      "GOOGLE_APPLICATION_CREDENTIALS_CONTENT": {
        <content from credentials json file from GCP>
      },
      "PAYLOAD_KEY": "<optional attribute which part of entity send as payload (if you don't want to pass whole entity)>",
      "PROJECT_ID": "<project id>"
    },
    "image": "ohuenno/sesam-gcp-pubsub-sink",
    "port": 5000
  },
  "verify_ssl": true
}
```

Example Pipe config
```json
{
  "_id": "gcp-pubsub",
  "type": "pipe",
  "source": {
    "type": "embedded",
    "entities": [{
      "_id": "an sesam id",
      "data": {
        "key1": "value1",
        "key2": "value2",
        "key3": {
          "type": "type value",
          "attributes": {
            "attr1": "00002117-0000020004",
            "attr2": "00002117-0000020004"
          },
          "id": "an external id"
        },
        "one more key": "one more value"
      }
    }]
  },
  "sink": {
    "type": "json",
    "system": "gcp-pubsub-sink",
    "url": "/<topic name>"
  },
  "pump": {
    "cron_expression": "0 0 1 1 ?"
  }
}
```

