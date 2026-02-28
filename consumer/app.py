import uuid

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer

# Create consumer
c = Consumer(
    {
        'bootstrap.servers': 'vskafkalab1.servicebus.windows.net:9093',
        'sasl.mechanism': 'PLAIN',
        'security.protocol': 'SASL_SSL',
        'sasl.username': '$ConnectionString',
        'sasl.password': 'Endpoint=sb://vskafkalab1.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=iId8K7LaPMa5fKxpJ3GB8SBVMp01IjfmR+AEhBfbtdA=;EntityPath=vspyhontopic',
        'group.id': str(uuid.uuid1()),  # this will create a new consumer group on each invocation.
        'auto.offset.reset': 'earliest' #,
        # 'error_cb': error_cb,
    }
)

c.subscribe(['vspyhontopic'])

try:
    while True:
        msg = c.poll(0.1)  # Wait for message or event/error
        if msg is None:
            # No message available within timeout.
            # Initial message consumption may take up to `session.timeout.ms` for
            #   the group to rebalance and start consuming.
            continue
        if msg.error():
            # Errors are typically temporary, print error and continue.
            print('Consumer error: {}'.format(msg.error()))
            continue

        print('Consumed: {}'.format(msg.value()))

except KeyboardInterrupt:
    pass

finally:
    # Leave group and commit final offsets
    c.close()
