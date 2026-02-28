import uuid

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer


def error_cb(err):
    """The error callback is used for generic client errors. These
    errors are generally to be considered informational as the client will
    automatically try to recover from all errors, and no extra action
    is typically required by the application.
    For this example however, we terminate the application if the client
    is unable to connect to any broker (_ALL_BROKERS_DOWN) and on
    authentication errors (_AUTHENTICATION)."""

    print("Client error: {}".format(err))
    if err.code() == KafkaError._ALL_BROKERS_DOWN or err.code() == KafkaError._AUTHENTICATION:
        # Any exception raised from this callback will be re-raised from the
        # triggering flush() or poll() call.
        raise KafkaException(err)


# Create producer
p = Producer(
    {
        'bootstrap.servers': 'vskafkalab1.servicebus.windows.net:9093',
        'sasl.mechanism': 'PLAIN',
        'security.protocol': 'SASL_SSL',
        'sasl.username': '$ConnectionString',
        'sasl.password': 'Endpoint=sb://vskafkalab1.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=iId8K7LaPMa5fKxpJ3GB8SBVMp01IjfmR+AEhBfbtdA=;EntityPath=vspyhontopic' #,
   #    'error_cb': error_cb,
    }
)


def acked(err, msg):
    """Delivery report callback called (from flush()) on successful or failed delivery of the message."""
    if err is not None:
        print('Failed to deliver message: {}'.format(err.str()))
    else:
        print('Produced to: {} [{}] @ {}'.format(msg.topic(), msg.partition(), msg.offset()))


for n in range(0, 10):
    # Produce message: this is an asynchronous operation.
    # Upon successful or permanently failed delivery to the broker the
    # callback will be called to propagate the produce result.
    # The delivery callback is triggered from poll() or flush().
    # For long running
    # produce loops it is recommended to call poll() to serve these
    # delivery report callbacks.
    p.produce('vspyhontopic', value=f'python test value nr {n}', callback=acked)

    # Trigger delivery report callbacks from previous produce calls.
    p.poll(0)

# flush() is typically called when the producer is done sending messages to wait
# for outstanding messages to be transmitted to the broker and delivery report
# callbacks to get called. For continous producing you should call p.poll(0)
# after each produce() call to trigger delivery report callbacks.
p.flush(10)
