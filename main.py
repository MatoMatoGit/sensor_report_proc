from common.MessageFileLoader import MessageFileLoader
import time
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport

_transport = RequestsHTTPTransport(
    url='http://localhost:5000/graphql',
    use_json=True,
)


def MessageDataToQuery(data, id, timestamp):
    return gql("""
    mutation {
      createMeasurement(data:""" + str(data) + """, sensorHash: \"""" + str(id) + """\"){
        sensor {
          id
          sensorHash
          createdOnServer
        }
      }
    }
    """)


def VerifyMessage(msg, msg_schema):
    return True


def VerifyId(msg_meta, tx_id):
    if msg_meta["id"] == tx_id:
        return True
    return False


def ProcessMessage(client, msg, id):
    if VerifyMessage(msg, msg) is False:
        print("[MsgProc] Error: Received message does not match the defined schema.")
        return

    if VerifyId(msg["meta"], id) is False:
        print("[MsgProc] Error: Transmit ID and message ID do not match.")
        return

    datetime = msg["meta"]["dt"]
    samples = msg["data"]["S"]
    for s in samples:
        query = MessageDataToQuery(s, id, datetime)
        print(client.execute(query))


def main(directory):

    msg_loader = MessageFileLoader(directory)

    client = Client(
        transport=_transport,
        fetch_schema_from_transport=True,
    )

    while True:
        while True:
            tx_id, seq_nr, msg = msg_loader.LoadNext()

            if seq_nr != -1:
                ProcessMessage(client, msg, tx_id)
                msg_loader.Remove(tx_id, seq_nr)
            else:
                print("No more files available")
                break

        time.sleep(5)


if __name__ == '__main__':
    main("../upyiot/test/0.2")

