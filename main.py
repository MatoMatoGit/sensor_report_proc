from common.MessageFileLoader import MessageFileLoader
from common.GraphQLSocket import GraphQLSocket
import time


def MessageDataToQuery(data, id, timestamp):
    return """
    mutation {
      createMeasurement(data:""" + str(data) + """, hash:""" + str(id) + """, createdOn: """ + timestamp + """){
        measurement{
          id
          data
          createdOn
          uuid
        }
        sensor {
          id
          hash
        }
      }
    }
    """

def VerifyMessage(msg, msg_schema):
    return True


def VerifyId(msg_meta, tx_id):
    if msg_meta["id"] == tx_id:
        return True
    return False


def ProcessMessage(msg, id):
    if VerifyMessage(msg, msg) is False:
        print("[MsgProc] Error: Received message does not match the defined schema.")
        return

    if VerifyId(msg["meta"], id) is False:
        print("[MsgProc] Error: Transmit ID and message ID do not match.")
        return

    datetime = msg["meta"]["dt"]
    samples = msg["data"]["smp"]
    for s in samples:
        query = MessageDataToQuery(s, id, datetime)
        print(query)
        # graphql_sock.Send(query)


def main(directory):

    msg_loader = MessageFileLoader(directory)

    #graphql_sock = GraphQLSocket("./cfg")

    while True:
        while True:
            tx_id, seq_nr, msg = msg_loader.LoadNext()

            if seq_nr != -1:
                ProcessMessage(msg, tx_id)
                msg_loader.Remove(tx_id, seq_nr)
            else:
                print("No more files available")
                break

        time.sleep(5)


if __name__ == '__main__':
    main("./tst/0.1")

