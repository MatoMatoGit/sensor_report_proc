from common.MessageFileLoader import MessageFileLoader
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
import time
import sys
import getopt
import json


transport = RequestsHTTPTransport(
    url='http://localhost:5000/graphql',
    use_json=True,
)

_MsgListFile = None
_MsgList = {}
_BaseDir = "./"


def ComposePath(base, type):
    return base + "/" + str(type)


# mutation {
#   createMeasurement(sensorHash:"bbb", sensorType:"MOIST", data:12, createdOnModule:"12u84872"){
#     measurement {
#       id
#     }
#   }
# }

def MessageDataToQuery(data, id, msg_type, timestamp):
    print(id)
    print(msg_type)
    print(timestamp)
    print(data)
    return gql("""
    mutation {
      createMeasurement(data:""" + str(data) + """, sensorHash: \"""" + str(id) + """\", sensorType: \"""" + msg_type + """\",  createdOnModule: \"""" + timestamp + """\"){
        measurement {
          id
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


def MessageTypeToString(msg_type):
    print(msg_type)
    global _MsgList

    for entry in _MsgList:
        if _MsgList[entry]["type"] == msg_type:
            return entry
    return None


def ProcessMessage(client, msg, id):
    if VerifyMessage(msg, msg) is False:
        print("[MsgProc] Error: Received message does not match the defined schema.")
        return

    if VerifyId(msg["meta"], id) is False:
        print("[MsgProc] Error: Transmit ID and message ID do not match.")
        return

    datetime = msg["meta"]["dt"]
    msg_type = str(msg["meta"]["type"])
    msg_stype = str(msg["meta"]["stype"])
    samples = msg["data"]["S"]

    msg_type = msg_type + '.' + msg_stype
    msg_type_str = MessageTypeToString(msg_type)

    if msg_type_str is None:
        return -1

    for s in samples:
        query = MessageDataToQuery(s, id, msg_type_str, datetime)
        print(client.execute(query))

    return 0


def ProcessInputsArgs(argv):
    global _MsgListFile
    global _BaseDir

    try:
        opts, args = getopt.getopt(argv, "hl:d:", ["list=", "dir="])
    except getopt.GetoptError:
        print('-l <list_file>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('-l <list_file>')
            sys.exit()
        elif opt in ("-l", "--list"):
            _MsgListFile = arg
        elif opt in ("-d", "--dir"):
            _BaseDir = arg
    print('Message list file is "', _MsgListFile)
    time.sleep(2)


def main(argv):
    global _MsgListFile
    global _MsgList
    global _BaseDir

    msg_loaders = set()

    ProcessInputsArgs(argv)

    msg_file = open(_MsgListFile, 'r')
    msg_file_str = msg_file.read()
    print(msg_file_str)
    msg_file.close()

    _MsgList = json.loads(msg_file_str)

    for msg_entry in _MsgList.keys():
        print(msg_entry)
        path = ComposePath(_BaseDir, _MsgList[msg_entry]["type"])
        msg_loaders.add(MessageFileLoader(path))

    client = Client(
        transport=transport,
        fetch_schema_from_transport=True,
    )

    while True:
        while True:
            for msg_loader in msg_loaders:
                tx_id, seq_nr, msg = msg_loader.LoadNext()
                if seq_nr != -1:
                    ProcessMessage(client, msg, tx_id)
                    msg_loader.Remove(tx_id, seq_nr)
            time.sleep(2)


if __name__ == '__main__':
    main(sys.argv[1:])

