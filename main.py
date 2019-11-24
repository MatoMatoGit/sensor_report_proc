from MessageFileLoader import MessageFileLoader
from GraphQLSocket import GraphQLSocket
import io
import time


def sensor_report_to_query(samples, id, timestamp):
    return """query
    """


def main(directory):

    msg_loader = MessageFileLoader(directory)

    # graphql_sock = GraphQLSocket("./cfg")

    while True:
        while True:
            id , seq_nr, msg = msg_loader.LoadNext()

            if seq_nr != -1:

                query = sensor_report_to_query(msg["data"]["S"], id, msg["meta"]["dt"])
                print(query)

                # graphql_sock.Send(query)

                msg_loader.Remove(id, seq_nr)
            else:
                print("No more files available")
                break

        time.sleep(5)


if __name__ == '__main__':
    main("./tst/0.1")

