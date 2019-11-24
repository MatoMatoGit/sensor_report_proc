import os
import json


class MessageFileLoader:

    def __init__(self, directory):
        """
        :param directory: A directory containing .json files with the following naming convention:
        <id>_<seq_nr>
        """
        self.FileNames = list()
        self.Dir = directory

    def LoadNext(self):
        """

        :return: (<id>, <seq nr>, <msg dict>) if a file was successfully loaded.
        :return: (-1, -1, -1) if an error has occurred.
        """
        # Get a list of files in the directory and sort the files by number.
        try:
            files = os.listdir(self.Dir)
            files = sorted(files, key=MessageFileLoader.SeqNrFromFileName)
            print(files)
        except OSError:
            print("[MsgFileLoader] Cannot load next file.")
            return -1, -1, -1

        # Get the list index of the next JSON file.
        i = 0
        while '.json' not in files[i]:
            if i < len(files) - 1:
                i += 1
            else:
                return -1, -1, -1

        # Read the JSON data and serialize it to the output stream.
        try:
            f_data = open(self.Dir + "/" + files[i], 'r')
            msg_data = json.load(f_data)
            f_data.close()
            return self.IdFromFileName(files[i]), i, msg_data
        except ValueError:
            print("Failed to read / serialize data for file {}".format(files[i]))
            return -1, -1, -1

    def Remove(self, id, seq_nr):
        try:
            f_path = self.Dir + "/" + str(id) + "_" + str(seq_nr) + ".json"
            os.remove(f_path)
        except OSError:
            print("Could not remove file: {}.".format(f_path))

    def IdFromFileName(self, file_name):
        return file_name.split('_')[0]

    @staticmethod
    def SeqNrFromFileName(x):
        return x.split('_')[1]
