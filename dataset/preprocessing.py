import json
import os

current_dir = os.path.dirname(os.path.realpath(__file__))
batch_dir = current_dir + "/batch_data"
streaming_dir = current_dir + "/streaming_data"

def flatten_data(dir_name):
    for file in os.listdir(dir_name):
        try:
            data = json.load(open(dir_name + "/" + file, "r"))
        except:
            print("Error in file: " + file)

        playlists = data["playlists"]

        file_name = dir_name + "/" + file
        with open(file_name, 'w') as outfile1:
            for row in playlists:
                print(json.dumps(row), file=outfile1)

flatten_data(batch_dir)
flatten_data(streaming_dir)