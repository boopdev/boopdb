import json
from os import PathLike
from os.path import exists

def createEmptyJsonFile(path : PathLike):
    if exists(path):
        return # Silently return if it exists, we dont overwrite shit here!

    with open(path, 'w+') as j:
        json.dump([], j)
        j.close()