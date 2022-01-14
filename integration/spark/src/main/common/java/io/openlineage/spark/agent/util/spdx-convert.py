

def insert_license():

    import os

    files = [f for f in os.listdir('.') if os.path.isfile(f)]
    # for root, dirs, files in os.walk('.', topdown=False):
    # files = [f for f in os.listdir('.')]
    for f in files:

        with open(f, 'r') as x:
            contents = x.readlines()

        contents.insert(0, '/* SPDX-License-Identifier: Apache-2.0 */\n\n')

        with open(f, 'w') as x:
            contents = ''.join(contents)
            x.write(contents)

insert_license()
