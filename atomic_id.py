# -*- coding: utf-8 -*-
# i'm not touching this


def _increment(id):
    id = [c for c in id]
    for i in reversed(range(len(id))):
        d = 0
        if id[i] == '9':
            id[i] = 'A'
        elif id[i] == 'Z':
            id[i] = '0'
            d = 1
        else:
            id[i] = chr(ord(id[i]) + 1)
        if d == 0:
            break
    return ''.join(id)


class AtomicId:
    def __init__(self, starting_number):
        self.current_id = str(starting_number)

    def next_id(self):
        self.current_id = _increment(self.current_id)
        return self.current_id
