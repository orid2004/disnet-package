class Job:
    def __init__(self, type, id, args, ttl=200):
        self.type = type
        self.id = id
        self.ttl = ttl
        self.args = args
        self.sock_fileno = 0


from random import randint


def digit():
    return randint(0, 9)


with open("file", "w") as f:
    for _ in range(20):
        f.write(f'{digit()}{digit()}{digit()}{digit()} : {randint(2, 6)}\n')
