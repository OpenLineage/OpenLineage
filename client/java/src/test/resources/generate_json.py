import json


if __name__ == "__main__":
    t = {}
    for i in range(0, 1000):
        t[str(i)] = {}
        for j in range(0, 1000):
            t[str(i)][str(j)] = str(i) + str(j)
    with open("large.json", 'w') as f:
        json.dump(t, f, indent=4)
