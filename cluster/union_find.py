#encoding=utf8

parent = [i for i in range(0, 2000)]
weight = [1 for i in range(0, 2000)]

def find(x):
    while (x != parent[x]):
        parent[x] = parent[parent[x]]
        x = parent[x]

    return parent[x]

def union(x, y):
    x = find(x)
    y = find(y)

    if (weight[x] < weight[y]):
        weight[y] += weight[x]
        parent[x] = y
    else:
        weight[x] += weight[y]
        parent[y] = x

#find(2)
#union(2, 3)
#print parent
#print ''
#print weight
