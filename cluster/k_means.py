#encoding=utf8

import math

# 使用余弦定理
def cal_distance(x_vector, y_vetctor):
    a = 0
    b = 0
    c = 0
    for i in range(x_vector):
        a += x_vector[i] * y_vetctor[i]
        b += x_vector[i] * x_vector[i]
        c += y_vetctor[i] * y_vetctor[i]

    cosine = 0
    if (b + c) > 0:
        cosine = 1.0 * a / (math.sqrt(b) * math.sqrt(c))

    return 1 - cosine

def init_center_points(vectors, k):
    center_points = []
    for i in range(k):
        center_points.append(vectors[i])
    return center_points

def get_min_distance(point_vector, center_points):
    min_index = 0
    min_dis = cal_distance(point_vector, center_points[0])
    for i in range(1, len(center_points)):
        dis = cal_distance(point_vector, center_points[i])
        if dis < min_dis:
            min_index = i
            min_dis = dis

    return min_index, min_dis

def get_new_center_point(point_vectors, stack):
    col_nums = len(point_vectors[0])
    new_center_vector = [0 for i in range(col_nums)]
    for col in range(col_nums):
        for row in stack:
            new_center_vector[col] += point_vectors[row][col]

    new_center_vector = map(lambda x: x / len(stack), new_center_vector)
    return new_center_vector

def k_means(vectors, k):
    points_num = len(vectors)
    stacks = [set() for i in range(k)]
    center_points = init_center_points(vectors, k)
    min_dis = [2 for i range(points_num)]
    is_changed = True
    while is_changed:
        is_changed = False

        for point_index in range(points_num):
            min_index, min_dis = get_min_distance(vectors[point_index], center_points)
            if point_index in stacks[min_index]:
                continue

            is_changed = True
            for center_index in range(k):
                stacks[center_index].remove(point_index)
            stacks[min_index].add(point_index)

        if is_changed:
            for center_index in range(k):
                center_points[center_index] = get_new_center_point(vectors, stacks[center_index])

    return stacks

