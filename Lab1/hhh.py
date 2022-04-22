# from turtle import *
#
# color('red', 'pink')
# begin_fill()
# left(135)
# fd(100)
# right(180)
#
# circle(30, -180)
#
# backward(35)
# right(90)
# forward(35)
# circle(-30, 180)
# fd(100)
# end_fill()
# hideturtle()
# done()

# def nChooseK(n,k):
#     ans = 1
#     for i in range(1, n+1):
#         ans = ans * i
#     for j in range(1, k+1):
#         ans = ans / j
#     for k in range(1, n-k+1):
#         ans = ans / k
#     return int(ans)

import datetime
# import time
# starttime = time.time()
# def nChooseK(n,k):
#     ans = 1
#     for i in range(1, n+1):
#         ans = ans * i
#     for j in range(1, k+1):
#         ans = ans / j
#     for k in range(1, n-k+1):
#         ans = ans / k
#     return int(ans)
#
# print(nChooseK(150,10))
# endtime = time.time()
# print(endtime - starttime)
import random
# def monte_carlo_pi(n):
#     r = 1
#     circle = 0
#     square = 0
#     for i in range(n):
#         x = random.uniform(-1,1)
#         y = random.uniform(-1,1)
#         if x**2 + y **2 <= r**2:
#             circle += 1
#         else:
#             square += 1
#     k = circle
#     p = k / n
#     pi = 4 * p
#     return pi
# print(monte_carlo_pi(10000))


def alpha2num(s):
	string1 = 'abcdefghijklmnopqrstuvwxyz'
	string2 = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
	dict1 = {}
	dict2 = {}
	for i in range(27):
        dict1[string1[i]] = i
        dict2[string2[i]] = i
    if dict1[s]:
        return dict1[s]
    else:
        return dict2[s]

#Test cases
print(alpha2num('a'))
print(alpha2num('C'))
print(alpha2num('R'))
